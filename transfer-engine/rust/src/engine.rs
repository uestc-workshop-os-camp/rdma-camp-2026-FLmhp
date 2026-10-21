#![allow(dead_code)]

#[allow(unused_imports)]
use rdma_mummy_sys as verbs;
use std::collections::HashMap;
use std::ffi::c_void;
use std::ffi::{CStr, CString};
use std::mem::{size_of, zeroed};
use std::ops::BitOr;
use std::{ptr, slice};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Unimplemented,
    Invalid,
    Busy,
    Io,
    Timeout,
    Unsupported,
    NoMemory,
    InvalidState,
    Canceled,
}

pub struct Config {
    pub port: u32,
    pub gid_index: u32,
    pub device: Option<String>, // None selects an available device.
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryAccess(u32);

impl MemoryAccess {
    pub const NONE: Self = Self(0);
    pub const LOCAL_WRITE: Self = Self(1 << 0);
    pub const REMOTE_WRITE: Self = Self(1 << 1);
    pub const REMOTE_READ: Self = Self(1 << 2);
    pub const REMOTE_ATOMIC: Self = Self(1 << 3);

    pub fn contains(self, access: Self) -> bool {
        self.0 & access.0 == access.0
    }
}

impl BitOr for MemoryAccess {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

pub type PeerId = u64;
pub type BatchId = u64;
pub type MemoryId = u64;
pub type RemoteMemoryId = u64;

#[derive(Debug, Clone, Copy)]
pub enum Operation {
    Write {
        remote_address: u64,
        inline_data: bool,
    },
    Read {
        remote_address: u64,
    },
    Send {
        inline_data: bool,
    },
    Recv,
    WriteImm {
        remote_address: u64,
        immediate: u32,
        inline_data: bool,
    },
    CompareSwap {
        remote_address: u64,
        compare: u64,
        value: u64,
    },
    FetchAdd {
        remote_address: u64,
        value: u64,
    },
}

/// The descriptor is owned; its buffer remains borrowed until terminal status.
#[derive(Debug, Clone, Copy)]
pub struct Request {
    pub peer: PeerId,
    pub local_address: *mut c_void,
    pub length: u64,
    pub operation: Operation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiveResult {
    Message { length: u64 },
    WriteNotice { immediate: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferStatus {
    Pending,
    Completed { receive: Option<ReceiveResult> },
    Failed(Error),
}

/// Engine owns the metadata; it stays valid until this peer or Engine is destroyed.
pub struct PeerMetadata<'a> {
    pub peer: PeerId,
    pub metadata: &'a [u8],
}

/// Bytes remain stable until this registration is deregistered or Engine is dropped.
pub struct MemoryMetadata<'a> {
    pub memory: MemoryId,
    pub metadata: &'a [u8],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct MemoryExchange {
    address: u64,
    length: u64,
    rkey: u32,
    access: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct PeerExchange {
    lid: u16,
    qpn: u32,
    psn: u32,
    gid: verbs::ibv_gid,
}

struct LocalMemory {
    mr: *mut verbs::ibv_mr,
    exported: Box<MemoryExchange>,
}

#[derive(Clone, Copy)]
struct RemoteMemory {
    id: RemoteMemoryId,
    address: u64,
    length: u64,
    rkey: u32,
    access: MemoryAccess,
}

struct Peer {
    qp: *mut verbs::ibv_qp,
    exported: Box<PeerExchange>,
    connected: bool,
    remotes: HashMap<RemoteMemoryId, RemoteMemory>,
}

struct Batch {
    statuses: Vec<TransferStatus>,
}

pub struct Engine {
    port: u8,
    gid_index: i32,
    active_mtu: verbs::ibv_mtu::Type,
    lid: u16,
    context: *mut verbs::ibv_context,
    pd: *mut verbs::ibv_pd,
    cq: *mut verbs::ibv_cq,
    locals: HashMap<MemoryId, LocalMemory>,
    peers: HashMap<PeerId, Peer>,
    batches: HashMap<BatchId, Batch>,
    next_memory: MemoryId,
    next_peer: PeerId,
    next_remote: RemoteMemoryId,
    next_batch: BatchId,
}

fn contains(base: u64, size: u64, address: u64, length: u64) -> bool {
    address >= base && address - base <= size && length <= size - (address - base)
}

unsafe fn open_device(name: Option<&str>) -> Result<*mut verbs::ibv_context, Error> {
    let mut count = 0;
    let devices = verbs::ibv_get_device_list(&mut count);
    if devices.is_null() {
        return Err(Error::Io);
    }
    if count == 0 {
        verbs::ibv_free_device_list(devices);
        return Err(Error::Unsupported);
    }
    let mut chosen = ptr::null_mut();
    for index in 0..count as isize {
        let device = *devices.offset(index);
        let device_name = CStr::from_ptr(verbs::ibv_get_device_name(device));
        if name.is_none() || name.is_some_and(|value| device_name.to_bytes() == value.as_bytes()) {
            chosen = device;
            break;
        }
    }
    if chosen.is_null() {
        verbs::ibv_free_device_list(devices);
        return Err(Error::Invalid);
    }
    let context = verbs::ibv_open_device(chosen);
    verbs::ibv_free_device_list(devices);
    if context.is_null() {
        Err(Error::Io)
    } else {
        Ok(context)
    }
}

fn range_local(engine: &Engine, address: u64, length: u64) -> Option<&LocalMemory> {
    engine.locals.values().find(|memory| {
        contains(
            memory.exported.address,
            memory.exported.length,
            address,
            length,
        )
    })
}

fn range_remote(peer: &Peer, address: u64, length: u64) -> Option<RemoteMemory> {
    peer.remotes
        .values()
        .copied()
        .find(|memory| contains(memory.address, memory.length, address, length))
}

impl Engine {
    /// Create an engine with device resources and a background worker.
    /// Return an initialized object; release partial resources on error.
    pub fn create(config: Config) -> Result<Box<Self>, Error> {
        let port = u8::try_from(config.port).map_err(|_| Error::Invalid)?;
        let gid_index = i32::try_from(config.gid_index).map_err(|_| Error::Invalid)?;
        if port == 0
            || config
                .device
                .as_deref()
                .is_some_and(|name| CString::new(name).is_err())
        {
            return Err(Error::Invalid);
        }
        unsafe {
            let context = open_device(config.device.as_deref())?;
            let pd = verbs::ibv_alloc_pd(context);
            if pd.is_null() {
                verbs::ibv_close_device(context);
                return Err(Error::Io);
            }
            let cq = verbs::ibv_create_cq(context, 1024, ptr::null_mut(), ptr::null_mut(), 0);
            if cq.is_null() {
                verbs::ibv_dealloc_pd(pd);
                verbs::ibv_close_device(context);
                return Err(Error::Io);
            }
            let mut attr: verbs::ibv_port_attr = zeroed();
            if verbs::ibv_query_port(context, port, &mut attr) != 0 {
                verbs::ibv_destroy_cq(cq);
                verbs::ibv_dealloc_pd(pd);
                verbs::ibv_close_device(context);
                return Err(Error::Io);
            }
            Ok(Box::new(Self {
                port,
                gid_index,
                active_mtu: attr.active_mtu,
                lid: attr.lid,
                context,
                pd,
                cq,
                locals: HashMap::new(),
                peers: HashMap::new(),
                batches: HashMap::new(),
                next_memory: 0,
                next_peer: 0,
                next_remote: 0,
                next_batch: 0,
            }))
        }
    }

    /// Register application memory and return a fresh handle and its metadata.
    ///
    /// # Safety
    /// Memory stays allocated until deregistration succeeds or Engine is dropped.
    /// Applications must not access a buffer in conflict with a pending transfer's
    /// reads or writes.
    pub unsafe fn register_local_memory(
        &mut self,
        address: *mut c_void,
        length: usize,
        access: MemoryAccess,
    ) -> Result<MemoryMetadata<'_>, Error> {
        if address.is_null()
            || length == 0
            || (access.contains(MemoryAccess::REMOTE_ATOMIC)
                && !access.contains(MemoryAccess::LOCAL_WRITE))
        {
            return Err(Error::Invalid);
        }
        let mr = verbs::ibv_reg_mr(self.pd, address, length, access.0 as i32);
        if mr.is_null() {
            return Err(Error::NoMemory);
        }
        self.next_memory = self.next_memory.checked_add(1).ok_or_else(|| {
            verbs::ibv_dereg_mr(mr);
            Error::NoMemory
        })?;
        let id = self.next_memory;
        let exported = Box::new(MemoryExchange {
            address: address as u64,
            length: length as u64,
            rkey: (*mr).rkey,
            access: access.0,
        });
        let metadata = (&*exported as *const MemoryExchange).cast::<u8>();
        self.locals.insert(id, LocalMemory { mr, exported });
        Ok(MemoryMetadata {
            memory: id,
            metadata: slice::from_raw_parts(metadata, size_of::<MemoryExchange>()),
        })
    }

    /// Deregister local memory without freeing the application's buffer.
    /// Return Busy while accepted local tasks still use it, including queued work.
    /// Success invalidates the registration handle and its metadata.
    ///
    /// # Safety
    /// The application has stopped and drained all remote accesses to this MR.
    pub unsafe fn deregister_local_memory(&mut self, memory: MemoryId) -> Result<(), Error> {
        let local = self.locals.remove(&memory).ok_or(Error::Invalid)?;
        if verbs::ibv_dereg_mr(local.mr) != 0 {
            self.locals.insert(memory, local);
            return Err(Error::Io);
        }
        Ok(())
    }

    /// Import a connected peer's memory registration and return a local handle.
    /// Copy the metadata without modifying the QP; reuse identical live imports
    /// and reject conflicting overlapping regions.
    pub fn import_remote_memory(
        &mut self,
        peer: PeerId,
        metadata: &[u8],
    ) -> Result<RemoteMemoryId, Error> {
        if metadata.len() != size_of::<MemoryExchange>() {
            return Err(Error::Invalid);
        }
        let info = unsafe { ptr::read_unaligned(metadata.as_ptr().cast::<MemoryExchange>()) };
        if info.length == 0 || info.address.checked_add(info.length).is_none() {
            return Err(Error::Invalid);
        }
        let peer = self.peers.get_mut(&peer).ok_or(Error::Invalid)?;
        if !peer.connected {
            return Err(Error::InvalidState);
        }
        for remote in peer.remotes.values() {
            if remote.address == info.address
                && remote.length == info.length
                && remote.rkey == info.rkey
            {
                return Ok(remote.id);
            }
            let end = remote
                .address
                .checked_add(remote.length)
                .ok_or(Error::Invalid)?;
            let info_end = info.address + info.length;
            if remote.address < info_end && info.address < end {
                return Err(Error::Invalid);
            }
        }
        self.next_remote = self.next_remote.checked_add(1).ok_or(Error::NoMemory)?;
        let id = self.next_remote;
        peer.remotes.insert(
            id,
            RemoteMemory {
                id,
                address: info.address,
                length: info.length,
                rkey: info.rkey,
                access: MemoryAccess(info.access),
            },
        );
        Ok(id)
    }

    /// Remove a local imported descriptor; return Busy while an accepted task uses it.
    /// The peer's MR and its other connections remain unchanged.
    pub fn remove_remote_memory(&mut self, memory: RemoteMemoryId) -> Result<(), Error> {
        for peer in self.peers.values_mut() {
            if peer.remotes.remove(&memory).is_some() {
                return Ok(());
            }
        }
        Err(Error::Invalid)
    }

    /// Create a local peer and export its connection metadata.
    /// Return a nonzero handle and nonempty metadata; the RC QP starts in INIT.
    /// Bytes stay immutable and stable until this peer or Engine is destroyed;
    /// roll back the creation on failure.
    pub fn create_peer(&mut self) -> Result<PeerMetadata<'_>, Error> {
        unsafe {
            let mut init: verbs::ibv_qp_init_attr = zeroed();
            init.send_cq = self.cq;
            init.recv_cq = self.cq;
            init.qp_type = verbs::ibv_qp_type::IBV_QPT_RC;
            init.cap.max_send_wr = 16;
            init.cap.max_recv_wr = 16;
            init.cap.max_send_sge = 16;
            init.cap.max_recv_sge = 16;
            init.cap.max_inline_data = 256;
            let qp = verbs::ibv_create_qp(self.pd, &mut init);
            if qp.is_null() {
                return Err(Error::Io);
            }
            let mut attr: verbs::ibv_qp_attr = zeroed();
            attr.qp_state = verbs::ibv_qp_state::IBV_QPS_INIT;
            attr.port_num = self.port;
            attr.qp_access_flags = (verbs::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | verbs::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | verbs::ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | verbs::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC)
                .0;
            let flags = (verbs::ibv_qp_attr_mask::IBV_QP_STATE
                | verbs::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
                | verbs::ibv_qp_attr_mask::IBV_QP_PORT
                | verbs::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS)
                .0 as i32;
            if verbs::ibv_modify_qp(qp, &mut attr, flags) != 0 {
                verbs::ibv_destroy_qp(qp);
                return Err(Error::Io);
            }
            self.next_peer = self.next_peer.checked_add(1).ok_or(Error::NoMemory)?;
            let id = self.next_peer;
            let mut gid: verbs::ibv_gid = zeroed();
            if verbs::ibv_query_gid(self.context, self.port, self.gid_index, &mut gid) != 0 {
                verbs::ibv_destroy_qp(qp);
                return Err(Error::Io);
            }
            let exported = Box::new(PeerExchange {
                lid: self.lid,
                qpn: (*qp).qp_num,
                psn: id as u32 & 0xffffff,
                gid,
            });
            let metadata = (&*exported as *const PeerExchange).cast::<u8>();
            self.peers.insert(
                id,
                Peer {
                    qp,
                    exported,
                    connected: false,
                    remotes: HashMap::new(),
                },
            );
            Ok(PeerMetadata {
                peer: id,
                metadata: slice::from_raw_parts(metadata, size_of::<PeerExchange>()),
            })
        }
    }

    /// Complete the local side of a connection using the remote peer's metadata.
    /// Copy retained fields and move the QP to RTR/RTS; input bytes are borrowed for this call.
    pub fn connect_peer(&mut self, peer: PeerId, metadata: &[u8]) -> Result<(), Error> {
        if metadata.len() != size_of::<PeerExchange>() {
            return Err(Error::Invalid);
        }
        let remote = unsafe { ptr::read_unaligned(metadata.as_ptr().cast::<PeerExchange>()) };
        let local = self.peers.get_mut(&peer).ok_or(Error::Invalid)?;
        if local.connected {
            return Err(Error::InvalidState);
        }
        unsafe {
            let mut attr: verbs::ibv_qp_attr = zeroed();
            attr.qp_state = verbs::ibv_qp_state::IBV_QPS_RTR;
            attr.path_mtu = self.active_mtu.min(verbs::ibv_mtu::IBV_MTU_1024);
            attr.dest_qp_num = remote.qpn;
            attr.rq_psn = remote.psn;
            attr.max_dest_rd_atomic = 1;
            attr.min_rnr_timer = 1;
            attr.ah_attr.dlid = remote.lid;
            attr.ah_attr.port_num = self.port;
            if remote.gid.raw != [0; 16] {
                attr.ah_attr.is_global = 1;
                attr.ah_attr.grh.dgid = remote.gid;
                attr.ah_attr.grh.sgid_index = self.gid_index as u8;
                attr.ah_attr.grh.hop_limit = 1;
            }
            let flags = (verbs::ibv_qp_attr_mask::IBV_QP_STATE
                | verbs::ibv_qp_attr_mask::IBV_QP_AV
                | verbs::ibv_qp_attr_mask::IBV_QP_PATH_MTU
                | verbs::ibv_qp_attr_mask::IBV_QP_DEST_QPN
                | verbs::ibv_qp_attr_mask::IBV_QP_RQ_PSN
                | verbs::ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
                | verbs::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER)
                .0 as i32;
            if verbs::ibv_modify_qp(local.qp, &mut attr, flags) != 0 {
                return Err(Error::Io);
            }
            attr = zeroed();
            attr.qp_state = verbs::ibv_qp_state::IBV_QPS_RTS;
            attr.timeout = 14;
            attr.retry_cnt = 7;
            attr.rnr_retry = 7;
            attr.sq_psn = local.exported.psn;
            attr.max_rd_atomic = 1;
            let flags = (verbs::ibv_qp_attr_mask::IBV_QP_STATE
                | verbs::ibv_qp_attr_mask::IBV_QP_TIMEOUT
                | verbs::ibv_qp_attr_mask::IBV_QP_RETRY_CNT
                | verbs::ibv_qp_attr_mask::IBV_QP_RNR_RETRY
                | verbs::ibv_qp_attr_mask::IBV_QP_SQ_PSN
                | verbs::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC)
                .0 as i32;
            if verbs::ibv_modify_qp(local.qp, &mut attr, flags) != 0 {
                return Err(Error::Io);
            }
        }
        local.connected = true;
        Ok(())
    }

    /// Destroy a local peer and its imported memory descriptors.
    /// Stop its work, including unmatched receives, and finish local cleanup.
    /// After accesses end, publish Failed(Canceled) for tasks pending at shutdown;
    /// preserve existing terminal results, all batches, local MRs and other peers.
    /// On success destroy its QP/imports and invalidate its metadata and handle.
    /// On cleanup failure keep the handle for retry, but reject further use.
    /// Also handle INIT and failed setup.
    pub fn destroy_peer(&mut self, peer: PeerId) -> Result<(), Error> {
        let peer = self.peers.remove(&peer).ok_or(Error::Invalid)?;
        if unsafe { verbs::ibv_destroy_qp(peer.qp) } != 0 {
            return Err(Error::Io);
        }
        Ok(())
    }

    /// Submit requests as a new batch for background execution and return its nonzero ID.
    /// Either the whole batch is accepted or no tasks are accepted.
    ///
    /// # Safety
    /// Referenced buffers stay valid until terminal status. Registration is required
    /// except for Inline sources; these remain borrowed until terminal status too.
    /// Sources cannot be modified; destinations cannot be accessed while pending.
    pub unsafe fn submit_transfer(&mut self, requests: Vec<Request>) -> Result<BatchId, Error> {
        if requests.is_empty() {
            return Err(Error::Invalid);
        }
        for request in &requests {
            let peer = self.peers.get(&request.peer).ok_or(Error::Invalid)?;
            if !peer.connected {
                return Err(Error::InvalidState);
            }
            let (remote_address, inline_data) = match request.operation {
                Operation::Write {
                    remote_address,
                    inline_data,
                } => (remote_address, inline_data),
                _ => return Err(Error::Unsupported),
            };
            if request.length > u32::MAX as u64 {
                return Err(Error::Invalid);
            }
            let remote =
                range_remote(peer, remote_address, request.length).ok_or(Error::Invalid)?;
            if !remote.access.contains(MemoryAccess::REMOTE_WRITE) {
                return Err(Error::Invalid);
            }
            if inline_data {
                if request.length > 256 || (request.length != 0 && request.local_address.is_null())
                {
                    return Err(Error::Invalid);
                }
            } else if range_local(self, request.local_address as u64, request.length).is_none() {
                return Err(Error::Invalid);
            }
        }

        self.next_batch = self.next_batch.checked_add(1).ok_or(Error::NoMemory)?;
        let batch_id = self.next_batch;
        let mut statuses = vec![TransferStatus::Pending; requests.len()];
        for (index, request) in requests.iter().enumerate() {
            if request.length == 0 {
                statuses[index] = TransferStatus::Completed { receive: None };
                continue;
            }
            let peer = self.peers.get(&request.peer).unwrap();
            let (remote_address, inline_data) = match request.operation {
                Operation::Write {
                    remote_address,
                    inline_data,
                } => (remote_address, inline_data),
                _ => unreachable!(),
            };
            let remote = range_remote(peer, remote_address, request.length).unwrap();
            let mut sge = verbs::ibv_sge {
                addr: request.local_address as u64,
                length: request.length as u32,
                lkey: if inline_data {
                    0
                } else {
                    (*range_local(self, request.local_address as u64, request.length)
                        .unwrap()
                        .mr)
                        .lkey
                },
            };
            let mut wr: verbs::ibv_send_wr = zeroed();
            wr.wr_id = index as u64 + 1;
            wr.sg_list = &mut sge;
            wr.num_sge = 1;
            wr.opcode = verbs::ibv_wr_opcode::IBV_WR_RDMA_WRITE;
            wr.send_flags = verbs::ibv_send_flags::IBV_SEND_SIGNALED.0;
            if inline_data {
                wr.send_flags |= verbs::ibv_send_flags::IBV_SEND_INLINE.0;
            }
            wr.wr = verbs::wr_t {
                rdma: verbs::rdma_t {
                    remote_addr: remote_address,
                    rkey: remote.rkey,
                },
            };
            let mut bad = ptr::null_mut();
            if verbs::ibv_post_send(peer.qp, &mut wr, &mut bad) != 0 {
                statuses[index] = TransferStatus::Failed(Error::Io);
                continue;
            }
            let mut completion: verbs::ibv_wc = zeroed();
            loop {
                let count = verbs::ibv_poll_cq(self.cq, 1, &mut completion);
                if count < 0 {
                    statuses[index] = TransferStatus::Failed(Error::Io);
                    break;
                }
                if count == 1 {
                    statuses[index] = if completion.status == verbs::ibv_wc_status::IBV_WC_SUCCESS {
                        TransferStatus::Completed { receive: None }
                    } else {
                        TransferStatus::Failed(Error::Io)
                    };
                    break;
                }
                std::thread::yield_now();
            }
        }
        self.batches.insert(batch_id, Batch { statuses });
        Ok(batch_id)
    }

    /// Read the current status of every task in a batch, in submission order.
    /// The output slice must match the task count. This call does not advance transfers.
    /// Completed/Failed mean device accesses have ended and buffers are no longer borrowed.
    pub fn get_transfer_statuses(
        &self,
        batch: BatchId,
        statuses: &mut [TransferStatus],
    ) -> Result<(), Error> {
        let batch = self.batches.get(&batch).ok_or(Error::Invalid)?;
        if statuses.len() != batch.statuses.len() {
            return Err(Error::Invalid);
        }
        statuses.copy_from_slice(&batch.statuses);
        Ok(())
    }

    /// Release the batch's task records. Return Busy while any task is Pending.
    pub fn free_batch(&mut self, batch: BatchId) -> Result<(), Error> {
        let record = self.batches.get(&batch).ok_or(Error::Invalid)?;
        if record
            .statuses
            .iter()
            .any(|status| *status == TransferStatus::Pending)
        {
            return Err(Error::Busy);
        }
        self.batches.remove(&batch);
        Ok(())
    }
}

impl Drop for Engine {
    /// Release the engine's resources after stopping worker and device accesses.
    /// Cleanup must also handle partial initialization and unmatched receives.
    fn drop(&mut self) {
        unsafe {
            for (_, peer) in self.peers.drain() {
                verbs::ibv_destroy_qp(peer.qp);
            }
            for (_, memory) in self.locals.drain() {
                verbs::ibv_dereg_mr(memory.mr);
            }
            verbs::ibv_destroy_cq(self.cq);
            verbs::ibv_dealloc_pd(self.pd);
            verbs::ibv_close_device(self.context);
        }
    }
}
