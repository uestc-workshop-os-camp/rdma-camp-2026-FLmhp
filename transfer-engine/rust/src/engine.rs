#![allow(dead_code)]

#[allow(unused_imports)]
use rdma_mummy_sys as verbs;
use std::collections::{HashMap, VecDeque};
use std::ffi::c_void;
use std::ffi::{CStr, CString};
use std::mem::{size_of, zeroed};
use std::ops::BitOr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;
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
    inline_limit: u32,
    send_capacity: u32,
    inflight: u32,
    recv_capacity: u32,
    recv_posted: u32,
    recv_pending: VecDeque<ReceiveEntry>,
    recv_active: VecDeque<ReceiveEntry>,
    remotes: HashMap<RemoteMemoryId, RemoteMemory>,
}

struct Batch {
    statuses: Vec<TransferStatus>,
    requests: Vec<Request>,
    remaining: Vec<u32>,
    failed: Vec<bool>,
}

#[derive(Clone, Copy)]
struct ReceiveEntry {
    batch: BatchId,
    index: usize,
    peer: PeerId,
    address: u64,
    length: u64,
}

struct PendingBatch {
    batch: BatchId,
    index: usize,
    offset: u64,
}

#[derive(Clone, Copy)]
struct PostedWork {
    index: usize,
    slices: u32,
}

struct PostedChain {
    batch: BatchId,
    peer: PeerId,
    wr_count: u32,
    work: Vec<PostedWork>,
}

struct State {
    cq: *mut verbs::ibv_cq,
    locals: HashMap<MemoryId, LocalMemory>,
    peers: HashMap<PeerId, Peer>,
    batches: HashMap<BatchId, Batch>,
    pending: VecDeque<PendingBatch>,
    posted: HashMap<u64, PostedChain>,
    next_memory: MemoryId,
    next_peer: PeerId,
    next_remote: RemoteMemoryId,
    next_batch: BatchId,
    next_chain: u64,
}

// All verbs objects are serialized by State's mutex and used until the worker joins.
unsafe impl Send for State {}

struct Shared {
    state: Mutex<State>,
    stop: AtomicBool,
}

pub struct Engine {
    port: u8,
    gid_index: i32,
    active_mtu: verbs::ibv_mtu::Type,
    lid: u16,
    context: *mut verbs::ibv_context,
    pd: *mut verbs::ibv_pd,
    cq: *mut verbs::ibv_cq,
    shared: Arc<Shared>,
    worker: Option<JoinHandle<()>>,
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

fn range_local(state: &State, address: u64, length: u64) -> Option<&LocalMemory> {
    state.locals.values().find(|memory| {
        contains(
            memory.exported.address,
            memory.exported.length,
            address,
            length,
        )
    })
}

fn range_local_id(state: &State, address: u64, length: u64) -> Option<MemoryId> {
    state.locals.iter().find_map(|(&id, memory)| {
        contains(
            memory.exported.address,
            memory.exported.length,
            address,
            length,
        )
        .then_some(id)
    })
}

fn range_remote(peer: &Peer, address: u64, length: u64) -> Option<RemoteMemory> {
    peer.remotes
        .values()
        .copied()
        .find(|memory| contains(memory.address, memory.length, address, length))
}

fn task_slices(request: Request) -> u32 {
    match request.operation {
        Operation::Write { .. } | Operation::Read { .. } => {
            request.length.div_ceil(64 * 1024) as u32
        }
        Operation::WriteImm { .. } => request.length.div_ceil(64 * 1024).max(1) as u32,
        Operation::Send { .. }
        | Operation::Recv
        | Operation::CompareSwap { .. }
        | Operation::FetchAdd { .. } => 1,
    }
}

fn lock_state(shared: &Shared) -> Result<MutexGuard<'_, State>, Error> {
    shared.state.lock().map_err(|_| Error::Io)
}

fn validate_request(state: &State, request: Request) -> Result<(), Error> {
    let peer = state.peers.get(&request.peer).ok_or(Error::Invalid)?;
    let inline = matches!(
        request.operation,
        Operation::Write {
            inline_data: true,
            ..
        } | Operation::Send { inline_data: true }
            | Operation::WriteImm {
                inline_data: true,
                ..
            }
    );
    if !matches!(request.operation, Operation::Recv) && !peer.connected {
        return Err(Error::InvalidState);
    }
    if inline && request.length > peer.inline_limit as u64 {
        return Err(Error::Unsupported);
    }
    if request.length == 0
        && !matches!(
            request.operation,
            Operation::CompareSwap { .. } | Operation::FetchAdd { .. }
        )
    {
        return Ok(());
    }
    if request.local_address.is_null() {
        return Err(Error::Invalid);
    }
    match request.operation {
        Operation::CompareSwap { remote_address, .. }
        | Operation::FetchAdd { remote_address, .. } => {
            if request.length != 8
                || request.local_address as u64 & 7 != 0
                || remote_address & 7 != 0
            {
                return Err(Error::Invalid);
            }
            let local = range_local(state, request.local_address as u64, 8)
                .filter(|memory| memory.exported.access & MemoryAccess::LOCAL_WRITE.0 != 0)
                .ok_or(Error::Invalid)?;
            let _ = local;
            range_remote(peer, remote_address, 8)
                .filter(|memory| memory.access.contains(MemoryAccess::REMOTE_ATOMIC))
                .ok_or(Error::Invalid)?;
        }
        Operation::Write { remote_address, .. } | Operation::WriteImm { remote_address, .. } => {
            if !inline && range_local(state, request.local_address as u64, request.length).is_none()
            {
                return Err(Error::Invalid);
            }
            range_remote(peer, remote_address, request.length)
                .filter(|memory| memory.access.contains(MemoryAccess::REMOTE_WRITE))
                .ok_or(Error::Invalid)?;
        }
        Operation::Read { remote_address } => {
            range_local(state, request.local_address as u64, request.length)
                .filter(|memory| memory.exported.access & MemoryAccess::LOCAL_WRITE.0 != 0)
                .ok_or(Error::Invalid)?;
            range_remote(peer, remote_address, request.length)
                .filter(|memory| memory.access.contains(MemoryAccess::REMOTE_READ))
                .ok_or(Error::Invalid)?;
        }
        Operation::Send { .. } => {
            if request.length > u32::MAX as u64
                || (!inline
                    && range_local(state, request.local_address as u64, request.length).is_none())
            {
                return Err(Error::Invalid);
            }
        }
        Operation::Recv => {
            if request.length > u32::MAX as u64 {
                return Err(Error::Invalid);
            }
            range_local(state, request.local_address as u64, request.length)
                .filter(|memory| memory.exported.access & MemoryAccess::LOCAL_WRITE.0 != 0)
                .ok_or(Error::Invalid)?;
        }
    }
    Ok(())
}

fn fail_task(state: &mut State, batch_id: BatchId, index: usize, error: Error) {
    if let Some(batch) = state.batches.get_mut(&batch_id) {
        if index < batch.statuses.len() && batch.statuses[index] == TransferStatus::Pending {
            batch.statuses[index] = TransferStatus::Failed(error);
            batch.remaining[index] = 0;
            batch.failed[index] = true;
        }
    }
}

unsafe fn post_receives(state: &mut State) {
    let peers: Vec<_> = state.peers.keys().copied().collect();
    for peer_id in peers {
        loop {
            let entry = match state.peers.get_mut(&peer_id) {
                Some(peer) if peer.recv_posted < peer.recv_capacity => {
                    peer.recv_pending.pop_front()
                }
                _ => None,
            };
            let Some(entry) = entry else { break };
            let mut sge: verbs::ibv_sge = zeroed();
            let mut wr: verbs::ibv_recv_wr = zeroed();
            wr.wr_id = (1_u64 << 63) | (entry.batch << 32) | entry.index as u64;
            let valid = if entry.length == 0 {
                true
            } else if let Some(memory) = range_local(state, entry.address, entry.length) {
                sge.addr = entry.address;
                sge.length = entry.length as u32;
                sge.lkey = (*memory.mr).lkey;
                wr.sg_list = &mut sge;
                wr.num_sge = 1;
                true
            } else {
                false
            };
            let peer = state.peers.get_mut(&peer_id).unwrap();
            let mut bad = ptr::null_mut();
            if !valid || verbs::ibv_post_recv(peer.qp, &mut wr, &mut bad) != 0 {
                fail_task(state, entry.batch, entry.index, Error::InvalidState);
                continue;
            }
            peer.recv_posted += 1;
            peer.recv_active.push_back(entry);
        }
    }
}

#[derive(Clone, Copy)]
struct WorkSpec {
    index: usize,
    offset: u64,
    length: u64,
    slices: u32,
    final_for_task: bool,
}

fn complete_send(state: &mut State, completion: &verbs::ibv_wc) {
    let Some(chain) = state.posted.remove(&completion.wr_id) else {
        return;
    };
    if let Some(peer) = state.peers.get_mut(&chain.peer) {
        peer.inflight = peer.inflight.saturating_sub(chain.wr_count);
    }
    if let Some(batch) = state.batches.get_mut(&chain.batch) {
        for work in chain.work {
            if work.index >= batch.statuses.len()
                || batch.statuses[work.index] != TransferStatus::Pending
            {
                continue;
            }
            if completion.status != verbs::ibv_wc_status::IBV_WC_SUCCESS {
                batch.failed[work.index] = true;
            }
            batch.remaining[work.index] = batch.remaining[work.index].saturating_sub(work.slices);
            if batch.remaining[work.index] == 0 {
                batch.statuses[work.index] = if batch.failed[work.index] {
                    TransferStatus::Failed(Error::Io)
                } else {
                    TransferStatus::Completed { receive: None }
                };
            }
        }
    }
}

unsafe fn complete_receive(state: &mut State, completion: &verbs::ibv_wc) {
    let batch_id = (completion.wr_id & !(1_u64 << 63)) >> 32;
    let index = completion.wr_id as u32 as usize;
    let mut found_peer = None;
    for (&peer_id, peer) in &mut state.peers {
        if let Some(position) = peer
            .recv_active
            .iter()
            .position(|entry| entry.batch == batch_id && entry.index == index)
        {
            peer.recv_active.remove(position);
            peer.recv_posted = peer.recv_posted.saturating_sub(1);
            found_peer = Some(peer_id);
            break;
        }
    }
    if found_peer.is_none() {
        return;
    }
    let Some(batch) = state.batches.get_mut(&batch_id) else {
        return;
    };
    if index >= batch.statuses.len() || batch.statuses[index] != TransferStatus::Pending {
        return;
    }
    batch.remaining[index] = 0;
    batch.statuses[index] = if completion.status != verbs::ibv_wc_status::IBV_WC_SUCCESS {
        TransferStatus::Failed(Error::Io)
    } else if completion.opcode == verbs::ibv_wc_opcode::IBV_WC_RECV_RDMA_WITH_IMM {
        TransferStatus::Completed {
            receive: Some(ReceiveResult::WriteNotice {
                immediate: u32::from_be(completion.imm_data_invalidated_rkey_union.imm_data),
            }),
        }
    } else {
        TransferStatus::Completed {
            receive: Some(ReceiveResult::Message {
                length: completion.byte_len as u64,
            }),
        }
    };
}

unsafe fn post_one_chain(state: &mut State) -> bool {
    let Some(mut pending) = state.pending.pop_front() else {
        return false;
    };
    loop {
        let Some(batch) = state.batches.get(&pending.batch) else {
            return true;
        };
        if pending.index >= batch.requests.len() {
            return true;
        }
        if batch.statuses[pending.index] != TransferStatus::Pending {
            pending.index += 1;
            pending.offset = 0;
            continue;
        }
        let request = batch.requests[pending.index];
        if matches!(request.operation, Operation::Recv) {
            let entry = ReceiveEntry {
                batch: pending.batch,
                index: pending.index,
                peer: request.peer,
                address: request.local_address as u64,
                length: request.length,
            };
            if let Some(peer) = state.peers.get_mut(&request.peer) {
                peer.recv_pending.push_back(entry);
            } else {
                fail_task(state, pending.batch, pending.index, Error::Canceled);
            }
            pending.index += 1;
            pending.offset = 0;
            continue;
        }
        break;
    }

    let first_request = state.batches[&pending.batch].requests[pending.index];
    let Some(peer) = state.peers.get(&first_request.peer) else {
        fail_task(state, pending.batch, pending.index, Error::Canceled);
        pending.index += 1;
        state.pending.push_back(pending);
        return true;
    };
    if !peer.connected {
        fail_task(state, pending.batch, pending.index, Error::InvalidState);
        pending.index += 1;
        state.pending.push_back(pending);
        return true;
    }
    let capacity = (peer.send_capacity - peer.inflight).min(16) as usize;
    if capacity == 0 {
        state.pending.push_back(pending);
        return false;
    }
    let peer_id = first_request.peer;
    let mut next_index = pending.index;
    let mut next_offset = pending.offset;
    let mut specs = Vec::with_capacity(capacity);
    while specs.len() < capacity {
        let batch = &state.batches[&pending.batch];
        if next_index >= batch.requests.len() {
            break;
        }
        if batch.statuses[next_index] != TransferStatus::Pending {
            next_index += 1;
            next_offset = 0;
            continue;
        }
        let request = batch.requests[next_index];
        if request.peer != peer_id || matches!(request.operation, Operation::Recv) {
            break;
        }
        let max_length = 16 * 64 * 1024;
        let remaining = request.length.saturating_sub(next_offset);
        let length = remaining.min(max_length);
        let slices = if length == 0 {
            1
        } else {
            length.div_ceil(64 * 1024) as u32
        };
        let final_for_task = next_offset + length >= request.length;
        specs.push(WorkSpec {
            index: next_index,
            offset: next_offset,
            length,
            slices,
            final_for_task,
        });
        if final_for_task {
            next_index += 1;
            next_offset = 0;
        } else {
            next_offset += length;
        }
    }
    if specs.is_empty() {
        state.pending.push_back(pending);
        return false;
    }

    state.next_chain = match state.next_chain.checked_add(1) {
        Some(id) => id,
        None => {
            fail_task(state, pending.batch, pending.index, Error::NoMemory);
            return true;
        }
    };
    let chain_id = state.next_chain;
    let batch = &state.batches[&pending.batch];
    let mut wrs: Vec<verbs::ibv_send_wr> = (0..specs.len()).map(|_| zeroed()).collect();
    let sge_count: usize = specs.iter().map(|spec| spec.slices as usize).sum();
    let mut sges: Vec<verbs::ibv_sge> = (0..sge_count).map(|_| zeroed()).collect();
    let mut sge_index = 0;
    for (wr_index, spec) in specs.iter().enumerate() {
        let request = batch.requests[spec.index];
        let peer = &state.peers[&peer_id];
        let inline = matches!(
            request.operation,
            Operation::Write {
                inline_data: true,
                ..
            } | Operation::Send { inline_data: true }
                | Operation::WriteImm {
                    inline_data: true,
                    ..
                }
        );
        let lkey = if inline || spec.length == 0 {
            0
        } else {
            (*range_local(
                state,
                request.local_address as u64 + spec.offset,
                spec.length,
            )
            .unwrap()
            .mr)
                .lkey
        };
        let first_sge = sge_index;
        let mut described = 0;
        while described < spec.length {
            let length = (spec.length - described).min(64 * 1024);
            sges[sge_index] = verbs::ibv_sge {
                addr: request.local_address as u64 + spec.offset + described,
                length: length as u32,
                lkey,
            };
            sge_index += 1;
            described += length;
        }
        let wr = &mut wrs[wr_index];
        wr.wr_id = chain_id;
        if spec.length != 0 {
            wr.sg_list = sges.as_mut_ptr().add(first_sge);
            wr.num_sge = (sge_index - first_sge) as i32;
        }
        if inline {
            wr.send_flags |= verbs::ibv_send_flags::IBV_SEND_INLINE.0;
        }
        match request.operation {
            Operation::Write { remote_address, .. } => {
                let remote = range_remote(peer, remote_address + spec.offset, spec.length).unwrap();
                wr.opcode = verbs::ibv_wr_opcode::IBV_WR_RDMA_WRITE;
                wr.wr = verbs::wr_t {
                    rdma: verbs::rdma_t {
                        remote_addr: remote_address + spec.offset,
                        rkey: remote.rkey,
                    },
                };
            }
            Operation::Read { remote_address } => {
                let remote = range_remote(peer, remote_address + spec.offset, spec.length).unwrap();
                wr.opcode = verbs::ibv_wr_opcode::IBV_WR_RDMA_READ;
                wr.wr = verbs::wr_t {
                    rdma: verbs::rdma_t {
                        remote_addr: remote_address + spec.offset,
                        rkey: remote.rkey,
                    },
                };
            }
            Operation::Send { .. } => wr.opcode = verbs::ibv_wr_opcode::IBV_WR_SEND,
            Operation::WriteImm {
                remote_address,
                immediate,
                ..
            } => {
                let rkey = if spec.length == 0 {
                    0
                } else {
                    range_remote(peer, remote_address + spec.offset, spec.length)
                        .unwrap()
                        .rkey
                };
                wr.opcode = if spec.final_for_task {
                    verbs::ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM
                } else {
                    verbs::ibv_wr_opcode::IBV_WR_RDMA_WRITE
                };
                if spec.final_for_task {
                    wr.imm_data_invalidated_rkey_union = verbs::imm_data_invalidated_rkey_union_t {
                        imm_data: immediate.to_be(),
                    };
                }
                wr.wr = verbs::wr_t {
                    rdma: verbs::rdma_t {
                        remote_addr: remote_address + spec.offset,
                        rkey,
                    },
                };
            }
            Operation::CompareSwap {
                remote_address,
                compare,
                value,
            } => {
                let remote = range_remote(peer, remote_address, 8).unwrap();
                wr.opcode = verbs::ibv_wr_opcode::IBV_WR_ATOMIC_CMP_AND_SWP;
                wr.wr = verbs::wr_t {
                    atomic: verbs::atomic_t {
                        remote_addr: remote_address,
                        compare_add: compare,
                        swap: value,
                        rkey: remote.rkey,
                    },
                };
            }
            Operation::FetchAdd {
                remote_address,
                value,
            } => {
                let remote = range_remote(peer, remote_address, 8).unwrap();
                wr.opcode = verbs::ibv_wr_opcode::IBV_WR_ATOMIC_FETCH_AND_ADD;
                wr.wr = verbs::wr_t {
                    atomic: verbs::atomic_t {
                        remote_addr: remote_address,
                        compare_add: value,
                        swap: 0,
                        rkey: remote.rkey,
                    },
                };
            }
            Operation::Recv => unreachable!(),
        }
    }
    for index in 0..wrs.len() - 1 {
        wrs[index].next = &mut wrs[index + 1];
    }
    wrs.last_mut().unwrap().send_flags |= verbs::ibv_send_flags::IBV_SEND_SIGNALED.0;
    let mut bad = ptr::null_mut();
    let qp = state.peers[&peer_id].qp;
    if verbs::ibv_post_send(qp, wrs.as_mut_ptr(), &mut bad) != 0 {
        for spec in &specs {
            fail_task(state, pending.batch, spec.index, Error::Io);
        }
    } else {
        state.peers.get_mut(&peer_id).unwrap().inflight += specs.len() as u32;
        state.posted.insert(
            chain_id,
            PostedChain {
                batch: pending.batch,
                peer: peer_id,
                wr_count: specs.len() as u32,
                work: specs
                    .iter()
                    .map(|spec| PostedWork {
                        index: spec.index,
                        slices: spec.slices,
                    })
                    .collect(),
            },
        );
    }
    pending.index = next_index;
    pending.offset = next_offset;
    if pending.index < state.batches[&pending.batch].requests.len() {
        state.pending.push_back(pending);
    }
    true
}

fn worker_main(shared: Arc<Shared>) {
    while !shared.stop.load(Ordering::Acquire) {
        if let Ok(mut state) = shared.state.lock() {
            unsafe {
                post_receives(&mut state);
                for _ in 0..state.pending.len().max(1) {
                    if !post_one_chain(&mut state) {
                        break;
                    }
                }
                let mut completions: [verbs::ibv_wc; 16] = zeroed();
                let count = verbs::ibv_poll_cq(
                    state.cq,
                    completions.len() as i32,
                    completions.as_mut_ptr(),
                );
                if count > 0 {
                    for completion in &completions[..count as usize] {
                        if completion.wr_id & (1_u64 << 63) != 0 {
                            complete_receive(&mut state, completion);
                        } else {
                            complete_send(&mut state, completion);
                        }
                    }
                }
            }
        }
        thread::sleep(Duration::from_micros(50));
    }
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
            let shared = Arc::new(Shared {
                state: Mutex::new(State {
                    cq,
                    locals: HashMap::new(),
                    peers: HashMap::new(),
                    batches: HashMap::new(),
                    pending: VecDeque::new(),
                    posted: HashMap::new(),
                    next_memory: 0,
                    next_peer: 0,
                    next_remote: 0,
                    next_batch: 0,
                    next_chain: 0,
                }),
                stop: AtomicBool::new(false),
            });
            let worker_shared = Arc::clone(&shared);
            let worker = match thread::Builder::new()
                .name("rdma101-progress".into())
                .spawn(move || worker_main(worker_shared))
            {
                Ok(worker) => worker,
                Err(_) => {
                    verbs::ibv_destroy_cq(cq);
                    verbs::ibv_dealloc_pd(pd);
                    verbs::ibv_close_device(context);
                    return Err(Error::NoMemory);
                }
            };
            Ok(Box::new(Self {
                port,
                gid_index,
                active_mtu: attr.active_mtu,
                lid: attr.lid,
                context,
                pd,
                cq,
                shared,
                worker: Some(worker),
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
        let mut state = match lock_state(&self.shared) {
            Ok(state) => state,
            Err(error) => {
                verbs::ibv_dereg_mr(mr);
                return Err(error);
            }
        };
        state.next_memory = state.next_memory.checked_add(1).ok_or_else(|| {
            verbs::ibv_dereg_mr(mr);
            Error::NoMemory
        })?;
        let id = state.next_memory;
        let exported = Box::new(MemoryExchange {
            address: address as u64,
            length: length as u64,
            rkey: (*mr).rkey,
            access: access.0,
        });
        let metadata = (&*exported as *const MemoryExchange).cast::<u8>();
        state.locals.insert(id, LocalMemory { mr, exported });
        drop(state);
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
        let mut state = lock_state(&self.shared)?;
        let local = state.locals.get(&memory).ok_or(Error::Invalid)?;
        let busy = state.batches.values().any(|batch| {
            batch.requests.iter().enumerate().any(|(index, request)| {
                if batch.statuses[index] != TransferStatus::Pending || request.length == 0 {
                    return false;
                }
                let inline = matches!(
                    request.operation,
                    Operation::Write {
                        inline_data: true,
                        ..
                    } | Operation::Send { inline_data: true }
                        | Operation::WriteImm {
                            inline_data: true,
                            ..
                        }
                );
                !inline
                    && contains(
                        local.exported.address,
                        local.exported.length,
                        request.local_address as u64,
                        request.length,
                    )
            })
        });
        if busy {
            return Err(Error::Busy);
        }
        let local = state.locals.remove(&memory).unwrap();
        if verbs::ibv_dereg_mr(local.mr) != 0 {
            state.locals.insert(memory, local);
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
        let mut state = lock_state(&self.shared)?;
        let local_peer = state.peers.get(&peer).ok_or(Error::Invalid)?;
        if !local_peer.connected {
            return Err(Error::InvalidState);
        }
        for remote in local_peer.remotes.values() {
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
        state.next_remote = state.next_remote.checked_add(1).ok_or(Error::NoMemory)?;
        let id = state.next_remote;
        state.peers.get_mut(&peer).unwrap().remotes.insert(
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
        let mut state = lock_state(&self.shared)?;
        let Some((peer_id, remote)) = state.peers.iter().find_map(|(&peer_id, peer)| {
            peer.remotes
                .get(&memory)
                .copied()
                .map(|remote| (peer_id, remote))
        }) else {
            return Err(Error::Invalid);
        };
        let busy = state.batches.values().any(|batch| {
            batch.requests.iter().enumerate().any(|(index, request)| {
                if batch.statuses[index] != TransferStatus::Pending || request.peer != peer_id {
                    return false;
                }
                let address = match request.operation {
                    Operation::Write { remote_address, .. }
                    | Operation::Read { remote_address }
                    | Operation::WriteImm { remote_address, .. }
                    | Operation::CompareSwap { remote_address, .. }
                    | Operation::FetchAdd { remote_address, .. } => remote_address,
                    Operation::Send { .. } | Operation::Recv => return false,
                };
                request.length != 0
                    && contains(remote.address, remote.length, address, request.length)
            })
        });
        if busy {
            return Err(Error::Busy);
        }
        state
            .peers
            .get_mut(&peer_id)
            .unwrap()
            .remotes
            .remove(&memory);
        Ok(())
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
            let mut gid: verbs::ibv_gid = zeroed();
            if verbs::ibv_query_gid(self.context, self.port, self.gid_index, &mut gid) != 0 {
                verbs::ibv_destroy_qp(qp);
                return Err(Error::Io);
            }
            let mut state = match lock_state(&self.shared) {
                Ok(state) => state,
                Err(error) => {
                    verbs::ibv_destroy_qp(qp);
                    return Err(error);
                }
            };
            state.next_peer = match state.next_peer.checked_add(1) {
                Some(id) => id,
                None => {
                    verbs::ibv_destroy_qp(qp);
                    return Err(Error::NoMemory);
                }
            };
            let id = state.next_peer;
            let exported = Box::new(PeerExchange {
                lid: self.lid,
                qpn: (*qp).qp_num,
                psn: id as u32 & 0xffffff,
                gid,
            });
            let metadata = (&*exported as *const PeerExchange).cast::<u8>();
            state.peers.insert(
                id,
                Peer {
                    qp,
                    exported,
                    connected: false,
                    inline_limit: init.cap.max_inline_data,
                    send_capacity: init.cap.max_send_wr,
                    inflight: 0,
                    recv_capacity: init.cap.max_recv_wr,
                    recv_posted: 0,
                    recv_pending: VecDeque::new(),
                    recv_active: VecDeque::new(),
                    remotes: HashMap::new(),
                },
            );
            drop(state);
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
        let mut state = lock_state(&self.shared)?;
        let local = state.peers.get_mut(&peer).ok_or(Error::Invalid)?;
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
        let mut state = lock_state(&self.shared)?;
        let qp = state.peers.get_mut(&peer).ok_or(Error::Invalid)?.qp;
        state.peers.get_mut(&peer).unwrap().connected = false;
        for batch in state.batches.values_mut() {
            for index in 0..batch.requests.len() {
                if batch.requests[index].peer == peer
                    && batch.statuses[index] == TransferStatus::Pending
                {
                    batch.statuses[index] = TransferStatus::Failed(Error::Canceled);
                    batch.remaining[index] = 0;
                    batch.failed[index] = true;
                }
            }
        }
        state.posted.retain(|_, chain| chain.peer != peer);
        if unsafe { verbs::ibv_destroy_qp(qp) } != 0 {
            return Err(Error::Io);
        }
        state.peers.remove(&peer);
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
        let mut state = lock_state(&self.shared)?;
        for &request in &requests {
            validate_request(&state, request)?;
        }
        state.next_batch = state.next_batch.checked_add(1).ok_or(Error::NoMemory)?;
        let batch_id = state.next_batch;
        let mut statuses = vec![TransferStatus::Pending; requests.len()];
        let mut remaining = Vec::with_capacity(requests.len());
        for (index, &request) in requests.iter().enumerate() {
            if request.length == 0
                && !matches!(
                    request.operation,
                    Operation::Recv | Operation::Send { .. } | Operation::WriteImm { .. }
                )
            {
                statuses[index] = TransferStatus::Completed { receive: None };
                remaining.push(0);
            } else {
                remaining.push(task_slices(request));
            }
        }
        let count = requests.len();
        state.batches.insert(
            batch_id,
            Batch {
                statuses,
                requests,
                remaining,
                failed: vec![false; count],
            },
        );
        state.pending.push_back(PendingBatch {
            batch: batch_id,
            index: 0,
            offset: 0,
        });
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
        let state = lock_state(&self.shared)?;
        let record = state.batches.get(&batch).ok_or(Error::Invalid)?;
        if statuses.len() != record.statuses.len() {
            return Err(Error::Invalid);
        }
        statuses.copy_from_slice(&record.statuses);
        Ok(())
    }

    /// Release the batch's task records. Return Busy while any task is Pending.
    pub fn free_batch(&mut self, batch: BatchId) -> Result<(), Error> {
        let mut state = lock_state(&self.shared)?;
        let record = state.batches.get(&batch).ok_or(Error::Invalid)?;
        if record.statuses.contains(&TransferStatus::Pending) {
            return Err(Error::Busy);
        }
        state.batches.remove(&batch);
        Ok(())
    }
}

impl Drop for Engine {
    /// Release the engine's resources after stopping worker and device accesses.
    /// Cleanup must also handle partial initialization and unmatched receives.
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        unsafe {
            if let Ok(mut state) = self.shared.state.lock() {
                for (_, peer) in state.peers.drain() {
                    verbs::ibv_destroy_qp(peer.qp);
                }
                for (_, memory) in state.locals.drain() {
                    verbs::ibv_dereg_mr(memory.mr);
                }
            }
            verbs::ibv_destroy_cq(self.cq);
            verbs::ibv_dealloc_pd(self.pd);
            verbs::ibv_close_device(self.context);
        }
    }
}
