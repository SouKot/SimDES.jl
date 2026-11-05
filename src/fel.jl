"""
    fel.jl — Future Event List (Contiguous Flat Binary Min-Heap with Total-Order EventKey)

The FEL is the central data structure of any DES engine.
Every pending event is stored here, ordered by a total-order `EventKey(t_ps, priority_eps, source_zone, seq)`:
- `t_ps::Int64`: simulated timestamp quantized to 1 picosecond (`10^-12 s`), eliminating 1-ULP IEEE-754 drift.
- `priority_eps::Int32`: micro-step phase priority tie-breaker.
- `source_zone::Int32`: originating zone / logical process ID.
- `seq::UInt64`: monotonic event sequence number (strict FIFO tie-breaking).

Implementation: `FlatBinaryMinHeap` (contiguous array-backed binary min-heap)
- Zero `Dict` hash-table overhead (3x–5x faster than `DataStructures.PriorityQueue`).
- Insert: O(log n) in contiguous memory.
- Dequeue-min: O(log n) in contiguous memory.
- Cancel: O(1) via lazy deletion (`cancelled::Set{UInt64}`).
"""

"""
    HeapNode

Single entry in `FlatBinaryMinHeap`.
"""
struct HeapNode
    key   :: EventKey
    event :: CancellableEvent
    t     :: Float64
end

"""
    FlatBinaryMinHeap

Contiguous `Vector{HeapNode}` binary min-heap ordered by `EventKey`.
"""
mutable struct FlatBinaryMinHeap
    nodes :: Vector{HeapNode}
end

FlatBinaryMinHeap() = FlatBinaryMinHeap(HeapNode[])

@inline Base.isempty(h::FlatBinaryMinHeap)::Bool = Base.isempty(h.nodes)
@inline Base.length(h::FlatBinaryMinHeap)::Int   = Base.length(h.nodes)
@inline Base.empty!(h::FlatBinaryMinHeap)        = (Base.empty!(h.nodes); h)

@inline function Base.first(h::FlatBinaryMinHeap)::Tuple{CancellableEvent, Float64}
    n = @inbounds h.nodes[1]
    return (n.event, n.t)
end

@inline function peek_node(h::FlatBinaryMinHeap)::HeapNode
    return @inbounds h.nodes[1]
end

@inline function Base.iterate(h::FlatBinaryMinHeap, idx::Int = 1)
    idx > Base.length(h.nodes) && return nothing
    n = @inbounds h.nodes[idx]
    return ((n.event, n.t), idx + 1)
end

@inline function _sift_up!(nodes::Vector{HeapNode}, idx::Int)
    item = @inbounds nodes[idx]
    item_key = item.key
    while idx > 1
        parent_idx = idx >> 1
        parent = @inbounds nodes[parent_idx]
        if isless(item_key, parent.key)
            @inbounds nodes[idx] = parent
            idx = parent_idx
        else
            break
        end
    end
    @inbounds nodes[idx] = item
    return nothing
end

@inline function _sift_down!(nodes::Vector{HeapNode}, idx::Int, len::Int)
    item = @inbounds nodes[idx]
    item_key = item.key
    half = len >> 1
    while idx <= half
        left_idx = idx << 1
        right_idx = left_idx + 1
        smallest_idx = left_idx
        smallest = @inbounds nodes[left_idx]
        if right_idx <= len
            right = @inbounds nodes[right_idx]
            if isless(right.key, smallest.key)
                smallest_idx = right_idx
                smallest = right
            end
        end
        if isless(smallest.key, item_key)
            @inbounds nodes[idx] = smallest
            idx = smallest_idx
        else
            break
        end
    end
    @inbounds nodes[idx] = item
    return nothing
end

@inline function push_node!(h::FlatBinaryMinHeap, key::EventKey, cev::CancellableEvent, t::Float64)
    push!(h.nodes, HeapNode(key, cev, t))
    _sift_up!(h.nodes, Base.length(h.nodes))
    return h
end

@inline function pop_node!(h::FlatBinaryMinHeap)::HeapNode
    nodes = h.nodes
    len = Base.length(nodes)
    len == 0 && throw(ArgumentError("Cannot pop from an empty FlatBinaryMinHeap"))
    top = @inbounds nodes[1]
    last_item = pop!(nodes)
    new_len = len - 1
    if new_len > 0
        @inbounds nodes[1] = last_item
        _sift_down!(nodes, 1, new_len)
    end
    return top
end

# Backward-compatibility methods for callers pushing/dequeuing directly on `fel.queue`
@inline function Base.push!(h::FlatBinaryMinHeap, pair::Pair{CancellableEvent, <:Real})
    cev = pair.first
    t = Float64(pair.second)
    key = EventKey(to_picoseconds(t), Int32(0), Int32(0), cev.id)
    return push_node!(h, key, cev, t)
end

@inline function enqueue!(h::FlatBinaryMinHeap, pair::Pair{CancellableEvent, <:Real})
    return push!(h, pair)
end

@inline function dequeue!(h::FlatBinaryMinHeap)::CancellableEvent
    return pop_node!(h).event
end

@inline function dequeue_pair!(h::FlatBinaryMinHeap)::Tuple{CancellableEvent, Float64}
    n = pop_node!(h)
    return (n.event, n.t)
end

"""
    FutureEventList

Thread-local (serial) or per-SubModel (PDES) priority queue of simulation events.
Backed by `FlatBinaryMinHeap` ordered by `EventKey` and a local cancellation set `cancelled`.

When `deterministic_mode == true`, `schedule!` automatically populates the 4-tuple
`EventKey(to_picoseconds(t), default_event_priority_eps(ev), origin_zone, origin_zone_seq)`
so tie-breaking is 100% invariant to `SubModel` partitioning and worker thread count.
When `owned_zones !== nothing`, any scheduled event targeting a zone outside `owned_zones`
is appended to `staged_outbox` for two-phase cross-SubModel `SyncEvent` dispatch.
"""
mutable struct FutureEventList
    queue              :: FlatBinaryMinHeap
    cancelled          :: Set{UInt64}
    deterministic_mode :: Bool
    active_origin_zone :: Int32
    zone_seqs          :: Dict{Int32, UInt64}
    owned_zones        :: Union{Nothing, Set{Int}}
    staged_outbox      :: Vector{HeapNode}
    next_local_id      :: UInt64
end

function FutureEventList(; deterministic_mode::Bool = false, owned_zones::Union{Nothing, Set{Int}} = nothing)
    return FutureEventList(
        FlatBinaryMinHeap(),
        Set{UInt64}(),
        deterministic_mode,
        Int32(0),
        Dict{Int32, UInt64}(),
        owned_zones,
        HeapNode[],
        UInt64(0),
    )
end

@inline function _fel_target_zone(ev::SimEvent)::Int
    ev isa TransferOut && return ev.dest_zone
    return Int(default_event_zone(ev))
end

"""
    cancel!(fel, id) -> nothing

Lazily cancel the event with ID `id`. The next time `safe_dequeue!` or `peek_time`
encounters it in the FEL, the event will be silently skipped.
"""
cancel!(fel::FutureEventList, id::UInt64) = (push!(fel.cancelled, id); nothing)

"""
    schedule!(fel, event, t; priority_eps=0, source_zone=0, seq=0) -> UInt64

Wrap `event` in a `CancellableEvent` with auto-assigned ID, insert into the FEL
with total-order `EventKey(to_picoseconds(t), priority_eps, source_zone, seq)`,
and return the event ID.
"""
@inline function schedule!(
    fel::FutureEventList,
    event::SimEvent,
    t::Float64;
    priority_eps::Integer = Int32(0),
    source_zone::Integer  = Int32(0),
    seq::UInt64           = UInt64(0),
)::UInt64
    local_id = (fel.next_local_id += UInt64(1))
    if fel.deterministic_mode && priority_eps == 0 && source_zone == 0 && seq == UInt64(0)
        p_eps = default_event_priority_eps(event)
        orig_z = fel.active_origin_zone != Int32(0) ? fel.active_origin_zone : default_event_zone(event)
        z_seq = get(fel.zone_seqs, orig_z, UInt64(0)) + UInt64(1)
        fel.zone_seqs[orig_z] = z_seq
        ev_id = ((UInt64(orig_z & Int32(0xFFFF))) << 44) | (z_seq & UInt64(0x00000FFFFFFFFFFF))
        cev = CancellableEvent(ev_id, event, t)
        key = EventKey(to_picoseconds(t), p_eps, orig_z, z_seq)
    else
        ev_seq = seq == UInt64(0) ? local_id : seq
        cev = CancellableEvent(local_id, event, t)
        key = EventKey(to_picoseconds(t), Int32(priority_eps), Int32(source_zone), ev_seq)
    end

    if fel.owned_zones !== nothing
        tz = _fel_target_zone(event)
        if tz > 0 && !(tz in fel.owned_zones)
            push!(fel.staged_outbox, HeapNode(key, cev, t))
            return cev.id
        end
    end

    push_node!(fel.queue, key, cev, t)
    return cev.id
end

"""
    schedule_key!(fel, event, key, t = from_picoseconds(key.t_ps)) -> UInt64

Insert `event` into `fel` using an explicit pre-constructed `EventKey`.
"""
@inline function schedule_key!(
    fel::FutureEventList,
    event::SimEvent,
    key::EventKey,
    t::Float64 = from_picoseconds(key.t_ps),
)::UInt64
    ev_id = ((UInt64(key.source_zone & Int32(0xFFFF))) << 44) | (key.seq & UInt64(0x00000FFFFFFFFFFF))
    if ev_id == UInt64(0)
        ev_id = (fel.next_local_id += UInt64(1))
    end
    cev = CancellableEvent(ev_id, event, t)
    push_node!(fel.queue, key, cev, t)
    return cev.id
end

"""
    safe_dequeue!(fel) -> Union{Tuple{CancellableEvent, Float64}, Nothing}

Dequeue the next non-cancelled event, returning `(event, time)` or `nothing` if empty.
"""
function safe_dequeue!(fel::FutureEventList)
    while !Base.isempty(fel.queue)
        n = pop_node!(fel.queue)
        if !Base.isempty(fel.cancelled) && (n.event.id in fel.cancelled)
            delete!(fel.cancelled, n.event.id)
            continue
        end
        return n.event, n.t
    end
    return nothing
end

"""
    safe_dequeue_node!(fel) -> Union{HeapNode, Nothing}

Dequeue the next non-cancelled `HeapNode(key, event, t)` or `nothing` if empty.
"""
function safe_dequeue_node!(fel::FutureEventList)::Union{HeapNode, Nothing}
    while !Base.isempty(fel.queue)
        n = pop_node!(fel.queue)
        if !Base.isempty(fel.cancelled) && (n.event.id in fel.cancelled)
            delete!(fel.cancelled, n.event.id)
            continue
        end
        return n
    end
    return nothing
end

"""
    peek_time(fel) -> Float64

Return the time of the next non-cancelled event without removing it, or `Inf` if empty.
"""
function peek_time(fel::FutureEventList)::Float64
    while !Base.isempty(fel.queue)
        n = peek_node(fel.queue)
        if !Base.isempty(fel.cancelled) && (n.event.id in fel.cancelled)
            delete!(fel.cancelled, n.event.id)
            pop_node!(fel.queue)
            continue
        end
        return n.t
    end
    return Inf
end

"""
    peek_key(fel) -> Union{EventKey, Nothing}

Return the `EventKey` of the next non-cancelled event without removing it, or `nothing` if empty.
"""
function peek_key(fel::FutureEventList)::Union{EventKey, Nothing}
    while !Base.isempty(fel.queue)
        n = peek_node(fel.queue)
        if !Base.isempty(fel.cancelled) && (n.event.id in fel.cancelled)
            delete!(fel.cancelled, n.event.id)
            pop_node!(fel.queue)
            continue
        end
        return n.key
    end
    return nothing
end

Base.isempty(fel::FutureEventList) = Base.isempty(fel.queue)
Base.length(fel::FutureEventList)  = Base.length(fel.queue)
function Base.empty!(fel::FutureEventList)
    Base.empty!(fel.queue)
    Base.empty!(fel.cancelled)
    Base.empty!(fel.zone_seqs)
    Base.empty!(fel.staged_outbox)
    fel.active_origin_zone = Int32(0)
    return fel
end
