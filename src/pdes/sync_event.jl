"""
    sync_event.jl — Two-Phase Cross-SubModel SyncEvent & LockFreeMPSCQueue

Implements InControl ERS's two-phase `ISyncEvent` handoff (`on_sender_side!` and
`on_target_side!`) together with `libdes`'s multi-producer single-consumer (MPSC)
cross-partition inbox queue (`LockFreeMPSCQueue`).

When an entity or signal crosses from `SubModel` `A` (`sender_world`) to `SubModel` `B`
(`target_world`):
1. `on_sender_side!` runs immediately inside Worker `A`'s execution loop while `A`
   exclusively owns `sender_world`, extracting and detaching the entity's metadata
   (`entry_time`, `DESAgent`, `entity_attributes`, `entity_visuals`, `_pending_arrivals`)
   into an immutable `CrossSubModelSyncEvent` packet and pushing it into `B`'s `inbox`.
2. `on_target_side!` runs inside Worker `B` during Phase 1 (inbox drain) while `B`
   exclusively owns `target_world`, materializing the entity's metadata into `target_world`
   and inserting the inner `SimEvent` into `B`'s local `FutureEventList` with its exact
   4-tuple `EventKey`.
"""

@inline _event_entity_id(::SimEvent)::UInt64             = UInt64(0)
@inline _event_entity_id(e::EntityArrival)::UInt64       = e.entity_id
@inline _event_entity_id(e::ProcessComplete)::UInt64     = e.entity_id
@inline _event_entity_id(e::TransferOut)::UInt64         = e.entity_id
@inline _event_entity_id(e::LoadingStart)::UInt64        = e.item_id
@inline _event_entity_id(e::LoadingComplete)::UInt64     = e.item_id
@inline _event_entity_id(e::UnloadingStart)::UInt64      = e.item_id
@inline _event_entity_id(e::UnloadingComplete)::UInt64   = e.item_id

@inline function _event_target_zone(ev::SimEvent)::Int
    ev isa TransferOut && return ev.dest_zone
    return Int(default_event_zone(ev))
end

"""
    CrossSubModelSyncEvent

Immutable cross-SubModel handoff packet transferred via `LockFreeMPSCQueue`.
"""
struct CrossSubModelSyncEvent
    key              :: EventKey
    inner            :: SimEvent
    t                :: Float64
    source_sm        :: Int
    target_sm        :: Int
    target_zone      :: Int
    entity_id        :: UInt64
    has_entry_time   :: Bool
    entry_time       :: Float64
    des_agent        :: Union{Nothing, DESAgent}
    attributes       :: Union{Nothing, Dict{String, Any}}
    visuals          :: Union{Nothing, Dict{String, Any}}
    route_override   :: Union{Nothing, Int}
    reserved_pending :: Bool
end

"""
    on_sender_side!(sender_world, source_sm, target_sm, key, ev, t) -> CrossSubModelSyncEvent

Phase 1 of cross-SubModel event transfer (executed by the sending `SubModel`):
snapshots and removes the migrating entity's state from `sender_world` so that
`sender_world` and `target_world` never share mutable entity dictionaries.
"""
function on_sender_side!(
    sender_world::SimWorld,
    source_sm::Int,
    target_sm::Int,
    key::EventKey,
    ev::SimEvent,
    t::Float64,
)::CrossSubModelSyncEvent
    target_zone = _event_target_zone(ev)
    uid = _event_entity_id(ev)

    has_entry = false
    entry_t = t
    ag = nothing
    attrs = nothing
    vis = nothing
    ro = nothing
    reserved = false

    if uid > UInt64(0)
        if haskey(sender_world.entry_times, uid)
            has_entry = true
            entry_t = pop!(sender_world.entry_times, uid)
        end
        if haskey(sender_world.des_agents, uid)
            ag = pop!(sender_world.des_agents, uid)
        end
        if !isempty(sender_world.entity_attributes) && haskey(sender_world.entity_attributes, uid)
            attrs = copy(pop!(sender_world.entity_attributes, uid))
        end
        if !isempty(sender_world.entity_visuals) && haskey(sender_world.entity_visuals, uid)
            vis = copy(pop!(sender_world.entity_visuals, uid))
        end
        if !isempty(sender_world.entity_route_overrides) && haskey(sender_world.entity_route_overrides, uid)
            ro = pop!(sender_world.entity_route_overrides, uid)
        end
        if !isempty(sender_world.entity_kinematics)
            delete!(sender_world.entity_kinematics, uid)
        end
    end

    if target_zone > 0 && !isempty(sender_world.zone_attributes)
        pending = SimCore.get_zone_attribute(sender_world, target_zone, "_pending_arrivals", nothing)
        if pending !== nothing && uid in pending
            delete!(pending, uid)
            reserved = true
        end
    end

    return CrossSubModelSyncEvent(
        key,
        ev,
        t,
        source_sm,
        target_sm,
        target_zone,
        uid,
        has_entry,
        entry_t,
        ag,
        attrs,
        vis,
        ro,
        reserved,
    )
end

"""
    on_target_side!(sync_ev, target_world, target_fel) -> UInt64

Phase 2 of cross-SubModel event transfer (executed by the receiving `SubModel` when
draining its inbox): restores the migrating entity's state into `target_world` and
enqueues `sync_ev.inner` into `target_fel` with its exact `EventKey`.
"""
function on_target_side!(
    sync_ev::CrossSubModelSyncEvent,
    target_world::SimWorld,
    target_fel::FutureEventList,
)::UInt64
    uid = sync_ev.entity_id
    if uid > UInt64(0)
        if sync_ev.has_entry_time
            target_world.entry_times[uid] = sync_ev.entry_time
        end
        if sync_ev.des_agent !== nothing
            target_world.des_agents[uid] = sync_ev.des_agent
        end
        if sync_ev.attributes !== nothing
            target_world.entity_attributes[uid] = sync_ev.attributes
        end
        if sync_ev.visuals !== nothing
            target_world.entity_visuals[uid] = sync_ev.visuals
        end
        if sync_ev.route_override !== nothing
            target_world.entity_route_overrides[uid] = sync_ev.route_override
        end
    end

    if sync_ev.reserved_pending && sync_ev.target_zone > 0
        pending = SimCore.get_zone_attribute(target_world, sync_ev.target_zone, "_pending_arrivals", nothing)
        if pending === nothing
            pending = Set{UInt64}()
            SimCore.set_zone_attribute!(target_world, sync_ev.target_zone, "_pending_arrivals", pending)
        end
        push!(pending, uid)
    end

    return schedule_key!(target_fel, sync_ev.inner, sync_ev.key, sync_ev.t)
end

"""
    LockFreeMPSCQueue

Multi-producer single-consumer (MPSC) inbox queue for `CrossSubModelSyncEvent` packets.
Any upstream `SubModel` worker can push events via `push_sync_event!`; only the owning
`SubModel` worker drains the buffer during Phase 1 via `drain_sync_events!`.
"""
mutable struct LockFreeMPSCQueue
    lock            :: Threads.SpinLock
    buf             :: Vector{CrossSubModelSyncEvent}
    min_incoming_ps :: Threads.Atomic{Int64}
end

function LockFreeMPSCQueue()
    return LockFreeMPSCQueue(
        Threads.SpinLock(),
        CrossSubModelSyncEvent[],
        Threads.Atomic{Int64}(typemax(Int64)),
    )
end

@inline function Base.isempty(q::LockFreeMPSCQueue)::Bool
    return q.min_incoming_ps[] == typemax(Int64) && Base.isempty(q.buf)
end

@inline function push_sync_event!(q::LockFreeMPSCQueue, sync_ev::CrossSubModelSyncEvent)
    lock(q.lock)
    try
        push!(q.buf, sync_ev)
        if sync_ev.key.t_ps < q.min_incoming_ps[]
            q.min_incoming_ps[] = sync_ev.key.t_ps
        end
    finally
        unlock(q.lock)
    end
    return nothing
end

@inline function drain_sync_events!(
    q::LockFreeMPSCQueue,
    target_world::SimWorld,
    target_fel::FutureEventList,
)::Int
    Base.isempty(q.buf) && return 0
    n_drained = 0
    lock(q.lock)
    try
        n_drained = Base.length(q.buf)
        for sync_ev in q.buf
            on_target_side!(sync_ev, target_world, target_fel)
        end
        Base.empty!(q.buf)
        q.min_incoming_ps[] = typemax(Int64)
    finally
        unlock(q.lock)
    end
    return n_drained
end

