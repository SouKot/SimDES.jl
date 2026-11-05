"""
    engine.jl — Unified AbstractDESEngine, SerialDESEngine, & ConservativePDESEngine

Provides a swappable execution layer for `SimDES`:
- `SerialDESEngine`: Single-heap execution using `FlatBinaryMinHeap` + `EventKey`.
- `ConservativePDESEngine`: Shared-memory multi-threaded Conservative PDES engine
  combining InControl ERS's `SubModel` / `Promise` / two-phase `SyncEvent` semantics
  with `libdes`'s cache-isolated worker partitions and adaptive event-density gate.
"""

abstract type AbstractDESEngine end

"""
    set_wire_delay!(world::SimWorld, from_zone::Int, to_zone::Int, delay_s::Real)

Configure a physical transport delay `delay_s` (in seconds) on the directed wire `from_zone -> to_zone`.
"""
function set_wire_delay!(world::SimWorld, from_zone::Int, to_zone::Int, delay_s::Real)
    delay = max(0.0, Float64(delay_s))
    SimCore.set_zone_attribute!(world, from_zone, "_wire_delay_to_$(to_zone)", delay)
    return delay
end

"""
    get_wire_delay(world::SimWorld, from_zone::Int, to_zone::Int) -> Float64

Retrieve the configured wire delay (in seconds) on `from_zone -> to_zone`, or `0.0` if none.
"""
@inline function get_wire_delay(world::SimWorld, from_zone::Int, to_zone::Int)::Float64
    isempty(world.zone_attributes) && return 0.0
    return max(0.0, Float64(SimCore.get_zone_attribute(world, from_zone, "_wire_delay_to_$(to_zone)", 0.0)))
end

"""
    SerialDESEngine <: AbstractDESEngine

Single-threaded DES engine wrapping `FutureEventList` (`FlatBinaryMinHeap`).
When `deterministic_mode == true`, populates full 4-tuple `EventKey`s and enables
per-zone `Xoshiro` RNGs and partitioned entity IDs so execution is bit-for-bit
identical to `ConservativePDESEngine`.
"""
mutable struct SerialDESEngine <: AbstractDESEngine
    fel                :: FutureEventList
    deterministic_mode :: Bool
    total_events       :: Int64
end

function SerialDESEngine(fel::FutureEventList = FutureEventList(); deterministic_mode::Bool = false)
    if deterministic_mode
        fel.deterministic_mode = true
    end
    return SerialDESEngine(fel, deterministic_mode, Int64(0))
end

"""
    SubModelPartition

Cache-line isolated execution state for one `SubModel` (cluster of tightly-coupled DES zones).
Owns its own shard `local_world::SimWorld`, `fel::FutureEventList`, and incoming `inbox::LockFreeMPSCQueue`
so worker threads never contend on shared `Dict` or `SimStats` memory during parallel windows.
"""
mutable struct SubModelPartition
    submodel_id      :: Int
    zone_ids         :: Vector{Int}
    zone_set         :: Set{Int}
    local_world      :: SimWorld
    fel              :: FutureEventList
    inbox            :: LockFreeMPSCQueue
    rng              :: Xoshiro
    local_next_ps    :: Int64
    safe_horizon_ps  :: Int64
    events_in_window :: Int
    total_executed   :: Int64
    # 64-byte padding fields to prevent false sharing across CPU cores
    _pad1            :: Int64
    _pad2            :: Int64
    _pad3            :: Int64
    _pad4            :: Int64
end

function _build_submodel_world(master_world::SimWorld, zone_ids::Vector{Int})::SimWorld
    sm_world = SimWorld(
        seed = master_world.master_seed,
        use_zone_rngs = true,
        use_partitioned_entity_ids = master_world.use_partitioned_entity_ids,
    )
    sm_world.time = master_world.time
    sm_world._next_entity_id = master_world._next_entity_id
    sm_world.stats.warmup_complete = master_world.stats.warmup_complete
    if haskey(master_world.zone_stats, 0)
        z0 = SimStats()
        z0.warmup_complete = master_world.zone_stats[0].warmup_complete
        sm_world.zone_stats[0] = z0
    end

    zone_set = Set{Int}(zone_ids)
    for zid in zone_ids
        if haskey(master_world.zone_states, zid)
            sm_world.zone_states[zid] = master_world.zone_states[zid]
        end
        if haskey(master_world.zone_stats, zid)
            sm_world.zone_stats[zid] = master_world.zone_stats[zid]
        end
        sm_world.zone_rngs[zid] = get_zone_rng(master_world, zid)
        sm_world.zone_event_seqs[zid] = get(master_world.zone_event_seqs, zid, UInt64(0))
        sm_world.zone_entity_seqs[zid] = get(master_world.zone_entity_seqs, zid, UInt64(0))
        if haskey(master_world.zone_attributes, zid)
            sm_world.zone_attributes[zid] = copy(master_world.zone_attributes[zid])
        end
    end

    for (uid, ag) in master_world.des_agents
        if ag.current_zone in zone_set
            sm_world.des_agents[uid] = ag
            if haskey(master_world.entry_times, uid)
                sm_world.entry_times[uid] = master_world.entry_times[uid]
            end
            if haskey(master_world.entity_attributes, uid)
                sm_world.entity_attributes[uid] = copy(master_world.entity_attributes[uid])
            end
            if haskey(master_world.entity_visuals, uid)
                sm_world.entity_visuals[uid] = copy(master_world.entity_visuals[uid])
            end
            if haskey(master_world.entity_kinematics, uid)
                sm_world.entity_kinematics[uid] = master_world.entity_kinematics[uid]
            end
        end
    end

    return sm_world
end

"""
    ConservativePDESEngine <: AbstractDESEngine

Multi-SubModel Conservative Parallel DES engine with per-dependency promise horizons,
two-phase `CrossSubModelSyncEvent` handoffs, and adaptive EWMA event-density gating.
"""
mutable struct ConservativePDESEngine <: AbstractDESEngine
    submodels           :: Vector{SubModelPartition}
    zone_to_submodel    :: Dict{Int, Int}
    dep_graph           :: SubModelDependencyGraph
    n_workers           :: Int
    min_window_events   :: Int
    use_parallel_mode   :: Bool
    force_parallel      :: Bool
    ewma_events_per_win :: Float64
    total_windows       :: Int64
    total_events        :: Int64
    parallel_windows    :: Int64
    serial_windows      :: Int64
    mode_switches       :: Int64
    active_workers_last :: Int
    local_next_buf      :: Vector{Int64}
    active_sm_buf       :: Vector{Int}
end

"""
    ConservativePDESEngine(
        world::SimWorld,
        configs::Dict{Int, ZoneConfig},
        submodel_zones::Vector{Vector{Int}};
        initial_fel::Union{Nothing, FutureEventList} = nothing,
        dep_graph::Union{Nothing, SubModelDependencyGraph} = nothing,
        n_workers::Int = Threads.nthreads(),
        min_window_events::Int = 32,
        force_parallel::Bool = false,
        seed::Integer = world.master_seed,
    ) -> ConservativePDESEngine

Construct a `ConservativePDESEngine` over `submodel_zones`.
Automatically enables per-zone `Xoshiro` RNGs and partitioned entity IDs on `world`,
routes any events from `initial_fel` into their owning `SubModelPartition`, and
infers `SubModelDependencyGraph` wire promises from `configs` and `world` if `dep_graph === nothing`.
"""
function ConservativePDESEngine(
    world::SimWorld,
    configs::Dict{Int, ZoneConfig},
    submodel_zones::Vector{Vector{Int}} = [sort!(collect(keys(configs)))];
    initial_fel::Union{Nothing, FutureEventList} = nothing,
    dep_graph::Union{Nothing, SubModelDependencyGraph} = nothing,
    n_workers::Int = Threads.nthreads(),
    min_window_events::Int = 32,
    force_parallel::Bool = false,
    seed::Integer = world.master_seed,
)
    K = length(submodel_zones)
    K >= 1 || throw(ArgumentError("submodel_zones must contain at least 1 SubModel"))

    if !world.use_zone_rngs || !world.use_partitioned_entity_ids
        enable_zone_rngs!(world; seed = seed, partitioned_entity_ids = true)
    end

    zone_to_sm = Dict{Int, Int}()
    for (sm_id, zids) in enumerate(submodel_zones)
        for zid in zids
            zone_to_sm[zid] = sm_id
        end
    end

    # Build or validate SubModelDependencyGraph
    graph = if dep_graph !== nothing
        dep_graph
    else
        g = SubModelDependencyGraph(K)
        if K > 1
            for (zid, cfg) in configs
                src_sm = get(zone_to_sm, zid, 0)
                src_sm == 0 && continue
                dests, _ = _routing_dests(cfg.routing)
                for d in copy(dests)
                    d in cfg.downstream || push!(dests, d)
                end
                for d in cfg.downstream
                    d in dests || push!(dests, d)
                end
                for dst_zid in dests
                    dst_sm = get(zone_to_sm, dst_zid, 0)
                    (dst_sm == 0 || dst_sm == src_sm) && continue
                    w_delay = get_wire_delay(world, zid, dst_zid)
                    if w_delay == 0.0 && haskey(configs, dst_zid)
                        w_delay = max(0.0, configs[dst_zid].lookahead)
                    end
                    add_submodel_dependency!(g, src_sm, dst_sm, w_delay)
                end
            end
        end
        g
    end

    submodels = Vector{SubModelPartition}(undef, K)
    for sm_id in 1:K
        zids = submodel_zones[sm_id]
        zset = Set{Int}(zids)
        sm_world = K == 1 ? world : _build_submodel_world(world, zids)
        owned = K == 1 ? nothing : zset
        sm_fel = FutureEventList(deterministic_mode = true, owned_zones = owned)
        sm_rng = Xoshiro(hash(world.master_seed, UInt64(0x9E3779B97F4A7C15) ⊻ UInt64(sm_id)))
        submodels[sm_id] = SubModelPartition(
            sm_id,
            zids,
            zset,
            sm_world,
            sm_fel,
            LockFreeMPSCQueue(),
            sm_rng,
            typemax(Int64),
            Int64(0),
            0,
            Int64(0),
            0, 0, 0, 0,
        )
    end

    if K > 1 && world.time > 0.0
        _add_simstats!(submodels[1].local_world.stats, world.stats)
        if haskey(world.zone_stats, 0) && haskey(submodels[1].local_world.zone_stats, 0)
            _add_simstats!(submodels[1].local_world.zone_stats[0], world.zone_stats[0])
        end
    end

    # Route any pre-scheduled initial events into their owning SubModel
    if initial_fel !== nothing
        if initial_fel.deterministic_mode && world.time > 0.0
            for sm in submodels
                for (zk, zv) in initial_fel.zone_seqs
                    sm.fel.zone_seqs[zk] = zv
                end
            end
        end
        # Sort initial nodes by their timestamp/id so per-zone sequence assignment is deterministic
        init_nodes = copy(initial_fel.queue.nodes)
        sort!(init_nodes, by = n -> n.key)
        for n in init_nodes
            n.event.id in initial_fel.cancelled && continue
            tz = _event_target_zone(n.event.inner)
            sm_id = get(zone_to_sm, tz, 1)
            sm = submodels[sm_id]
            uid = _event_entity_id(n.event.inner)
            if uid > UInt64(0) && K > 1
                if haskey(world.entry_times, uid)
                    sm.local_world.entry_times[uid] = world.entry_times[uid]
                end
                if haskey(world.des_agents, uid)
                    sm.local_world.des_agents[uid] = world.des_agents[uid]
                end
                if haskey(world.entity_attributes, uid)
                    sm.local_world.entity_attributes[uid] = copy(world.entity_attributes[uid])
                end
                if haskey(world.entity_visuals, uid)
                    sm.local_world.entity_visuals[uid] = copy(world.entity_visuals[uid])
                end
            end
            if initial_fel.deterministic_mode && world.time > 0.0
                push_node!(sm.fel.queue, n.key, n.event, n.t)
            else
                sm.fel.active_origin_zone = Int32(tz)
                schedule!(sm.fel, n.event.inner, n.t)
                sm.fel.active_origin_zone = Int32(0)
            end
        end
    end

    use_par = force_parallel || (n_workers > 1 && K > 1)
    init_workers = use_par ? min(max(1, n_workers), K) : 1
    return ConservativePDESEngine(
        submodels,
        zone_to_sm,
        graph,
        max(1, n_workers),
        max(1, min_window_events),
        use_par,
        force_parallel,
        Float64(min_window_events),
        Int64(0),
        Int64(0),
        Int64(0),
        Int64(0),
        Int64(0),
        init_workers,
        Vector{Int64}(undef, K),
        Vector{Int}(undef, K),
    )
end

@inline function _reset_simstats!(s::SimStats, warmup_complete::Bool)
    s.total_events          = 0
    s.total_arrivals        = 0
    s.total_departures      = 0
    s.blocked_count         = 0
    s.wait_time_sum         = 0.0
    s.wait_time_samples     = 0
    s.sojourn_time_sum      = 0.0
    s.sojourn_time_samples  = 0
    s.queue_length_sum      = 0.0
    s.queue_length_samples  = 0
    s.elapsed_sim_time      = 0.0
    s.busy_time             = 0.0
    s.uptime                = 0.0
    s.warmup_complete       = warmup_complete
    return s
end

@inline function _add_simstats!(dst::SimStats, src::SimStats)
    dst.total_events          += src.total_events
    dst.total_arrivals        += src.total_arrivals
    dst.total_departures      += src.total_departures
    dst.blocked_count         += src.blocked_count
    dst.wait_time_sum         += src.wait_time_sum
    dst.wait_time_samples     += src.wait_time_samples
    dst.sojourn_time_sum      += src.sojourn_time_sum
    dst.sojourn_time_samples  += src.sojourn_time_samples
    dst.queue_length_sum      += src.queue_length_sum
    dst.queue_length_samples  += src.queue_length_samples
    dst.elapsed_sim_time      += src.elapsed_sim_time
    dst.busy_time             += src.busy_time
    dst.uptime                += src.uptime
    return dst
end

"""
    sync_submodel_worlds_to_master!(engine::ConservativePDESEngine, master_world::SimWorld)

Idempotently synchronize per-SubModel shard states (`stats`, `zone_stats[0]`, active
`des_agents`, `entry_times`, `entity_attributes`, `entity_kinematics`) back to `master_world`.
"""
function sync_submodel_worlds_to_master!(engine::ConservativePDESEngine, master_world::SimWorld)
    length(engine.submodels) == 1 && return master_world

    _reset_simstats!(master_world.stats, master_world.stats.warmup_complete)
    has_z0 = haskey(master_world.zone_stats, 0)
    if has_z0
        _reset_simstats!(master_world.zone_stats[0], master_world.zone_stats[0].warmup_complete)
    end
    for (zk, zs) in master_world.zone_stats
        if zk < 0
            _reset_simstats!(zs, zs.warmup_complete)
        end
    end

    empty!(master_world.des_agents)
    empty!(master_world.entry_times)

    for sm in engine.submodels
        _add_simstats!(master_world.stats, sm.local_world.stats)
        if has_z0 && haskey(sm.local_world.zone_stats, 0)
            _add_simstats!(master_world.zone_stats[0], sm.local_world.zone_stats[0])
        end
        for (zk, zs) in sm.local_world.zone_stats
            if zk < 0
                dst_zs = get!(master_world.zone_stats, zk) do
                    s = SimStats()
                    s.warmup_complete = zs.warmup_complete
                    s
                end
                _add_simstats!(dst_zs, zs)
            end
        end
        for (uid, ag) in sm.local_world.des_agents
            master_world.des_agents[uid] = ag
        end
        for (uid, et) in sm.local_world.entry_times
            master_world.entry_times[uid] = et
        end
        for (uid, attrs) in sm.local_world.entity_attributes
            master_world.entity_attributes[uid] = attrs
        end
        for (uid, vis) in sm.local_world.entity_visuals
            master_world.entity_visuals[uid] = vis
        end
        for (uid, k) in sm.local_world.entity_kinematics
            master_world.entity_kinematics[uid] = k
        end
        for zid in sm.zone_ids
            if haskey(sm.local_world.zone_entity_seqs, zid)
                master_world.zone_entity_seqs[zid] = sm.local_world.zone_entity_seqs[zid]
            end
        end
    end

    return master_world
end

@inline function _flush_staged_outbox!(engine::ConservativePDESEngine, sm::SubModelPartition)
    isempty(sm.fel.staged_outbox) && return nothing
    for out_node in sm.fel.staged_outbox
        tz = _event_target_zone(out_node.event.inner)
        target_sm = get(engine.zone_to_submodel, tz, 0)
        if target_sm == 0 || target_sm == sm.submodel_id
            push_node!(sm.fel.queue, out_node.key, out_node.event, out_node.t)
            continue
        end

        if engine.dep_graph.strict_check && haskey(engine.dep_graph.edge_lookup, (sm.submodel_id, target_sm))
            edge = engine.dep_graph.edge_lookup[(sm.submodel_id, target_sm)]
            if out_node.key.t_ps < edge.promised_ps[]
                throw(AssertionError(
                    "PDES Promise Violation on SubModel $(sm.submodel_id) -> $target_sm (zone $tz): " *
                    "emitted event at t_ps=$(out_node.key.t_ps) ($(out_node.t)s) < promised_ps=$(edge.promised_ps[])"
                ))
            end
        end

        sync_ev = on_sender_side!(
            sm.local_world,
            sm.submodel_id,
            target_sm,
            out_node.key,
            out_node.event.inner,
            out_node.t,
        )
        push_sync_event!(engine.submodels[target_sm].inbox, sync_ev)
    end
    empty!(sm.fel.staged_outbox)
    return nothing
end

@inline function _execute_submodel_window!(
    engine::ConservativePDESEngine,
    sm::SubModelPartition,
    configs::Dict{Int, ZoneConfig};
    pipeline::Union{Nothing, StatsPipeline} = nothing,
    sync_bufs::Union{Nothing, HybridSyncBuffers} = nothing,
)
    sm.events_in_window = 0
    horizon_ps = sm.safe_horizon_ps
    while (k = peek_key(sm.fel)) !== nothing && k.t_ps <= horizon_ps
        node = safe_dequeue_node!(sm.fel)
        node === nothing && break
        sm.local_world.time = node.t
        sm.fel.active_origin_zone = default_event_zone(node.event.inner)
        dispatch!(
            sm.local_world,
            sm.fel,
            configs,
            sm.rng,
            node.event.inner,
            node.t;
            pipeline = pipeline,
            sync_bufs = sync_bufs,
        )
        sm.fel.active_origin_zone = Int32(0)
        _flush_staged_outbox!(engine, sm)
        sm.events_in_window += 1
    end
    sm.total_executed += sm.events_in_window
    return nothing
end

"""
    step_des_engine_until!(
        engine::AbstractDESEngine,
        world::SimWorld,
        configs::Dict{Int, ZoneConfig},
        t_target::Float64;
        rng::AbstractRNG = default_rng(),
        pipeline::Union{Nothing, StatsPipeline} = nothing,
        sync_bufs::Union{Nothing, HybridSyncBuffers} = nothing,
    ) -> Int64

Advance `engine` (`SerialDESEngine` or `ConservativePDESEngine`) up to `t_target`
(quantized to integer picoseconds `to_picoseconds(t_target)`), returning the number
of events executed during this step.
"""
function step_des_engine_until!(
    engine::SerialDESEngine,
    world::SimWorld,
    configs::Dict{Int, ZoneConfig},
    t_target::Float64;
    rng::AbstractRNG = default_rng(),
    pipeline::Union{Nothing, StatsPipeline} = nothing,
    sync_bufs::Union{Nothing, HybridSyncBuffers} = nothing,
)::Int64
    ceiling_ps = to_picoseconds(t_target)
    n_exec = Int64(0)
    fel = engine.fel
    while (k = peek_key(fel)) !== nothing && k.t_ps <= ceiling_ps
        node = safe_dequeue_node!(fel)
        node === nothing && break
        world.time = node.t
        if fel.deterministic_mode
            fel.active_origin_zone = default_event_zone(node.event.inner)
        end
        dispatch!(world, fel, configs, rng, node.event.inner, node.t; pipeline = pipeline, sync_bufs = sync_bufs)
        if fel.deterministic_mode
            fel.active_origin_zone = Int32(0)
        end
        n_exec += 1
    end
    world.time = t_target
    engine.total_events += n_exec
    return n_exec
end

function step_des_engine_until!(
    engine::ConservativePDESEngine,
    world::SimWorld,
    configs::Dict{Int, ZoneConfig},
    t_target::Float64;
    rng::AbstractRNG = default_rng(),
    pipeline::Union{Nothing, StatsPipeline} = nothing,
    sync_bufs::Union{Nothing, HybridSyncBuffers} = nothing,
)::Int64
    ceiling_ps = to_picoseconds(t_target)
    K = length(engine.submodels)

    # Fast path for single SubModel (K == 1): zero barrier overhead, identical to SerialDESEngine
    if K == 1
        sm = engine.submodels[1]
        sm.safe_horizon_ps = ceiling_ps
        _execute_submodel_window!(engine, sm, configs; pipeline = pipeline, sync_bufs = sync_bufs)
        world.time = t_target
        engine.total_windows += 1
        engine.serial_windows += 1
        engine.active_workers_last = 1
        engine.total_events += sm.events_in_window
        return Int64(sm.events_in_window)
    end

    step_events = Int64(0)
    local_next_vec = engine.local_next_buf
    active_sm_vec = engine.active_sm_buf
    while true
        # Phase 1a: Drain cross-SubModel inboxes & update local_next_ps
        global_min_ps = typemax(Int64)
        for sm in engine.submodels
            drain_sync_events!(sm.inbox, sm.local_world, sm.fel)
            k = peek_key(sm.fel)
            sm.local_next_ps = k === nothing ? typemax(Int64) : k.t_ps
            if sm.local_next_ps < global_min_ps
                global_min_ps = sm.local_next_ps
            end
        end

        # If no SubModel has any event at or before ceiling_ps, we are done!
        global_min_ps > ceiling_ps && break

        # Phase 1b: Transitive promise propagation across SubModelDependencyGraph
        for idx in 1:K
            @inbounds local_next_vec[idx] = engine.submodels[idx].local_next_ps
        end
        propagate_promises!(engine.dep_graph, local_next_vec)

        # Phase 1c: Compute each SubModel's conservative safe execution horizon T_safe(B)
        # and collect active SubModels (with local_next_ps <= safe_horizon_ps) without allocation
        n_active_sm = 0
        for idx in 1:K
            local sm = @inbounds engine.submodels[idx]
            h_ps = safe_horizon_ps(engine.dep_graph, sm.submodel_id, ceiling_ps)
            sm.safe_horizon_ps = h_ps
            if sm.local_next_ps <= h_ps
                n_active_sm += 1
                @inbounds active_sm_vec[n_active_sm] = idx
            else
                sm.events_in_window = 0
            end
        end

        # Phase 2: Execute safe windows across active SubModels (parallel or adaptive serial fallback)
        # Note: Only wake multiple worker threads when >= 2 SubModels actually have ready events!
        if (engine.force_parallel || engine.use_parallel_mode) && engine.n_workers > 1 && n_active_sm >= 2
            engine.parallel_windows += 1
            eff_workers = min(engine.n_workers, n_active_sm, Threads.nthreads())
            engine.active_workers_last = eff_workers
            Threads.@threads :static for a_idx in 1:n_active_sm
                local sm_idx = @inbounds active_sm_vec[a_idx]
                local part = @inbounds engine.submodels[sm_idx]
                _execute_submodel_window!(engine, part, configs)
            end
        else
            engine.serial_windows += 1
            engine.active_workers_last = 1
            for a_idx in 1:n_active_sm
                local sm_idx = @inbounds active_sm_vec[a_idx]
                local part = @inbounds engine.submodels[sm_idx]
                _execute_submodel_window!(engine, part, configs)
            end
        end

        win_events = 0
        for sm in engine.submodels
            win_events += sm.events_in_window
        end
        step_events += win_events
        engine.total_windows += 1
        engine.total_events += win_events

        # Update adaptive EWMA density gate (O(1) register arithmetic, 0 bytes allocated)
        if !engine.force_parallel
            prev_mode = engine.use_parallel_mode
            engine.ewma_events_per_win = 0.85 * engine.ewma_events_per_win + 0.15 * Float64(win_events)
            new_mode = (engine.ewma_events_per_win >= Float64(engine.min_window_events)) && (engine.n_workers > 1)
            if new_mode != prev_mode
                engine.mode_switches += 1
            end
            engine.use_parallel_mode = new_mode
        end

        # Safety guard against zero-promise cycles (if caller bypassed Union-Find contraction)
        if win_events == 0
            # Advance the single globally earliest event serially to guarantee forward progress
            best_sm = 1
            for idx in 2:K
                if engine.submodels[idx].local_next_ps < engine.submodels[best_sm].local_next_ps
                    best_sm = idx
                end
            end
            sm = engine.submodels[best_sm]
            sm.safe_horizon_ps = sm.local_next_ps
            _execute_submodel_window!(engine, sm, configs)
            step_events += sm.events_in_window
            engine.total_events += sm.events_in_window
        end
    end

    for sm in engine.submodels
        sm.local_world.time = t_target
    end
    world.time = t_target
    sync_submodel_worlds_to_master!(engine, world)
    return step_events
end

"""
    consolidate_to_serial_fel!(
        engine::ConservativePDESEngine,
        master_world::SimWorld,
        target_fel::FutureEventList,
    ) -> FutureEventList

Losslessly consolidate all pending events and per-zone sequence counters from a running
`ConservativePDESEngine` back into `target_fel` and `master_world`, allowing mid-run
switching from `:pdes` to `:serial` at any simulated timestamp `t > 0`.
"""
function consolidate_to_serial_fel!(
    engine::ConservativePDESEngine,
    master_world::SimWorld,
    target_fel::FutureEventList,
)::FutureEventList
    for sm in engine.submodels
        drain_sync_events!(sm.inbox, sm.local_world, sm.fel)
    end
    sync_submodel_worlds_to_master!(engine, master_world)

    empty!(target_fel.queue)
    empty!(target_fel.cancelled)
    empty!(target_fel.staged_outbox)
    target_fel.deterministic_mode = true
    target_fel.owned_zones = nothing

    for sm in engine.submodels
        for (zid, zseq) in sm.fel.zone_seqs
            target_fel.zone_seqs[zid] = max(get(target_fel.zone_seqs, zid, UInt64(0)), zseq)
        end
        for n in sm.fel.queue.nodes
            n.event.id in sm.fel.cancelled && continue
            push_node!(target_fel.queue, n.key, n.event, n.t)
        end
    end
    return target_fel
end
