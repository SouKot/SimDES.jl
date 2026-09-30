"""
    dispatch.jl — DES Event Dispatch (Multiple Dispatch routing)

Each `dispatch!` method handles one `SimEvent` subtype.
Julia's multiple dispatch routes each event to the correct handler
with zero overhead — no if/elseif chains, no vtable lookups.

Performance: O(log n) FEL dequeue + O(1) FIFO queue ops per event.
Correctness: Exact Wq tracked via service_start_time in DESAgent.

Phase 2C additions:
  - Priority queuing: non-preemptive HOL (Head-Of-Line) via sorted Vector
  - Routing: ExitSystem / FixedRoute / ProbRoute after ProcessComplete
  - NHPP: thinning algorithm for time-varying arrival rates
  - Machine failures: configurable α/β with availability tracking
  - Fork-Join: parallel sub-task spawning and barrier synchronisation

Design ref: §7.11 (Event Handler Pattern), §7.12 (Cancel Support)
DEVS equivalents: δ_ext (EntityArrival) and δ_int (ProcessComplete)
"""

# ─────────────────────────────────────────────────────────────────────────────
# Core dispatch signature
# ─────────────────────────────────────────────────────────────────────────────

"""
    dispatch!(world, fel, configs, rng, event, t)

Route a simulation event to its handler. The correct method is selected
by Julia's multiple dispatch on the type of `event`.

# Arguments
- `world::SimWorld`: mutable simulation state
- `fel::FutureEventList`: future event list (handlers schedule new events here)
- `configs::Dict{Int, ZoneConfig}`: zone configurations (read-only)
- `rng::AbstractRNG`: random number generator (seeded per run)
- `event::SimEvent`: the event being dispatched
- `t::Float64`: current simulated time

# Adding custom events
```julia
struct MyEvent <: SimEvent; zone_id::Int end

function SimDES.dispatch!(world, fel, configs, rng, e::MyEvent, t)
    # your logic here
end
```
"""
function dispatch! end

# ─────────────────────────────────────────────────────────────────────────────
# Task 20 bridge helpers — optional StatsPipeline recording (non-breaking)
# ─────────────────────────────────────────────────────────────────────────────

@inline _record_arrival_optional!(::Nothing) = nothing
@inline _record_arrival_optional!(p::StatsPipeline) = (record_arrival!(p); nothing)

@inline _record_departure_optional!(::Nothing, ::Float64, ::Float64) = nothing
@inline _record_departure_optional!(p::StatsPipeline, wait_time::Float64, sojourn_time::Float64) =
    (record_departure!(p, wait_time, sojourn_time); nothing)

@inline _record_blocked_optional!(::Nothing) = nothing
@inline _record_blocked_optional!(p::StatsPipeline) = (record_blocked!(p); nothing)

@inline _record_queue_optional!(::Nothing, ::Int, ::Int, ::Float64) = nothing
@inline _record_queue_optional!(p::StatsPipeline, n_sys::Int, n_q::Int, dt::Float64) =
    (record_queue_length!(p, n_sys, n_q, dt); nothing)

@inline _record_util_optional!(::Nothing, ::Float64) = nothing
@inline _record_util_optional!(p::StatsPipeline, busy_dt::Float64) =
    (record_utilization!(p, busy_dt); nothing)

@inline _record_idle_optional!(::Nothing, ::Float64) = nothing
@inline _record_idle_optional!(p::StatsPipeline, idle_dt::Float64) =
    (record_idle!(p, idle_dt); nothing)

@inline _record_uptime_optional!(::Nothing, ::Float64) = nothing
@inline _record_uptime_optional!(p::StatsPipeline, dt::Float64) =
    (record_uptime!(p, dt); nothing)

@inline _mark_departure_optional!(::Nothing, ::UInt64) = nothing
@inline _mark_departure_optional!(sync_bufs::HybridSyncBuffers, entity_id::UInt64) =
    (mark_departure!(sync_bufs, Int(entity_id)); nothing)

# ─────────────────────────────────────────────────────────────────────────────
# NullEvent — Chandy-Misra null message; no-op in Tier 1
# ─────────────────────────────────────────────────────────────────────────────

dispatch!(world, fel, configs, rng, ::NullEvent, t;
          pipeline::Union{Nothing,StatsPipeline}=nothing,
          sync_bufs::Union{Nothing,HybridSyncBuffers}=nothing) = nothing

# ─────────────────────────────────────────────────────────────────────────────
# EntityArrival — entity enters zone (DEVS δ_ext)
# ─────────────────────────────────────────────────────────────────────────────

"""
    dispatch!(world, fel, configs, rng, e::EntityArrival, t)

Handle an entity arriving at a zone.

Logic:
1. Update time-average stats for the interval [last_event_time, t]
2. Record system-entry time (preserved across zone transfers for total sojourn)
3. Check fork-join config: if fork zone, spawn sub-entities instead of queuing
4. If system is at capacity (M/M/1/K blocking) → reject entity
5. If server is free → start service immediately; set service_start_time = t (Wq = 0)
6. If all servers busy → join queue:
   - :fifo → push to tail (O(1))
   - :priority → insert at sorted position (non-preemptive HOL, O(n))
7. Schedule next arrival:
   - Homogeneous Poisson if arrival_rate > 0
   - NHPP thinning if arrival_schedule is set
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::EntityArrival, t::Float64;
                   pipeline::Union{Nothing,StatsPipeline}=nothing,
                   sync_bufs::Union{Nothing,HybridSyncBuffers}=nothing)
    zone = get_zone(world, e.zone_id)
    cfg  = configs[e.zone_id]

    # ── Time-average stats for interval since last event
    _update_time_averages!(world, zone, e.zone_id, t; pipeline=pipeline)

    # ── Record system-entry time (first time we see this entity)
    is_first_entry = !haskey(world.entry_times, e.entity_id)
    if is_first_entry
        world.entry_times[e.entity_id] = t
    end

    # ── Fork-join: if this is a FORK zone, spawn sub-entities and return
    if cfg.fork_join !== nothing
        _handle_fork!(world, fel, configs, rng, e, cfg.fork_join, t)
        # Only self-schedule next external arrival if THIS arrival was external
        e.is_external && _schedule_next_arrival!(world, fel, configs, rng, e.zone_id, cfg, t)
        return
    end

    # ── Check finite buffer (M/M/1/K blocking)
    entities_in_system = zone.queue_length + zone.busy_servers
    if entities_in_system >= cfg.capacity
        # Entity rejected — buffer full
        record_blocked!(world.stats)
        _record_blocked_optional!(pipeline)
        is_first_entry && delete!(world.entry_times, e.entity_id)   # entity never entered system
    else
        # Accepted arrival (not blocked)
        record_arrival!(world.stats)
        _record_arrival_optional!(pipeline)
        _record_zone_arrival!(world, e.zone_id)
        if is_first_entry && !isempty(world.zone_stats)
            sys_zs = get(world.zone_stats, 0, nothing)
            sys_zs !== nothing && record_arrival!(sys_zs)
        end

        pmode = !isempty(world.zone_attributes) ?
                SimCore.get_zone_attribute(world, e.zone_id, "_process_mode", cfg.process_mode) :
                cfg.process_mode

        if pmode === :custom
            # Custom DEVS process mode: place entity in queue/station without auto-scheduling ProcessComplete
            agent = DESAgent(t, e.zone_id, e.priority, Inf)
            add_des_agent!(world, e.entity_id, agent)
            zone.queue_length += 1
            _enqueue_entity!(world, zone, cfg, e.entity_id, e.priority, t)
            if cfg.routing isa FixedRoute && !isempty(world.port_directory.wires)
                dzid = cfg.routing.to
                if haskey(world.zone_states, dzid) && haskey(configs, dzid)
                    _try_pull_from_upstream_queues!(world, fel, configs, rng, dzid, world.zone_states[dzid], configs[dzid], t)
                end
            end
        elseif zone.busy_servers < zone.num_servers
            # ── Server free → begin service immediately (Wq = 0)
            zone.busy_servers += 1
            agent = DESAgent(t, e.zone_id, e.priority, t)   # service_start_time = t → Wq = 0
            add_des_agent!(world, e.entity_id, agent)
            cmode = !isempty(world.zone_attributes) ?
                    SimCore.get_zone_attribute(world, e.zone_id, "_conveyor_mode", cfg.conveyor_mode) :
                    cfg.conveyor_mode

            if cmode === :indexing
                plen = Float64(SimCore.get_zone_attribute(world, e.zone_id, "_path_length", cfg.path_length))
                spd  = Float64(SimCore.get_zone_attribute(world, e.zone_id, "_nominal_speed", cfg.nominal_speed))
                k = SimCore.EntityKinematics(e.zone_id, plen, 0.0, t; exit_event_id=UInt64(0))
                k.nominal_speed = spd
                world.entity_kinematics[e.entity_id] = k
                if !Bool(SimCore.get_zone_attribute(world, e.zone_id, "_index_pulse_active", false))
                    SimCore.set_zone_attribute!(world, e.zone_id, "_index_pulse_active", true)
                    iv = max(1e-4, Float64(SimCore.get_zone_attribute(world, e.zone_id, "_conveyor_index_interval", cfg.conveyor_index_interval)))
                    schedule!(fel, SimCore.CustomUserEvent(e.zone_id, "", :_index_pulse, t + iv; interval=iv), t + iv)
                end
            else
                service_time = _compute_service_duration!(world, rng, e.zone_id, e.entity_id, cfg)
                cev_id = schedule!(fel, ProcessComplete(e.entity_id, e.zone_id, t + service_time),
                                   t + service_time)
                if cmode === :accumulating || !isempty(world.entity_kinematics) || !isempty(world.zone_attributes)
                    plen = Float64(SimCore.get_zone_attribute(world, e.zone_id, "_path_length", cfg.path_length))
                    spd  = Float64(SimCore.get_zone_attribute(world, e.zone_id, "_nominal_speed", cfg.nominal_speed))
                    world.entity_kinematics[e.entity_id] =
                        SimCore.EntityKinematics(e.zone_id, plen, spd, t; exit_event_id=cev_id)
                    if cmode === :accumulating
                        _recompute_accumulating_conveyor!(world, fel, configs, e.zone_id, cfg, t)
                    end
                end
            end
        else
            # ── All servers busy (or down) → join queue
            agent = DESAgent(t, e.zone_id, e.priority, Inf)
            add_des_agent!(world, e.entity_id, agent)
            zone.queue_length += 1
            _enqueue_entity!(world, zone, cfg, e.entity_id, e.priority, t)
        end
    end

    # ── Schedule next arrival from this zone's Poisson/NHPP process.
    e.is_external && _schedule_next_arrival!(world, fel, configs, rng, e.zone_id, cfg, t)
end

# ─────────────────────────────────────────────────────────────────────────────
# ProcessComplete — service finishes (DEVS δ_int)
# ─────────────────────────────────────────────────────────────────────────────

"""
    dispatch!(world, fel, configs, rng, e::ProcessComplete, t)

Handle a service completion at a station.
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::ProcessComplete, t::Float64;
                   pipeline::Union{Nothing,StatsPipeline}=nothing,
                   sync_bufs::Union{Nothing,HybridSyncBuffers}=nothing)
    zone = get_zone(world, e.station_id)
    cfg  = configs[e.station_id]

    # ── Time-average stats for interval since last event
    _update_time_averages!(world, zone, e.station_id, t; pipeline=pipeline)

    # ── Check accumulating conveyor blocking before departure
    cmode = !isempty(world.zone_attributes) ?
            SimCore.get_zone_attribute(world, e.station_id, "_conveyor_mode", cfg.conveyor_mode) :
            cfg.conveyor_mode
    if cmode === :accumulating && _is_downstream_blocked(world, configs, e.station_id, cfg)
        k = get(world.entity_kinematics, e.entity_id, nothing)
        if k !== nothing
            k.base_distance = k.path_length
            k.current_speed = 0.0
            k.last_update_time = t
            k.exit_event_id = UInt64(0)
        end
        _recompute_accumulating_conveyor!(world, fel, configs, e.station_id, cfg, t)
        return
    end

    # ── Record departure stats and route/remove entity
    agent = get_des_agent(world, e.entity_id)
    if agent !== nothing
        # Save last_processed_attrs for sequence-dependent setup time queries
        if !isempty(world.entity_attributes) && haskey(world.entity_attributes, e.entity_id)
            world.last_processed_attrs[e.station_id] = copy(world.entity_attributes[e.entity_id])
        end
        !isempty(world.entity_kinematics) && delete!(world.entity_kinematics, e.entity_id)

        wait_time = (agent.service_start_time == Inf) ? 0.0 :
                    agent.service_start_time - agent.arrival_time
        zone_sojourn = t - agent.arrival_time

        # ── Check fork-join: is this entity a sub-task completing?
        if haskey(world.sub_entity_map, e.entity_id)
            _record_zone_departure!(world, e.station_id, wait_time, zone_sojourn)
            _handle_join!(world, fel, configs, rng, e.entity_id, t; pipeline=pipeline)
            remove_des_agent!(world, e.entity_id)
        else
            record_departure!(world.stats, wait_time, zone_sojourn)
            _record_departure_optional!(pipeline, wait_time, zone_sojourn)
            _record_zone_departure!(world, e.station_id, wait_time, zone_sojourn)
            route_outcome = _route_entity!(world, fel, configs, rng, e.entity_id, agent, cfg, t, wait_time)
            if haskey(world.zone_stats, 0)
                prio_key = -100 - agent.priority
                pstats = get!(world.zone_stats, prio_key) do
                    s = SimStats()
                    s.warmup_complete = world.zone_stats[0].warmup_complete
                    s
                end
                pstats.warmup_complete = world.zone_stats[0].warmup_complete
                if route_outcome === :exit
                    record_departure!(pstats, wait_time, zone_sojourn)
                elseif pstats.warmup_complete
                    pstats.wait_time_sum += wait_time
                    pstats.sojourn_time_sum += zone_sojourn
                end
            end
            route_outcome === :exit && _mark_departure_optional!(sync_bufs, e.entity_id)
        end
    end

    # ── Serve next in queue (if any)
    if zone.queue_length > 0
        zone.queue_length -= 1
        next_id = _dequeue_next_entity!(world, zone, cfg)
        next_agent = get_des_agent(world, next_id)
        if next_agent !== nothing
            world.des_agents[next_id] = DESAgent(next_agent.arrival_time,
                                                  next_agent.current_zone,
                                                  next_agent.priority, t)
            service_time = _compute_service_duration!(world, rng, e.station_id, next_id, cfg)
            cev_id = schedule!(fel, ProcessComplete(next_id, e.station_id, t + service_time),
                               t + service_time)
            if cmode === :accumulating || !isempty(world.entity_kinematics)
                plen = Float64(SimCore.get_zone_attribute(world, e.station_id, "_path_length", cfg.path_length))
                spd  = Float64(SimCore.get_zone_attribute(world, e.station_id, "_nominal_speed", cfg.nominal_speed))
                world.entity_kinematics[next_id] =
                    SimCore.EntityKinematics(e.station_id, plen, spd, t; exit_event_id=cev_id)
            end
        end
    else
        zone.busy_servers = max(0, zone.busy_servers - 1)
        if !isempty(world.port_directory.wires)
            _try_pull_from_upstream_queues!(world, fel, configs, rng, e.station_id, zone, cfg, t)
        end
    end

    if cmode === :accumulating
        _recompute_accumulating_conveyor!(world, fel, configs, e.station_id, cfg, t)
    end
    if !isempty(world.entity_kinematics)
        _unblock_upstream_accumulating_conveyors!(world, fel, configs, e.station_id, t)
    end
end

# ─────────────────────────────────────────────────────────────────────────────
# ResourceFailure — machine/server goes down (DES-M-04)
# ─────────────────────────────────────────────────────────────────────────────

"""
    dispatch!(world, fel, configs, rng, e::ResourceFailure, t)

Handle a resource (machine/server) failure.

Uses `cfg.failures::BernoulliFailure` for repair time (β) and reschedule (α).
If `cfg.failures` is `NoFailure`, this handler is a no-op (event should not have
been scheduled, but is handled defensively).
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::ResourceFailure, t::Float64;
                   pipeline::Union{Nothing,StatsPipeline}=nothing,
                   sync_bufs::Union{Nothing,HybridSyncBuffers}=nothing)
    zone = get_zone(world, e.resource_id)
    cfg  = configs[e.resource_id]

    _update_time_averages!(world, zone, e.resource_id, t; pipeline=pipeline)

    # Reduce effective server count (minimum 0)
    zone.busy_servers = max(0, zone.busy_servers - 1)
    zone.num_servers  = max(0, zone.num_servers - 1)

    # Schedule repair using BernoulliFailure repair rate β
    if cfg.failures isa BernoulliFailure
        repair_time = rand(rng, Exponential(1.0 / cfg.failures.β))
        schedule!(fel, ScheduledChange{:Repair}(e.resource_id, t + repair_time),
                  t + repair_time)
    end
end

"""
    dispatch!(world, fel, configs, rng, e::ScheduledChange{:Repair}, t)

Restore one server after a machine repair; serve a waiting entity if any.
Reschedules the next machine failure using `cfg.failures::BernoulliFailure` rate α.
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::ScheduledChange{:Repair}, t::Float64;
                   pipeline::Union{Nothing,StatsPipeline}=nothing,
                   sync_bufs::Union{Nothing,HybridSyncBuffers}=nothing)
    zone = get_zone(world, e.zone_id)
    cfg  = configs[e.zone_id]

    _update_time_averages!(world, zone, e.zone_id, t; pipeline=pipeline)

    zone.num_servers += 1   # restore one server

    # If a waiting entity exists and a server slot freed, start serving
    if zone.queue_length > 0 && zone.busy_servers < zone.num_servers
        zone.queue_length -= 1
        zone.busy_servers += 1
        next_id = _dequeue_next_entity!(world, zone, cfg)
        next_agent = get_des_agent(world, next_id)
        if next_agent !== nothing
            world.des_agents[next_id] = DESAgent(next_agent.arrival_time,
                                                  next_agent.current_zone,
                                                  next_agent.priority, t)
            service_time = cfg.service_dist(rng)
            schedule!(fel, ProcessComplete(next_id, e.zone_id, t + service_time),
                      t + service_time)
        end
    end

    # Reschedule next failure using BernoulliFailure failure rate α
    if cfg.failures isa BernoulliFailure
        ttf = rand(rng, Exponential(1.0 / cfg.failures.α))
        schedule!(fel, ResourceFailure(e.zone_id, 1.0f0, t + ttf), t + ttf)
    end
end

# ─────────────────────────────────────────────────────────────────────────────
# TransferOut — entity moves between zones (legacy; routing now via RoutingPolicy)
# ─────────────────────────────────────────────────────────────────────────────

"""
    dispatch!(world, fel, configs, rng, e::TransferOut, t)

Entity leaves one zone and arrives at a downstream zone after the transit delay.
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::TransferOut, t::Float64;
                   pipeline::Union{Nothing,StatsPipeline}=nothing,
                   sync_bufs::Union{Nothing,HybridSyncBuffers}=nothing)
    dest_cfg = get(configs, e.dest_zone, nothing)
    dest_cfg === nothing && return   # unknown destination — drop silently

    transit = max(dest_cfg.lookahead, 0.0)
    schedule!(fel, EntityArrival(e.entity_id, e.dest_zone, t + transit), t + transit)
end

# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

"""
    _update_time_averages!(world, zone, zone_id, t)

Record time-weighted statistics for the period [zone.last_event_time, t].
Updates both global `world.stats` and per-zone `world.zone_stats[zone_id]`.
Called at the start of every event handler before mutating zone state.
"""
function _update_time_averages!(world::SimWorld, zone::ZoneState, zone_id::Int, t::Float64;
                                pipeline::Union{Nothing,StatsPipeline}=nothing)
    dt = t - zone.last_event_time
    if dt > 0.0
        n_in_system = zone.queue_length + zone.busy_servers
        record_queue_length!(world.stats, n_in_system, dt)
        _record_queue_optional!(pipeline, n_in_system, zone.queue_length, dt)
        if zone.busy_servers > 0
            # Per-server utilisation: fraction of server capacity in use
            frac_busy = zone.num_servers > 0 ?
                        zone.busy_servers / zone.num_servers : 0.0
            record_utilization!(world.stats, frac_busy * dt)
            _record_util_optional!(pipeline, frac_busy * dt)
            _record_idle_optional!(pipeline, dt - frac_busy * dt)
        else
            _record_idle_optional!(pipeline, dt)
        end

        # Availability: only accrue when at least one server is operational.
        if zone.num_servers > 0
            _record_uptime_optional!(pipeline, dt)
        end

        # Per-zone stats (for multi-zone network validation).
        # Guard: skip the Dict lookup entirely for single-zone simulations where
        # world.zone_stats is empty (avoids a hash-miss on every event).
        if !isempty(world.zone_stats)
            zs = get(world.zone_stats, zone_id, nothing)
            if zs !== nothing
                zs.warmup_complete = world.stats.warmup_complete
                record_queue_length!(zs, n_in_system, dt)
                # Track machine UPTIME (for availability = uptime/elapsed_sim_time)
                # Also track server busy fraction for utilization metric
                if zone.num_servers > 0
                    record_uptime!(zs, dt)   # machine is up during this interval
                    if zone.busy_servers > 0
                        frac_busy = zone.busy_servers / zone.num_servers
                        record_utilization!(zs, frac_busy * dt)
                    end
                end
                # Note: when num_servers == 0 (machine down), neither uptime nor
                # busy_time accrues — this is the correct semantics.
            end
        end
    end
    zone.last_event_time = t
    return nothing
end

"""
    _record_system_exit!(world, entity_id, fallback_entry_t, t)

Record true end-to-end system sojourn W = t_exit - t_entry into `world.zone_stats[0]` if present.
"""
@inline function _record_system_exit!(world::SimWorld, entity_id::UInt64, fallback_entry_t::Float64, t::Float64, final_wait::Float64=0.0)
    entry_t = get(world.entry_times, entity_id, fallback_entry_t)
    total_sojourn = max(0.0, t - entry_t)
    if haskey(world.zone_stats, 0)
        record_departure!(world.zone_stats[0], final_wait, total_sojourn)
    end
    delete!(world.entry_times, entity_id)
    remove_des_agent!(world, entity_id)   # hot path: skip 3 wasted Dict ops
end

@inline function _record_routed_wait!(world::SimWorld, wait_time::Float64)
    if haskey(world.zone_stats, 0) && world.zone_stats[0].warmup_complete
        world.zone_stats[0].wait_time_sum += wait_time
    end
end

"""
    _route_entity!(world, fel, configs, rng, entity_id, agent, cfg, t, wait_time=0.0)

Route or remove an entity based on the zone's `RoutingPolicy`.
- `ExitSystem`: record total sojourn W, remove entity from world
- `FixedRoute(to)`: schedule `EntityArrival` at the next zone; update current_zone
- `ProbRoute(choices)`: sample destination, then route or exit
- `ShortestQueueRoute(candidates)`: pick candidate zone with minimum `(queue_length + busy_servers)`
- `RoundRobinRoute(candidates)`: cycle across candidate zones
- `DynamicPolicyRoute(candidates, fn)`: invoke `fn(world, entity_id, agent, candidates)`
"""
function _route_entity!(world::SimWorld, fel::FutureEventList,
                        configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                        entity_id::UInt64, agent::DESAgent,
                        cfg::ZoneConfig, t::Float64, wait_time::Float64=0.0)
    # I-2: Check for hook-level one-shot route_to! override first
    if !isempty(world.entity_route_overrides) && haskey(world.entity_route_overrides, entity_id)
        dest_override = pop!(world.entity_route_overrides, entity_id)
        if dest_override <= 0 || !haskey(configs, dest_override)
            _record_system_exit!(world, entity_id, agent.arrival_time, t, wait_time)
            return :exit
        else
            _record_routed_wait!(world, wait_time)
            world.des_agents[entity_id] = DESAgent(t, dest_override, agent.priority, Inf)
            schedule!(fel, EntityArrival(entity_id, dest_override, t, agent.priority, false), t)
            return :routed
        end
    end

    if cfg.routing isa ExitSystem
        _record_system_exit!(world, entity_id, agent.arrival_time, t, wait_time)
        return :exit

    elseif cfg.routing isa FixedRoute
        dest = cfg.routing.to
        _record_routed_wait!(world, wait_time)
        # Keep entity in world; update current_zone for DESAgent
        world.des_agents[entity_id] = DESAgent(t, dest, agent.priority, Inf)
        # Routed arrival: is_external=false — does NOT trigger next external arrival at dest
        schedule!(fel, EntityArrival(entity_id, dest, t, agent.priority, false), t)
        return :routed

    elseif cfg.routing isa ProbRoute
        dest = sample_destination(cfg.routing, rng)
        if dest === nothing
            _record_system_exit!(world, entity_id, agent.arrival_time, t, wait_time)
            return :exit
        else
            _record_routed_wait!(world, wait_time)
            world.des_agents[entity_id] = DESAgent(t, dest, agent.priority, Inf)
            # Routed arrival: is_external=false — does NOT trigger next external arrival at dest
            schedule!(fel, EntityArrival(entity_id, dest, t, agent.priority, false), t)
            return :routed
        end

    elseif cfg.routing isa ShortestQueueRoute
        cands = cfg.routing.candidates
        if isempty(cands)
            _record_system_exit!(world, entity_id, agent.arrival_time, t, wait_time)
            return :exit
        end
        best_dest = cands[1]
        best_load = typemax(Int)
        for cid in cands
            if haskey(world.zone_states, cid)
                z = world.zone_states[cid]
                # Load = waiting in queue + fractional utilization or busy count
                load = z.queue_length * 1000 + z.busy_servers
                if load < best_load
                    best_load = load
                    best_dest = cid
                end
            end
        end
        _record_routed_wait!(world, wait_time)
        world.des_agents[entity_id] = DESAgent(t, best_dest, agent.priority, Inf)
        schedule!(fel, EntityArrival(entity_id, best_dest, t, agent.priority, false), t)
        return :routed

    elseif cfg.routing isa RoundRobinRoute
        cands = cfg.routing.candidates
        if isempty(cands)
            _record_system_exit!(world, entity_id, agent.arrival_time, t, wait_time)
            return :exit
        end
        idx = mod1(cfg.routing.cursor + 1, length(cands))
        cfg.routing.cursor = idx
        dest = cands[idx]
        _record_routed_wait!(world, wait_time)
        world.des_agents[entity_id] = DESAgent(t, dest, agent.priority, Inf)
        schedule!(fel, EntityArrival(entity_id, dest, t, agent.priority, false), t)
        return :routed

    elseif cfg.routing isa DynamicPolicyRoute
        cands = cfg.routing.candidates
        if isempty(cands)
            _record_system_exit!(world, entity_id, agent.arrival_time, t, wait_time)
            return :exit
        end
        dest = cfg.routing.policy_fn(world, entity_id, agent, cands)
        if dest === nothing || dest <= 0
            _record_system_exit!(world, entity_id, agent.arrival_time, t, wait_time)
            return :exit
        end
        _record_routed_wait!(world, wait_time)
        world.des_agents[entity_id] = DESAgent(t, dest, agent.priority, Inf)
        schedule!(fel, EntityArrival(entity_id, dest, t, agent.priority, false), t)
        return :routed
    end
    return :unknown
end

"""
    _schedule_next_arrival!(world, fel, configs, rng, zone_id, cfg, t)

Schedule the next entity arrival for a zone based on `cfg.arrival::ArrivalProcess`:
- `NoArrival`: no-op
- `PoissonArrival(λ)`: homogeneous Poisson with rate λ
- `NHPPArrival(sched)`: NHPP via thinning (Lewis & Shedler 1979)
"""
function _schedule_next_arrival!(world::SimWorld, fel::FutureEventList,
                                  configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                                  zone_id::Int, cfg::ZoneConfig, t::Float64)
    _schedule_next_arrival!(world, fel, rng, zone_id, cfg.arrival, t)
end

# Dispatch on ArrivalProcess subtype — zero-overhead, closed extension point
_schedule_next_arrival!(::SimWorld, ::FutureEventList, ::AbstractRNG,
                        ::Int, ::NoArrival, ::Float64) = nothing

function _schedule_next_arrival!(world::SimWorld, fel::FutureEventList,
                                  rng::AbstractRNG, zone_id::Int,
                                  a::PoissonArrival, t::Float64)
    Δt = rand(rng, Exponential(1.0 / a.rate))
    schedule!(fel, EntityArrival(new_entity_id!(world), zone_id, t + Δt), t + Δt)
    return nothing
end

function _schedule_next_arrival!(world::SimWorld, fel::FutureEventList,
                                  rng::AbstractRNG, zone_id::Int,
                                  a::NHPPArrival, t::Float64)
    t_next = next_nhpp_arrival(a.schedule, t, rng)
    if isfinite(t_next)
        schedule!(fel, EntityArrival(new_entity_id!(world), zone_id, t_next), t_next)
    end
    return nothing
end

"""
    _priority_enqueue!(world, zone, entity_id, priority, t)

Insert `entity_id` into `zone.queue` maintaining non-increasing priority order.
Within the same priority, entities are ordered by arrival time (FIFO).

Implements non-preemptive Head-Of-Line (HOL) priority discipline.
Complexity: O(n) insertion via `searchsortedfirst`.
"""
function _priority_enqueue!(world::SimWorld, zone::ZoneState,
                             entity_id::UInt64, priority::Int, t::Float64)
    q = zone.queue
    if isempty(q)
        push!(q, entity_id)
        return
    end
    # Find insertion point: first position with strictly lower priority
    insert_pos = length(q) + 1
    for (i, qid) in enumerate(q)
        qa = get_des_agent(world, qid)
        qa === nothing && continue
        if qa.priority < priority
            insert_pos = i
            break
        end
    end
    insert!(q, insert_pos, entity_id)
end

function _eval_custom_comparator(fn::Function, world::SimWorld, id_a::UInt64, id_b::UInt64)::Bool
    ea = SimCore.get_entity_state(world, id_a)
    eb = SimCore.get_entity_state(world, id_b)
    try
        return Bool(Base.invokelatest(fn, ea, eb))
    catch
        return ea.arrival_time < eb.arrival_time
    end
end

function _numeric_attr(world::SimWorld, id::UInt64, key::String, fallback::Float64=Inf)::Float64
    val = SimCore.get_entity_attribute(world, id, key, fallback)
    return val isa Real ? Float64(val) : fallback
end

function _sort_queue_by_discipline!(world::SimWorld, zone::ZoneState, cfg::ZoneConfig)
    length(zone.queue) <= 1 && return nothing
    if cfg.custom_discipline !== nothing
        fn = cfg.custom_discipline
        sort!(zone.queue, lt = (a, b) -> _eval_custom_comparator(fn, world, a, b))
    elseif cfg.queue_discipline == EDD
        sort!(zone.queue, lt = (a, b) -> begin
            da = _numeric_attr(world, a, "due_date", Inf)
            db = _numeric_attr(world, b, "due_date", Inf)
            if da != db
                return da < db
            end
            aga = get_des_agent(world, a)
            agb = get_des_agent(world, b)
            ta = aga !== nothing ? aga.arrival_time : 0.0
            tb = agb !== nothing ? agb.arrival_time : 0.0
            return ta < tb
        end)
    elseif cfg.queue_discipline == SPT
        sort!(zone.queue, lt = (a, b) -> begin
            sa = _numeric_attr(world, a, "estimated_service", _numeric_attr(world, a, "service_time", Inf))
            sb = _numeric_attr(world, b, "estimated_service", _numeric_attr(world, b, "service_time", Inf))
            if sa != sb
                return sa < sb
            end
            aga = get_des_agent(world, a)
            agb = get_des_agent(world, b)
            ta = aga !== nothing ? aga.arrival_time : 0.0
            tb = agb !== nothing ? agb.arrival_time : 0.0
            return ta < tb
        end)
    elseif cfg.queue_discipline == PRIORITY_HOL
        sort!(zone.queue, lt = (a, b) -> begin
            aga = get_des_agent(world, a)
            agb = get_des_agent(world, b)
            pa = aga !== nothing ? aga.priority : 0
            pb = agb !== nothing ? agb.priority : 0
            if pa != pb
                return pa > pb
            end
            ta = aga !== nothing ? aga.arrival_time : 0.0
            tb = agb !== nothing ? agb.arrival_time : 0.0
            return ta < tb
        end)
    end
    return nothing
end

function _enqueue_entity!(world::SimWorld, zone::ZoneState, cfg::ZoneConfig,
                          entity_id::UInt64, priority::Int, t::Float64)
    if cfg.custom_discipline !== nothing || cfg.queue_discipline in (EDD, SPT)
        push!(zone.queue, entity_id)
        _sort_queue_by_discipline!(world, zone, cfg)
    elseif cfg.queue_discipline == PRIORITY_HOL && priority != 0
        _priority_enqueue!(world, zone, entity_id, priority, t)
    elseif cfg.queue_discipline == LIFO
        pushfirst!(zone.queue, entity_id)
    else
        push!(zone.queue, entity_id)
    end
    return nothing
end

function _dequeue_next_entity!(world::SimWorld, zone::ZoneState, cfg::ZoneConfig)::UInt64
    if cfg.custom_discipline !== nothing || cfg.queue_discipline in (EDD, SPT)
        _sort_queue_by_discipline!(world, zone, cfg)
    end
    return popfirst!(zone.queue)
end

"""
    _record_zone_arrival!(world, zone_id)

Record an arrival in the per-zone SimStats (if registered).
"""
function _record_zone_arrival!(world::SimWorld, zone_id::Int)
    zs = get(world.zone_stats, zone_id, nothing)
    zs !== nothing && record_arrival!(zs)
end

"""
    _record_zone_departure!(world, zone_id, wait_time, sojourn)

Record a departure in the per-zone SimStats (if registered).
"""
function _record_zone_departure!(world::SimWorld, zone_id::Int,
                                  wait_time::Float64, sojourn::Float64)
    zs = get(world.zone_stats, zone_id, nothing)
    zs !== nothing && record_departure!(zs, wait_time, sojourn)
end

"""
    _handle_fork!(world, fel, configs, rng, e, fj_cfg, t)

Fork: spawn parallel sub-entities at each sub-zone.
Records a join barrier keyed by `e.entity_id` tracking how many sub-tasks remain.
"""
function _handle_fork!(world::SimWorld, fel::FutureEventList,
                        configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                        e::EntityArrival, fj_cfg::ForkJoinConfig, t::Float64)
    n_tasks = length(fj_cfg.sub_zones)
    # Register join barrier: (total_tasks, completed=0, system_entry_time)
    world.join_barriers[e.entity_id] = (n_tasks, 0, t)

    for sub_zone_id in fj_cfg.sub_zones
        sub_id = new_entity_id!(world)
        world.sub_entity_map[sub_id] = e.entity_id   # sub → parent mapping
        # Sub-entity arrivals: is_external=false — must NOT trigger next arrival at sub-zone
        schedule!(fel, EntityArrival(sub_id, sub_zone_id, t, e.priority, false), t)
    end
end

"""
    _handle_join!(world, fel, configs, rng, sub_entity_id, t)

Join: decrement join barrier counter for the parent entity.
When all sub-tasks complete, record total fork-join sojourn time.
"""
function _handle_join!(world::SimWorld, fel::FutureEventList,
                        configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                        sub_entity_id::UInt64, t::Float64;
                        pipeline::Union{Nothing,StatsPipeline}=nothing)
    parent_id = world.sub_entity_map[sub_entity_id]
    delete!(world.sub_entity_map, sub_entity_id)

    barrier = get(world.join_barriers, parent_id, nothing)
    barrier === nothing && return

    total, done, entry_t = barrier
    done += 1
    if done >= total
        # All sub-tasks complete — record total fork-join sojourn
        join_sojourn = t - entry_t
        record_arrival!(world.stats)    # count this as one order arrival
        _record_arrival_optional!(pipeline)
        record_departure!(world.stats, 0.0, join_sojourn)   # Wq=0 for fork-join
        _record_departure_optional!(pipeline, 0.0, join_sojourn)
        delete!(world.join_barriers, parent_id)
        delete!(world.entry_times, parent_id)   # clean up entry_times
    else
        world.join_barriers[parent_id] = (total, done, entry_t)
    end
end

"""
    _mean_service_time(cfg) -> Float64

Extract mean service time (E[S] = 1/μ) from zone configuration.
"""
_mean_service_time(cfg::ZoneConfig) = mean(cfg.service_dist.dist)

# ─────────────────────────────────────────────────────────────────────────────
# SimViz M-1: Service overrides, CustomUserEvent, Conveyor Physics & Hook Commands
# ─────────────────────────────────────────────────────────────────────────────

@inline function _compute_service_duration!(
    world::SimWorld,
    rng::AbstractRNG,
    zone_id::Int,
    entity_id::UInt64,
    cfg::ZoneConfig
)::Float64
    base_st = if !isempty(world.zone_service_override) && haskey(world.zone_service_override, (zone_id, entity_id))
        pop!(world.zone_service_override, (zone_id, entity_id))
    else
        cfg.service_dist(rng)
    end
    extra_setup = if !isempty(world.zone_setup_time) && haskey(world.zone_setup_time, (zone_id, entity_id))
        pop!(world.zone_setup_time, (zone_id, entity_id))
    else
        0.0
    end
    return max(0.0001, base_st + extra_setup)
end

function _is_downstream_blocked(
    world::SimWorld,
    configs::Dict{Int, ZoneConfig},
    station_id::Int,
    cfg::ZoneConfig
)::Bool
    if cfg.routing isa FixedRoute
        dest_id = cfg.routing.to
        if haskey(world.zone_states, dest_id)
            dz = world.zone_states[dest_id]
            dcfg = get(configs, dest_id, nothing)
            cap = dcfg !== nothing ? min(dz.capacity, dcfg.capacity) : dz.capacity
            if dz.num_servers == 0 || (dz.queue_length + dz.busy_servers) >= cap
                return true
            end
        end
    end
    return false
end

function _recompute_accumulating_conveyor!(
    world::SimWorld,
    fel::FutureEventList,
    configs::Dict{Int, ZoneConfig},
    zone_id::Int,
    cfg::ZoneConfig,
    t::Float64
)
    pitch = Float64(SimCore.get_zone_attribute(world, zone_id, "_conveyor_pitch", cfg.conveyor_pitch))
    plen  = Float64(SimCore.get_zone_attribute(world, zone_id, "_path_length", cfg.path_length))
    spd   = max(0.001, Float64(SimCore.get_zone_attribute(world, zone_id, "_nominal_speed", cfg.nominal_speed)))
    blocked_out = _is_downstream_blocked(world, configs, zone_id, cfg)

    # Collect all items currently on this conveyor
    items = Tuple{UInt64, SimCore.EntityKinematics, Float64}[]
    for (uid, k) in world.entity_kinematics
        if k.zone_id == zone_id
            d_now = SimCore.kinematics_distance(k, t)
            push!(items, (uid, k, d_now))
        end
    end
    isempty(items) && return nothing

    # Sort from front (closest to outlet, highest distance) to back (lowest distance)
    sort!(items, by = x -> (-x[3], x[1]))

    for (idx, (uid, k, d_now)) in enumerate(items)
        stop_pos = max(0.0, plen - (idx - 1) * pitch)
        k.base_distance = min(d_now, stop_pos)
        k.last_update_time = t

        if idx == 1 && !blocked_out
            # Leading item and downstream is open -> move to outlet atplen
            rem_dist = max(0.0, plen - k.base_distance)
            k.current_speed = spd
            if k.exit_event_id != 0
                cancel!(fel, k.exit_event_id)
            end
            dt = rem_dist / spd
            k.exit_event_id = schedule!(fel, ProcessComplete(uid, zone_id, t + dt), t + dt)
        else
            # Either downstream is blocked OR we are behind another item
            # Check if leader is stopped or moving
            leader_stopped = (idx == 1 && blocked_out) || (idx > 1 && items[idx - 1][2].current_speed == 0.0)
            if leader_stopped
                if k.base_distance >= stop_pos - 1e-4
                    # Reached accumulation slot -> stop!
                    k.base_distance = stop_pos
                    k.current_speed = 0.0
                    if k.exit_event_id != 0
                        cancel!(fel, k.exit_event_id)
                        k.exit_event_id = UInt64(0)
                    end
                else
                    # Still moving toward stop_pos; cancel exit event if it would overshoot
                    k.current_speed = spd
                    if k.exit_event_id != 0
                        cancel!(fel, k.exit_event_id)
                        k.exit_event_id = UInt64(0)
                    end
                    # Schedule an internal arrival at the stop position via CustomUserEvent
                    dt_stop = (stop_pos - k.base_distance) / spd
                    schedule!(fel, SimCore.CustomUserEvent(zone_id, "", :_accum_check, t + dt_stop), t + dt_stop)
                end
            else
                # Leader is moving toward outlet
                rem_dist = max(0.0, plen - k.base_distance)
                k.current_speed = spd
                if k.exit_event_id != 0
                    cancel!(fel, k.exit_event_id)
                end
                dt = rem_dist / spd + (idx - 1) * (pitch / spd)
                k.exit_event_id = schedule!(fel, ProcessComplete(uid, zone_id, t + dt), t + dt)
            end
        end
    end
    return nothing
end

function _unblock_upstream_accumulating_conveyors!(
    world::SimWorld,
    fel::FutureEventList,
    configs::Dict{Int, ZoneConfig},
    freed_zone_id::Int,
    t::Float64
)
    for (uzid, ucfg) in configs
        cmode = !isempty(world.zone_attributes) ?
                SimCore.get_zone_attribute(world, uzid, "_conveyor_mode", ucfg.conveyor_mode) :
                ucfg.conveyor_mode
        if cmode === :accumulating && ucfg.routing isa FixedRoute && ucfg.routing.to == freed_zone_id
            if !_is_downstream_blocked(world, configs, uzid, ucfg)
                _recompute_accumulating_conveyor!(world, fel, configs, uzid, ucfg, t)
            end
        end
    end
    return nothing
end

function _try_pull_from_upstream_queues!(
    world::SimWorld,
    fel::FutureEventList,
    configs::Dict{Int, ZoneConfig},
    rng::AbstractRNG,
    station_id::Int,
    zone::ZoneState,
    cfg::ZoneConfig,
    t::Float64
)
    zone.busy_servers >= zone.num_servers && return nothing
    pd = world.port_directory
    h = get(pd.zone_to_handle, station_id, SimCore.INVALID_HANDLE)
    !isvalid(h) && return nothing

    imode = SimCore.get_zone_attribute(world, station_id, "_intake_mode", cfg.intake_mode)
    imode === :custom && return nothing   # handled by user's on_pull hook

    ctx = SimCore.HookContext(world, 0, station_id, get(pd.handle_to_name, h, ""), t; rng=rng, fel=fel, configs=configs)
    pulled = if imode === :round_robin
        SimCore.SimViz.pull_from_port_round_robin!(ctx, :in_flow)
    elseif imode === :longest_queue
        SimCore.SimViz.pull_from_port_longest!(ctx, :in_flow)
    elseif imode === :highest_fill
        SimCore.SimViz.pull_from_port_highest_fill!(ctx, :in_flow)
    else
        SimCore.SimViz.pull_from_port_slot_order!(ctx, :in_flow)
    end

    if isvalid(pulled)
        uid = UInt64(pulled.id)
        zone.busy_servers += 1
        ag = get_des_agent(world, uid)
        arr_t = ag !== nothing ? ag.arrival_time : t
        prio  = ag !== nothing ? ag.priority : 0
        world.des_agents[uid] = DESAgent(arr_t, station_id, prio, t)
        st = _compute_service_duration!(world, rng, station_id, uid, cfg)
        schedule!(fel, ProcessComplete(uid, station_id, t + st), t + st)
    end
    return nothing
end

"""
    dispatch!(world, fel, configs, rng, e::CustomUserEvent, t)

Handle a user-scheduled event (`schedule_event!`, `schedule_at!`, `schedule_every!`, `after!`).
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::SimCore.CustomUserEvent, t::Float64;
                   pipeline::Union{Nothing,StatsPipeline}=nothing,
                   sync_bufs::Union{Nothing,HybridSyncBuffers}=nothing)
    if e.zone_id > 0 && haskey(world.zone_states, e.zone_id)
        _update_time_averages!(world, world.zone_states[e.zone_id], e.zone_id, t; pipeline=pipeline)
    end

    if e.tag === :_accum_check && e.zone_id > 0 && haskey(configs, e.zone_id)
        _recompute_accumulating_conveyor!(world, fel, configs, e.zone_id, configs[e.zone_id], t)
        return nothing
    elseif e.tag === :_index_pulse && e.zone_id > 0 && haskey(configs, e.zone_id)
        cfg = configs[e.zone_id]
        cmode = !isempty(world.zone_attributes) ?
                SimCore.get_zone_attribute(world, e.zone_id, "_conveyor_mode", cfg.conveyor_mode) :
                cfg.conveyor_mode
        if cmode === :indexing
            pitch = Float64(SimCore.get_zone_attribute(world, e.zone_id, "_conveyor_pitch", cfg.conveyor_pitch))
            has_remaining = false
            for (uid, ag) in world.des_agents
                if ag.current_zone == e.zone_id && haskey(world.entity_kinematics, uid)
                    k = world.entity_kinematics[uid]
                    new_d = clamp(SimCore.kinematics_distance(k, t) + pitch, 0.0, k.path_length)
                    k.base_distance = new_d
                    k.last_update_time = t
                    if new_d >= k.path_length - 1e-9 && k.exit_event_id == 0
                        k.exit_event_id = schedule!(fel, ProcessComplete(uid, e.zone_id, t), t)
                    else
                        has_remaining = true
                    end
                end
            end
            if has_remaining
                iv = max(1e-4, Float64(SimCore.get_zone_attribute(world, e.zone_id, "_conveyor_index_interval", cfg.conveyor_index_interval)))
                schedule!(fel, SimCore.CustomUserEvent(e.zone_id, e.element_id, :_index_pulse, t + iv; interval=iv), t + iv)
            else
                SimCore.set_zone_attribute!(world, e.zone_id, "_index_pulse_active", false)
            end
        else
            SimCore.set_zone_attribute!(world, e.zone_id, "_index_pulse_active", false)
        end
        return nothing
    end

    if e.callback !== nothing
        ctx = SimCore.HookContext(world, 0, e.zone_id, e.element_id, t;
                                  rng=rng, fel=fel, configs=configs,
                                  event_tag=e.tag, event_payload=e.payload)
        SimCore.with_hook_context(ctx) do _
            Base.invokelatest(e.callback)
        end
        apply_hook_commands!(world, fel, configs, rng, ctx)
    end

    # Reschedule if recurring (schedule_every!)
    if e.interval > 0.0
        next_t = t + e.interval
        next_ev = SimCore.CustomUserEvent(e.zone_id, e.element_id, e.tag, next_t, e.payload, e.interval, e.callback)
        cev_id = schedule!(fel, next_ev, next_t)
        if e.zone_id > 0
            evs = get!(world.active_user_events, e.zone_id, Set{UInt64}())
            push!(evs, cev_id)
        end
    end
    return nothing
end

"""
    apply_hook_commands!(world, fel, configs, rng, ctx::HookContext)

Execute structural simulation commands buffered in `ctx.commands` during a hook invocation.
"""
function apply_hook_commands!(
    world::SimWorld,
    fel::FutureEventList,
    configs::Dict{Int, ZoneConfig},
    rng::AbstractRNG,
    ctx::SimCore.HookContext
)
    t = ctx.t
    # 1. Priority override on current item
    if ctx.priority_override !== nothing && ctx.entity_id > 0
        new_p = ctx.priority_override
        ag = get_des_agent(world, ctx.entity_id)
        if ag !== nothing
            world.des_agents[ctx.entity_id] = DESAgent(ag.arrival_time, ag.current_zone, new_p, ag.service_start_time)
            if haskey(world.zone_states, ag.current_zone) && haskey(configs, ag.current_zone)
                _sort_queue_by_discipline!(world, world.zone_states[ag.current_zone], configs[ag.current_zone])
            end
        end
    end

    # 2. Route override on current item
    if ctx.route_override !== nothing && ctx.entity_id > 0
        if ctx.route_override === :exit
            world.entity_route_overrides[ctx.entity_id] = -1
        elseif ctx.route_override isa Integer && ctx.route_override > 0
            world.entity_route_overrides[ctx.entity_id] = Int(ctx.route_override)
        end
    end

    isempty(ctx.commands) && return nothing

    for cmd in ctx.commands
        op = cmd.op
        if op == SimCore.OP_SET_SERVICE_TIME || op == SimCore.OP_ADD_SETUP_TIME
            # If the entity already started service at time t, reschedule its ProcessComplete event!
            uid = UInt64(max(0, cmd.target_id))
            zid = Int(cmd.int_arg)
            if uid > 0 && zid > 0 && haskey(configs, zid)
                # Cancel any existing ProcessComplete for (uid, zid) in FEL
                for (cev, _) in fel.queue
                    if cev.inner isa ProcessComplete && cev.inner.entity_id == uid && cev.inner.station_id == zid
                        cancel!(fel, cev.id)
                    end
                end
                st = _compute_service_duration!(world, rng, zid, uid, configs[zid])
                cev_id = schedule!(fel, ProcessComplete(uid, zid, t + st), t + st)
                if haskey(world.entity_kinematics, uid)
                    world.entity_kinematics[uid].exit_event_id = cev_id
                end
            end

        elseif op == SimCore.OP_START_SERVICE
            uid = UInt64(max(0, cmd.target_id))
            zid = Int(cmd.int_arg)
            dur = max(0.0001, cmd.float_arg)
            if uid > 0 && zid > 0 && haskey(world.zone_states, zid)
                zs = world.zone_states[zid]
                qidx = findfirst(==(uid), zs.queue)
                if qidx !== nothing
                    deleteat!(zs.queue, qidx)
                    zs.queue_length = length(zs.queue)
                end
                zs.busy_servers += 1
                ag = get_des_agent(world, uid)
                arr_t = ag !== nothing ? ag.arrival_time : t
                prio  = ag !== nothing ? ag.priority : 0
                world.des_agents[uid] = DESAgent(arr_t, zid, prio, t)
                schedule!(fel, ProcessComplete(uid, zid, t + dur), t + dur)
            end

        elseif op == SimCore.OP_COMPLETE_SERVICE
            uid = UInt64(max(0, cmd.target_id))
            zid = Int(cmd.int_arg)
            if uid > 0 && zid > 0
                for (cev, _) in fel.queue
                    if cev.inner isa ProcessComplete && cev.inner.entity_id == uid && cev.inner.station_id == zid
                        cancel!(fel, cev.id)
                    end
                end
                schedule!(fel, ProcessComplete(uid, zid, t), t)
            end

        elseif op == SimCore.OP_SCHEDULE_EVENT
            zid = Int(cmd.target_id)
            delay = max(0.0, cmd.float_arg)
            interval = max(0.0, cmd.float_arg2)
            payload, cb = cmd.any_arg isa Tuple ? cmd.any_arg : (cmd.any_arg, nothing)
            ev = SimCore.CustomUserEvent(zid, cmd.str_arg, cmd.sym_arg, t + delay, payload, interval, cb)
            cev_id = schedule!(fel, ev, t + delay)
            if zid > 0
                evs = get!(world.active_user_events, zid, Set{UInt64}())
                push!(evs, cev_id)
            end

        elseif op == SimCore.OP_CANCEL_EVENT
            cancel!(fel, UInt64(max(0, cmd.target_id)))

        elseif op == SimCore.OP_FORWARD_ENTITY
            uid = UInt64(max(0, cmd.target_id))
            dest_z = Int(cmd.int_arg)
            delay = max(0.0, cmd.float_arg)
            zid = ctx.zone_id
            if haskey(world.zone_states, zid)
                zs = world.zone_states[zid]
                qidx = findfirst(==(uid), zs.queue)
                if qidx !== nothing
                    deleteat!(zs.queue, qidx)
                    zs.queue_length = length(zs.queue)
                else
                    zs.busy_servers = max(0, zs.busy_servers - 1)
                end
            end
            ag = get_des_agent(world, uid)
            prio = ag !== nothing ? ag.priority : 0
            if dest_z > 0 && haskey(configs, dest_z)
                world.des_agents[uid] = DESAgent(t + delay, dest_z, prio, Inf)
                schedule!(fel, EntityArrival(uid, dest_z, t + delay, prio, false), t + delay)
            else
                _record_system_exit!(world, uid, ag !== nothing ? ag.arrival_time : t, t)
            end

        elseif op == SimCore.OP_SET_CONVEYOR_MODE
            zid = Int(cmd.target_id)
            if zid > 0 && haskey(configs, zid)
                if cmd.sym_arg === :accumulating
                    _recompute_accumulating_conveyor!(world, fel, configs, zid, configs[zid], t)
                end
            end

        elseif op == SimCore.OP_SET_SPEED
            if cmd.int_arg == 1
                # Conveyor speed updated -> reschedule exit events for items on conveyor
                zid = Int(cmd.target_id)
                new_spd = cmd.float_arg
                cmode = SimCore.get_zone_attribute(world, zid, "_conveyor_mode", :free_flow)
                if cmode === :accumulating && haskey(configs, zid)
                    _recompute_accumulating_conveyor!(world, fel, configs, zid, configs[zid], t)
                else
                    for (uid, k) in world.entity_kinematics
                        if k.zone_id == zid
                            if k.exit_event_id != 0
                                cancel!(fel, k.exit_event_id)
                                k.exit_event_id = UInt64(0)
                            end
                            if new_spd > 0.0
                                rem = max(0.0, k.path_length - k.base_distance)
                                dt = rem / new_spd
                                k.exit_event_id = schedule!(fel, ProcessComplete(uid, zid, t + dt), t + dt)
                            end
                        end
                    end
                end
            else
                uid = UInt64(max(0, cmd.target_id))
                new_spd = cmd.float_arg
                k = get(world.entity_kinematics, uid, nothing)
                if k !== nothing
                    if k.exit_event_id != 0
                        cancel!(fel, k.exit_event_id)
                        k.exit_event_id = UInt64(0)
                    end
                    if new_spd > 0.0
                        rem = max(0.0, k.path_length - k.base_distance)
                        dt = rem / new_spd
                        k.exit_event_id = schedule!(fel, ProcessComplete(uid, k.zone_id, t + dt), t + dt)
                    end
                end
            end

        elseif op == SimCore.OP_STEP_DISTANCE
            zid = Int(cmd.target_id)
            for (uid, k) in collect(world.entity_kinematics)
                if k.zone_id == zid && k.base_distance >= k.path_length - 1e-6
                    if k.exit_event_id == 0
                        k.exit_event_id = schedule!(fel, ProcessComplete(uid, zid, t), t)
                    end
                end
            end

        elseif op == SimCore.OP_SPAWN_TO_PORT || op == SimCore.OP_CLONE_ENTITY
            uid = UInt64(max(0, cmd.target_id))
            dest_z = Int(cmd.int_arg)
            prio = Int(SimCore.get_entity_attribute(world, uid, "priority", 0))
            if dest_z > 0 && haskey(configs, dest_z)
                schedule!(fel, EntityArrival(uid, dest_z, t, prio, false), t)
            end

        elseif op == SimCore.OP_DESTROY_ENTITY
            uid = UInt64(max(0, cmd.target_id))
            zid = Int(cmd.int_arg)
            if haskey(world.zone_states, zid)
                zs = world.zone_states[zid]
                qidx = findfirst(==(uid), zs.queue)
                if qidx !== nothing
                    deleteat!(zs.queue, qidx)
                    zs.queue_length = length(zs.queue)
                end
            end
            for (cev, _) in fel.queue
                if (cev.inner isa ProcessComplete || cev.inner isa EntityArrival) && cev.inner.entity_id == uid
                    cancel!(fel, cev.id)
                end
            end
            remove_des_agent!(world, uid)

        elseif op == SimCore.OP_TRIGGER_FAILURE
            zid = Int(cmd.target_id)
            rep_dur = max(0.001, cmd.float_arg)
            if zid > 0 && haskey(world.zone_states, zid)
                zs = world.zone_states[zid]
                zs.busy_servers = max(0, zs.busy_servers - 1)
                zs.num_servers  = max(0, zs.num_servers - 1)
                schedule!(fel, ScheduledChange{:Repair}(zid, t + rep_dur), t + rep_dur)
            end

        elseif op == SimCore.OP_TRIGGER_REPAIR
            zid = Int(cmd.target_id)
            if zid > 0 && haskey(world.zone_states, zid)
                schedule!(fel, ScheduledChange{:Repair}(zid, t), t)
            end

        elseif op == SimCore.OP_FLUSH_QUEUE
            zid = Int(cmd.target_id)
            dest_z = Int(cmd.int_arg)
            if zid > 0 && haskey(world.zone_states, zid)
                zs = world.zone_states[zid]
                flushed = copy(zs.queue)
                empty!(zs.queue)
                zs.queue_length = 0
                for uid in flushed
                    ag = get_des_agent(world, uid)
                    prio = ag !== nothing ? ag.priority : 0
                    if dest_z > 0 && haskey(configs, dest_z)
                        world.des_agents[uid] = DESAgent(t, dest_z, prio, Inf)
                        schedule!(fel, EntityArrival(uid, dest_z, t, prio, false), t)
                    else
                        _record_system_exit!(world, uid, ag !== nothing ? ag.arrival_time : t, t)
                    end
                end
            end
        end
    end
    empty!(ctx.commands)
    return nothing
end

