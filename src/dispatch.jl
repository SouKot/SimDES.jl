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
# NullEvent — Chandy-Misra null message; no-op in Tier 1
# ─────────────────────────────────────────────────────────────────────────────

dispatch!(world, fel, configs, rng, ::NullEvent, t) = nothing

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
                   e::EntityArrival, t::Float64)
    zone = get_zone(world, e.zone_id)
    cfg  = configs[e.zone_id]

    # ── Time-average stats for interval since last event
    _update_time_averages!(world, zone, e.zone_id, t)

    # ── Record system-entry time (first time we see this entity)
    if !haskey(world.entry_times, e.entity_id)
        world.entry_times[e.entity_id] = t
    end

    # ── Fork-join: if this is a FORK zone, spawn sub-entities and return
    if cfg.fork_join !== nothing
        _handle_fork!(world, fel, configs, rng, e, cfg.fork_join, t)
        # Only self-schedule next external arrival if THIS arrival was external
        e.is_external && _schedule_next_arrival!(world, fel, configs, rng, e.zone_id, cfg, t)
        return
    end

    # ── Record arrival (before blocking check)
    record_arrival!(world.stats)
    _record_zone_arrival!(world, e.zone_id)

    # ── Check finite buffer (M/M/1/K blocking)
    entities_in_system = zone.queue_length + zone.busy_servers
    if entities_in_system >= cfg.capacity
        # Entity rejected — buffer full
        record_blocked!(world.stats)
        delete!(world.entry_times, e.entity_id)   # entity never entered system
    else
        if zone.busy_servers < zone.num_servers
            # ── Server free → begin service immediately (Wq = 0)
            # NOTE: uses zone.num_servers (runtime), not cfg.num_servers (static).
            # When machine fails, zone.num_servers is decremented to 0,
            # so this condition correctly becomes false (entities queue, not serve).
            zone.busy_servers += 1
            agent = DESAgent(t, e.zone_id, e.priority, t)   # service_start_time = t → Wq = 0
            add_des_agent!(world, e.entity_id, agent)
            service_time = cfg.service_dist(rng)
            schedule!(fel, ProcessComplete(e.entity_id, e.zone_id, t + service_time),
                      t + service_time)
        else
            # ── All servers busy (or down) → join queue
            agent = DESAgent(t, e.zone_id, e.priority, Inf)
            add_des_agent!(world, e.entity_id, agent)
            zone.queue_length += 1
            if cfg.queue_discipline == PRIORITY_HOL && e.priority != 0
                # Non-preemptive HOL: insert at correct priority position
                _priority_enqueue!(world, zone, e.entity_id, e.priority, t)
            else
                push!(zone.queue, e.entity_id)   # O(1) FIFO enqueue
            end
        end
    end

    # ── Schedule next arrival from this zone's Poisson/NHPP process.
    # CRITICAL: only fire for zone-owned external arrivals (is_external=true).
    # Routed arrivals (e.is_external==false) must NOT trigger a new external arrival
    # at the destination zone — that would exponentially inflate downstream traffic.
    e.is_external && _schedule_next_arrival!(world, fel, configs, rng, e.zone_id, cfg, t)
end

# ─────────────────────────────────────────────────────────────────────────────
# ProcessComplete — service finishes (DEVS δ_int)
# ─────────────────────────────────────────────────────────────────────────────

"""
    dispatch!(world, fel, configs, rng, e::ProcessComplete, t)

Handle a service completion at a station.

Logic:
1. Update time-average stats
2. Record exact Wq = service_start_time - arrival_time for departing entity
3. Route entity according to cfg.routing:
   - ExitSystem  → record total sojourn W, remove entity
   - FixedRoute  → schedule EntityArrival at next zone (entity persists in world)
   - ProbRoute   → sample destination, then route or exit
4. Check fork-join: if sub-entity, decrement barrier; fire join if complete
5. Serve next entity from queue (O(1) FIFO or O(1) priority dequeue)
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::ProcessComplete, t::Float64)
    zone = get_zone(world, e.station_id)
    cfg  = configs[e.station_id]

    # ── Time-average stats for interval since last event
    _update_time_averages!(world, zone, e.station_id, t)

    # ── Record departure stats and route/remove entity
    agent = get_des_agent(world, e.entity_id)
    if agent !== nothing
        wait_time = (agent.service_start_time == Inf) ? 0.0 :
                    agent.service_start_time - agent.arrival_time
        zone_sojourn = t - agent.arrival_time

        # ── Check fork-join: is this entity a sub-task completing?
        if haskey(world.sub_entity_map, e.entity_id)
            # Sub-task: record to zone stats only, NOT global stats.
            # The join completion (in _handle_join!) records to world.stats so
            # that sim_summary(world.stats).W reflects true fork-join sojourn.
            _record_zone_departure!(world, e.station_id, wait_time, zone_sojourn)
            _handle_join!(world, fel, configs, rng, e.entity_id, t)
            remove_des_agent!(world, e.entity_id)   # hot path: skip 3 wasted Dict ops
        else
            # Regular entity: record to both global and zone-specific stats
            record_departure!(world.stats, wait_time, zone_sojourn)
            _record_zone_departure!(world, e.station_id, wait_time, zone_sojourn)
            # ── Route entity according to routing policy
            _route_entity!(world, fel, configs, rng, e.entity_id, agent, cfg, t)
        end
    end

    # ── Serve next in queue (if any)
    if zone.queue_length > 0
        zone.queue_length -= 1
        next_id = popfirst!(zone.queue)   # O(1) for both FIFO and priority queues
        next_agent = get_des_agent(world, next_id)
        if next_agent !== nothing
            # Update service_start_time = now (entity waited until this moment)
            world.des_agents[next_id] = DESAgent(next_agent.arrival_time,
                                                  next_agent.current_zone,
                                                  next_agent.priority, t)
            service_time = cfg.service_dist(rng)
            schedule!(fel, ProcessComplete(next_id, e.station_id, t + service_time),
                      t + service_time)
        end
        # busy_servers stays the same — same server takes next entity
    else
        zone.busy_servers -= 1   # server goes idle
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
                   e::ResourceFailure, t::Float64)
    zone = get_zone(world, e.resource_id)
    cfg  = configs[e.resource_id]

    _update_time_averages!(world, zone, e.resource_id, t)

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
                   e::ScheduledChange{:Repair}, t::Float64)
    zone = get_zone(world, e.zone_id)
    cfg  = configs[e.zone_id]

    _update_time_averages!(world, zone, e.zone_id, t)

    zone.num_servers += 1   # restore one server

    # If a waiting entity exists and a server slot freed, start serving
    if zone.queue_length > 0 && zone.busy_servers < zone.num_servers
        zone.queue_length -= 1
        zone.busy_servers += 1
        next_id = popfirst!(zone.queue)
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
                   e::TransferOut, t::Float64)
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
function _update_time_averages!(world::SimWorld, zone::ZoneState, zone_id::Int, t::Float64)
    dt = t - zone.last_event_time
    if dt > 0.0
        n_in_system = zone.queue_length + zone.busy_servers
        record_queue_length!(world.stats, n_in_system, dt)
        if zone.busy_servers > 0
            # Per-server utilisation: fraction of server capacity in use
            frac_busy = zone.num_servers > 0 ?
                        zone.busy_servers / zone.num_servers : 0.0
            record_utilization!(world.stats, frac_busy * dt)
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
    _route_entity!(world, fel, configs, rng, entity_id, agent, cfg, t)

Route or remove an entity based on the zone's `RoutingPolicy`.
- `ExitSystem`: record total sojourn W, remove entity from world
- `FixedRoute(to)`: schedule `EntityArrival` at the next zone; update current_zone
- `ProbRoute(choices)`: sample destination, then route or exit
"""
function _route_entity!(world::SimWorld, fel::FutureEventList,
                        configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                        entity_id::UInt64, agent::DESAgent,
                        cfg::ZoneConfig, t::Float64)
    if cfg.routing isa ExitSystem
        # Entity exits — record total sojourn from system entry
        entry_t = get(world.entry_times, entity_id, agent.arrival_time)
        total_sojourn = t - entry_t
        delete!(world.entry_times, entity_id)
        remove_des_agent!(world, entity_id)   # hot path: skip 3 wasted Dict ops

    elseif cfg.routing isa FixedRoute
        dest = cfg.routing.to
        # Keep entity in world; update current_zone for DESAgent
        world.des_agents[entity_id] = DESAgent(t, dest, agent.priority, Inf)
        # Routed arrival: is_external=false — does NOT trigger next external arrival at dest
        schedule!(fel, EntityArrival(entity_id, dest, t, agent.priority, false), t)

    elseif cfg.routing isa ProbRoute
        dest = sample_destination(cfg.routing, rng)
        if dest === nothing
            # Exit system
            delete!(world.entry_times, entity_id)
            remove_des_agent!(world, entity_id)   # hot path: skip 3 wasted Dict ops
        else
            world.des_agents[entity_id] = DESAgent(t, dest, agent.priority, Inf)
            # Routed arrival: is_external=false — does NOT trigger next external arrival at dest
            schedule!(fel, EntityArrival(entity_id, dest, t, agent.priority, false), t)
        end
    end
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
                        sub_entity_id::UInt64, t::Float64)
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
        record_departure!(world.stats, 0.0, join_sojourn)   # Wq=0 for fork-join
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
