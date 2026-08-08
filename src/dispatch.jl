"""
    dispatch.jl — DES Event Dispatch (Multiple Dispatch routing)

Each `dispatch!` method handles one `SimEvent` subtype.
Julia's multiple dispatch routes each event to the correct handler
with zero overhead — no if/elseif chains, no vtable lookups.

Performance: O(log n) FEL dequeue + O(1) FIFO queue ops per event.
Correctness: Exact Wq tracked via service_start_time in DESAgent.

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
2. If system is at capacity (M/M/1/K blocking) → reject entity
3. If server is free → start service immediately; set service_start_time = t (Wq = 0)
4. If all servers busy → join FIFO queue; service_start_time left as Inf until served
5. Schedule next Poisson arrival (if zone has arrival_rate > 0)
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::EntityArrival, t::Float64)
    zone = get_zone(world, e.zone_id)
    cfg  = configs[e.zone_id]

    # ── Time-average stats for interval since last event
    _update_time_averages!(world.stats, zone, t)

    # ── Record arrival (before blocking check)
    record_arrival!(world.stats)

    # ── Check finite buffer (M/M/1/K blocking)
    entities_in_system = zone.queue_length + zone.busy_servers
    if entities_in_system >= cfg.capacity
        # Entity rejected — buffer full
        record_blocked!(world.stats)
    else
        if zone.busy_servers < cfg.num_servers
            # ── Server free → begin service immediately (Wq = 0)
            zone.busy_servers += 1
            agent = DESAgent(t, e.zone_id, 0, t)   # service_start_time = t → Wq = 0
            add_des_agent!(world, e.entity_id, agent)
            service_time = cfg.service_dist(rng)
            schedule!(fel, ProcessComplete(e.entity_id, e.zone_id, t + service_time),
                      t + service_time)
        else
            # ── All servers busy → join FIFO queue (service_start_time = Inf until claimed)
            agent = DESAgent(t, e.zone_id, 0, Inf)
            add_des_agent!(world, e.entity_id, agent)
            zone.queue_length += 1
            push!(zone.queue, e.entity_id)   # O(1) enqueue
        end
    end

    # ── Schedule next arrival from this zone's Poisson process (if λ > 0)
    if cfg.arrival_rate > 0.0
        Δt = rand(rng, Exponential(1.0 / cfg.arrival_rate))
        schedule!(fel, EntityArrival(new_entity_id!(world), e.zone_id, t + Δt), t + Δt)
    end
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
3. Remove entity from world
4. If queue is non-empty → popfirst! (O(1)), update service_start_time, schedule ProcessComplete
5. Else → server goes idle (decrement busy_servers)
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::ProcessComplete, t::Float64)
    zone = get_zone(world, e.station_id)
    cfg  = configs[e.station_id]

    # ── Time-average stats for interval since last event
    _update_time_averages!(world.stats, zone, t)

    # ── Record departure with exact Wq and W for departing entity
    agent = get_des_agent(world, e.entity_id)
    if agent !== nothing
        sojourn   = t - agent.arrival_time
        wait_time = (agent.service_start_time == Inf) ? 0.0 :
                    agent.service_start_time - agent.arrival_time
        record_departure!(world.stats, wait_time, sojourn)
        remove_entity!(world, e.entity_id)
    end

    # ── Serve next in queue (if any) — O(1) FIFO popfirst!
    if zone.queue_length > 0
        zone.queue_length -= 1
        next_id = popfirst!(zone.queue)   # O(1) amortized
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
# ResourceFailure — machine/server goes down (2A-09)
# ─────────────────────────────────────────────────────────────────────────────

"""
    dispatch!(world, fel, configs, rng, e::ResourceFailure, t)

Handle a resource (machine/server) failure.

Reduces effective server count by 1 and schedules a repair.
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::ResourceFailure, t::Float64)
    zone = get_zone(world, e.resource_id)
    cfg  = configs[e.resource_id]

    _update_time_averages!(world.stats, zone, t)

    # Reduce effective server count (minimum 0)
    zone.busy_servers  = max(0, zone.busy_servers - 1)
    zone.num_servers   = max(1, zone.num_servers - 1)

    # Schedule repair (mean repair time = 10× mean service time)
    mean_service = _mean_service_time(cfg)
    repair_time  = rand(rng, Exponential(10.0 * mean_service))
    schedule!(fel, ScheduledChange{:Repair}(e.resource_id, t + repair_time),
              t + repair_time)
end

"""
    dispatch!(world, fel, configs, rng, e::ScheduledChange{:Repair}, t)

Restore one server after a machine repair; serve a waiting entity if any.
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::ScheduledChange{:Repair}, t::Float64)
    zone = get_zone(world, e.zone_id)
    cfg  = configs[e.zone_id]

    _update_time_averages!(world.stats, zone, t)

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
end

# ─────────────────────────────────────────────────────────────────────────────
# TransferOut — entity moves between zones
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
    _update_time_averages!(stats, zone, t)

Record time-weighted statistics for the period [zone.last_event_time, t].
Called at the start of every event handler before mutating zone state.
"""
function _update_time_averages!(stats::SimStats, zone::ZoneState, t::Float64)
    dt = t - zone.last_event_time
    if dt > 0.0
        n_in_system = zone.queue_length + zone.busy_servers
        record_queue_length!(stats, n_in_system, dt)
        if zone.busy_servers > 0
            # Record per-server utilisation: fraction of server capacity in use
            # For M/M/1:  busy_servers/num_servers = 1/1 = 1 (same as before)
            # For M/M/c:  e.g. 2 busy / 4 servers = 0.5 → ρ = λ/(c·μ)
            frac_busy = zone.busy_servers / zone.num_servers
            record_utilization!(stats, frac_busy * dt)
        end
    end
    zone.last_event_time = t
    return nothing
end


"""
    _mean_service_time(cfg) -> Float64

Extract mean service time (E[S] = 1/μ) from zone configuration.
"""
_mean_service_time(cfg::ZoneConfig) = mean(cfg.service_dist.dist)
