"""
    dispatch.jl — DES Event Dispatch (Multiple Dispatch routing)

Each `dispatch!` method handles one `SimEvent` subtype.
Julia's multiple dispatch routes each event to the correct handler
with zero overhead — no if/elseif chains, no vtable lookups.

Users can extend the system by adding new `dispatch!` methods
in their own code without modifying SimDES source.

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
struct MyCustomEvent <: SimEvent
    zone_id::Int
    time::Float64
end

function SimDES.dispatch!(world, fel, configs, rng, e::MyCustomEvent, t)
    # your logic here
end
```
"""
function dispatch! end

# ─────────────────────────────────────────────────────────────────────────────
# NullEvent — Chandy-Misra null message, no-op in Tier 1
# ─────────────────────────────────────────────────────────────────────────────

dispatch!(world, fel, configs, rng, ::NullEvent, t) = nothing

# ─────────────────────────────────────────────────────────────────────────────
# EntityArrival — entity enters zone (DEVS δ_ext)
# ─────────────────────────────────────────────────────────────────────────────

"""
    dispatch!(world, fel, configs, rng, e::EntityArrival, t)

Handle an entity arriving at a zone.

Logic:
1. Register entity in `world.des_agents`
2. Check if zone has capacity (M/M/1/K blocking)
3. If server is free → start service immediately, schedule `ProcessComplete`
4. If all servers busy → join queue (increment queue_length)
5. Record statistics: arrival, queue length time-average
6. Schedule next arrival (if this zone has an arrival_rate > 0)
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::EntityArrival, t::Float64)
    zone = get_zone(world, e.zone_id)
    cfg  = configs[e.zone_id]

    # ── Record time-average queue length BEFORE this event changes it
    dt = t - zone.last_event_time
    if dt > 0.0
        record_queue_length!(world.stats, zone.queue_length + zone.busy_servers, dt)
        record_utilization!(world.stats, zone.busy_servers > 0 ? dt : 0.0)
    end
    zone.last_event_time = t

    # ── Record arrival
    record_arrival!(world.stats)
    world.stats.warmup_complete || (world.stats.warmup_complete = (world.stats.total_arrivals >= _warmup_arrivals(cfg)))

    # ── Check finite buffer (M/M/1/K blocking)
    entities_in_system = zone.queue_length + zone.busy_servers
    if entities_in_system >= cfg.capacity
        record_blocked!(world.stats)
        # Entity is rejected — do NOT add to world
    else
        # Entity enters the system
        agent = DESAgent(t, e.zone_id)
        add_des_agent!(world, e.entity_id, agent)

        if zone.busy_servers < cfg.num_servers
            # A server is free → begin service immediately
            zone.busy_servers += 1
            service_time = cfg.service_dist(rng)
            schedule!(fel, ProcessComplete(e.entity_id, e.zone_id, t + service_time),
                      t + service_time)
        else
            # All servers busy → join queue
            zone.queue_length += 1
        end
    end

    # ── Schedule next arrival from this zone's Poisson process (if λ > 0)
    if cfg.arrival_rate > 0.0
        next_arrival_t = t + rand(rng, Exponential(1.0 / cfg.arrival_rate))
        next_id        = new_entity_id!(world)
        schedule!(fel, EntityArrival(next_id, e.zone_id, next_arrival_t),
                  next_arrival_t)
    end
end

# ─────────────────────────────────────────────────────────────────────────────
# ProcessComplete — service finishes (DEVS δ_int)
# ─────────────────────────────────────────────────────────────────────────────

"""
    dispatch!(world, fel, configs, rng, e::ProcessComplete, t)

Handle a service completion at a station.

Logic:
1. Record time-average stats for the interval since last event
2. Compute wait time (Wq) and sojourn time (W) for the departing entity
3. Remove entity from world
4. If queue is non-empty → dequeue next entity, start service, schedule ProcessComplete
5. Else → decrement busy_server count (server goes idle)
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::ProcessComplete, t::Float64)
    zone = get_zone(world, e.station_id)
    cfg  = configs[e.station_id]

    # ── Record time-average stats for the interval
    dt = t - zone.last_event_time
    if dt > 0.0
        record_queue_length!(world.stats, zone.queue_length + zone.busy_servers, dt)
        record_utilization!(world.stats, zone.busy_servers > 0 ? dt : 0.0)
    end
    zone.last_event_time = t

    # ── Compute and record sojourn time for departing entity
    agent = get_des_agent(world, e.entity_id)
    if agent !== nothing
        sojourn = t - agent.arrival_time
        # TODO (Phase 2B): To compute exact Wq, DESAgent needs a `service_start_time`
        # field set when service begins. Currently Wq=0 in all records; use Little's Law
        # (Wq = W - 1/μ) in post-processing if exact Wq is needed before 2B.
        record_departure!(world.stats, 0.0, sojourn)
        remove_entity!(world, e.entity_id)
    end

    # ── Serve next in queue (if any)
    if zone.queue_length > 0
        zone.queue_length -= 1
        # Dequeue oldest entity (FIFO — find the one with earliest arrival_time)
        next_id, next_agent = _dequeue_oldest(world, e.station_id)
        if next_id !== nothing
            service_time = cfg.service_dist(rng)
            schedule!(fel, ProcessComplete(next_id, e.station_id, t + service_time),
                      t + service_time)
        end
    else
        zone.busy_servers -= 1
    end
end

# ─────────────────────────────────────────────────────────────────────────────
# ResourceFailure — machine/server goes down (2A-09)
# ─────────────────────────────────────────────────────────────────────────────

"""
    dispatch!(world, fel, configs, rng, e::ResourceFailure, t)

Handle a resource (machine/server) failure.

Logic:
1. Record time stats
2. Mark resource as failed in zone state (reduce effective num_servers by 1)
3. Schedule repair event: `ScheduledChange{:Repair}` at t + repair_time
4. If severity == 1.0 (complete failure), remove in-service entity (lost)
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::ResourceFailure, t::Float64)
    zone = get_zone(world, e.resource_id)

    dt = t - zone.last_event_time
    if dt > 0.0
        record_queue_length!(world.stats, zone.queue_length + zone.busy_servers, dt)
        record_utilization!(world.stats, zone.busy_servers > 0 ? dt : 0.0)
    end
    zone.last_event_time = t

    # Reduce effective server count (floor at 0)
    zone.busy_servers = max(0, zone.busy_servers - 1)
    zone.num_servers  = max(1, zone.num_servers - 1)

    # Schedule repair (mean repair time = 1 / (1-availability); use 10× mean service)
    cfg          = configs[e.resource_id]
    repair_time  = rand(rng, Exponential(10.0 / max(cfg.arrival_rate, 0.01)))
    schedule!(fel, ScheduledChange{:Repair}(e.resource_id, t + repair_time),
              t + repair_time)
end

"""
    dispatch!(world, fel, configs, rng, e::ScheduledChange{:Repair}, t)

Handle a machine repair completion — restore one server.
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::ScheduledChange{:Repair}, t::Float64)
    zone = get_zone(world, e.zone_id)
    cfg  = configs[e.zone_id]

    dt = t - zone.last_event_time
    if dt > 0.0
        record_queue_length!(world.stats, zone.queue_length + zone.busy_servers, dt)
        record_utilization!(world.stats, zone.busy_servers > 0 ? dt : 0.0)
    end
    zone.last_event_time = t

    zone.num_servers += 1   # restore one server

    # If entities are waiting, start serving them now
    if zone.queue_length > 0 && zone.busy_servers < zone.num_servers
        zone.queue_length -= 1
        zone.busy_servers += 1
        next_id, _ = _dequeue_oldest(world, e.zone_id)
        if next_id !== nothing
            service_time = cfg.service_dist(rng)
            schedule!(fel, ProcessComplete(next_id, e.zone_id, t + service_time),
                      t + service_time)
        end
    end
end

# ─────────────────────────────────────────────────────────────────────────────
# TransferOut — entity moves to downstream zone
# ─────────────────────────────────────────────────────────────────────────────

"""
    dispatch!(world, fel, configs, rng, e::TransferOut, t)

Entity leaves one zone and arrives at a downstream zone after transit time.
The transit time is the `lookahead` of the *source* zone (minimum delay).
"""
function dispatch!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig}, rng::AbstractRNG,
                   e::TransferOut, t::Float64)
    dest_cfg     = get(configs, e.dest_zone, nothing)
    dest_cfg === nothing && return   # unknown destination — drop silently

    transit_time = configs[e.dest_zone].lookahead
    arrival_t    = t + max(transit_time, 0.0)
    schedule!(fel, EntityArrival(e.entity_id, e.dest_zone, arrival_t), arrival_t)
end

# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

"""
    _warmup_arrivals(cfg) -> Int

Number of arrivals to discard before collecting statistics.
Rule of thumb: 20× the expected steady-state queue length (min 100).
"""
function _warmup_arrivals(cfg::ZoneConfig)
    ρ = cfg.arrival_rate / (cfg.num_servers * _mean_service_rate(cfg))
    ρ = min(ρ, 0.99)   # cap at 0.99 for near-saturated systems
    expected_L = ρ / (1 - ρ)
    return max(100, round(Int, 20 * expected_L))
end

"""
    _mean_service_rate(cfg) -> Float64

Extract the mean service rate from the zone config's service distribution.
Used for warmup estimation.
"""
function _mean_service_rate(cfg::ZoneConfig)
    d = cfg.service_dist.dist
    return 1.0 / mean(d)
end

"""
    _dequeue_oldest(world, zone_id) -> (UInt64, DESAgent) or (nothing, nothing)

Find and return the entity in `zone_id` with the earliest `arrival_time`
(FIFO discipline). Returns `(nothing, nothing)` if no entity found.
"""
function _dequeue_oldest(world::SimWorld, zone_id::Int)
    oldest_id   = nothing
    oldest_time = Inf
    oldest_agent = nothing
    for (id, agent) in world.des_agents
        if agent.current_zone == zone_id && agent.arrival_time < oldest_time
            oldest_id    = id
            oldest_time  = agent.arrival_time
            oldest_agent = agent
        end
    end
    return oldest_id, oldest_agent
end
