"""
    loop.jl — Main simulation event loop

`sim_loop!` is the heart of the DES engine. It:
1. Dequeues the minimum-time event from the FEL
2. Calls `throttle!` to honour SimClock speed setting
3. Dispatches to the correct handler via multiple dispatch
4. Repeats until `t_end` or the FEL is empty

Design ref: §7.5 (Option A — serial loop), §7.8 (SimClock integration)
"""

"""
    sim_loop!(world, fel, configs, clock, rng; t_end) -> SimStats

Run the DES engine until simulated time `t_end` or the FEL empties.

Returns `world.stats` (a `SimStats` with all collected metrics).

# Arguments
- `world::SimWorld`: mutable simulation state
- `fel::FutureEventList`: future event list (pre-seeded with first arrivals)
- `configs::Dict{Int,ZoneConfig}`: zone configurations
- `clock::SimClock`: speed controller
- `rng::AbstractRNG`: seeded random number generator
- `t_end::Float64`: simulation end time (simulated seconds)

# Example (M/M/1 run)
```julia
world   = SimWorld()
fel     = FutureEventList()
cfg     = ZoneConfig(id=1, service_dist=exponential_service(3.0), arrival_rate=2.0)
configs = Dict(1 => cfg)
build_world!(world, cfg)
clock   = SimClock(Inf)
rng     = Random.MersenneTwister(42)

# Seed first arrival
schedule!(fel, EntityArrival(new_entity_id!(world), 1, rand(rng, Exponential(0.5))), 0.0)

stats = sim_loop!(world, fel, configs, clock, rng; t_end=10_000.0)
sm    = sim_summary(stats)
println("L=\$(sm.L)  Wq=\$(sm.Wq)  util=\$(sm.utilization)")
```
"""
function sim_loop!(world::SimWorld, fel::FutureEventList,
                   configs::Dict{Int,ZoneConfig},
                   clock::SimClock, rng::AbstractRNG;
                   t_end::Float64 = Inf)
    while true
        result = safe_dequeue!(fel)
        result === nothing && break   # FEL exhausted

        cev, t = result
        t > t_end && break

        throttle!(clock, t)          # honour speed setting (pause / real-time / fastest)
        world.time = t

        dispatch!(world, fel, configs, rng, cev.inner, t)
    end

    return world.stats
end
