"""
    zone.jl — ZoneConfig: static zone parameters + routing policies + NHPP schedule

`ZoneConfig` holds the *immutable* configuration of a simulation zone (DES logical process).
It is read-only during a simulation run; all mutable state lives in `SimWorld.zone_states`.

Design ref: §7.5–7.6 (Zone as LP agent), §7.12 (Routing), DES-M-01..07 validation specs
"""

# ── Service distribution wrapper ───────────────────────────────────────────────

"""
    ServiceDist

A callable service-time distribution. `rand(rng, dist)` returns one
service-time sample in simulated seconds.

Wraps any `Distributions.jl` `ContinuousUnivariateDistribution`.

# Common configurations
```julia
ServiceDist(Exponential(1/μ))    # M/M/1 — memoryless service, mean = 1/μ
ServiceDist(Dirac(d))            # M/D/1 — deterministic service time d
ServiceDist(Erlang(k, 1/(k*μ))) # M/Ek/1 — Erlang-k, mean ≈ 1/μ
```
"""
struct ServiceDist{D <: UnivariateDistribution}
    dist :: D
end

"""
    (sd::ServiceDist)(rng) -> Float64

Sample one service time from the distribution.
"""
(sd::ServiceDist)(rng::AbstractRNG) = rand(rng, sd.dist)

# Convenience constructors
ServiceDist(μ::Float64)                        = ServiceDist(Exponential(1.0/μ))
exponential_service(μ::Float64)                = ServiceDist(Exponential(1.0/μ))
deterministic_service(d::Float64)              = ServiceDist(Dirac(d))
erlang_service(k::Int, μ::Float64)             = ServiceDist(Erlang(k, 1.0/(k*μ)))

# ── Routing policies ───────────────────────────────────────────────────────────

"""
    RoutingPolicy

Abstract supertype for entity routing policies applied after service completion.

Subtype and dispatch enable zero-overhead routing without if/elseif chains.
"""
abstract type RoutingPolicy end

"""
    ExitSystem()

Default routing: entity exits the simulation system after service.
Statistics are recorded and the entity is removed from the world.
"""
struct ExitSystem <: RoutingPolicy end

"""
    FixedRoute(to::Int)

Route entity to a fixed downstream zone after service.
Used for tandem queues and series networks (DES-M-01).

# Example
```julia
# Node 1 routes all departures to zone 2
cfg = ZoneConfig(id=1, ..., routing=FixedRoute(2))
```
"""
struct FixedRoute <: RoutingPolicy
    to :: Int
end

"""
    ProbRoute(choices)

Probabilistic routing: entity is routed to one of several downstream zones
(or exits the system) with given probabilities. Implements Jackson network
routing matrices (DES-M-02).

# Arguments
- `choices`: `Vector{Tuple{Union{Int,Nothing}, Float64}}` — each `(dest, prob)` pair.
  `dest = nothing` means the entity exits the system. Probabilities must sum ≤ 1.0;
  any remaining probability mass routes to exit.

# Example
```julia
# 30% → zone 2, 40% → zone 3, 30% exit
cfg = ZoneConfig(id=1, ...,
    routing = ProbRoute([(2, 0.30), (3, 0.40)]))
```
"""
struct ProbRoute <: RoutingPolicy
    choices :: Vector{Tuple{Union{Int,Nothing}, Float64}}
end

"""
    sample_destination(route::ProbRoute, rng) -> Union{Int, Nothing}

Sample a destination from a `ProbRoute`. Returns a zone ID or `nothing` (= exit).
"""
function sample_destination(route::ProbRoute, rng::AbstractRNG)
    u = rand(rng)
    cum = 0.0
    for (dest, prob) in route.choices
        cum += prob
        u < cum && return dest
    end
    return nothing   # residual probability → exit
end

# ── Non-Homogeneous Poisson Process schedule ───────────────────────────────────

"""
    ArrivalRateSchedule(breakpoints, rates)

A piecewise-constant arrival rate schedule for Non-Homogeneous Poisson Process (NHPP)
simulation via the thinning algorithm (Lewis & Shedler 1979).

# Fields
- `breakpoints::Vector{Float64}`: time breakpoints of length `n+1`, defining `n` intervals.
  The first breakpoint is the schedule start time (usually 0.0), the last is the end.
- `rates::Vector{Float64}`: arrival rate λ for each interval; length must equal `n`.
- `λ_max::Float64`: `maximum(rates)` — used as the dominating Poisson process for thinning.

# Example (24-hour call centre pattern)
```julia
sched = ArrivalRateSchedule(
    [0.0, 6.0, 8.0, 12.0, 14.0, 18.0, 22.0, 24.0],  # breakpoints (hours)
    [0.5,  2.0,  5.0,  3.0,   5.0,   2.0,   0.5])     # rates per hour
)
```
"""
struct ArrivalRateSchedule
    breakpoints :: Vector{Float64}
    rates       :: Vector{Float64}
    λ_max       :: Float64

    function ArrivalRateSchedule(bp::Vector{Float64}, rates::Vector{Float64})
        length(bp) == length(rates) + 1 ||
            throw(ArgumentError("length(breakpoints) must equal length(rates)+1"))
        all(r >= 0.0 for r in rates) ||
            throw(ArgumentError("all rates must be non-negative"))
        new(bp, rates, maximum(rates; init=0.0))
    end
end

"""
    rate_at(sched, t) -> Float64

Return the piecewise-constant arrival rate at simulated time `t`.
Returns 0.0 if `t` is beyond the schedule end.
"""
function rate_at(sched::ArrivalRateSchedule, t::Float64)
    for i in 1:length(sched.rates)
        if t < sched.breakpoints[i+1]
            return sched.rates[i]
        end
    end
    return 0.0
end

"""
    next_nhpp_arrival(sched, t_now, rng) -> Float64

Sample the next NHPP arrival time after `t_now` using the thinning algorithm.
Returns `Inf` if the schedule has ended and no more arrivals will occur.

# Algorithm (Lewis & Shedler 1979)
1. Draw candidate inter-arrival time from dominating homogeneous Poisson(λ_max)
2. Accept with probability λ(t_candidate) / λ_max; reject and repeat if not accepted
"""
function next_nhpp_arrival(sched::ArrivalRateSchedule, t_now::Float64,
                            rng::AbstractRNG)::Float64
    sched.λ_max <= 0.0 && return Inf
    t = t_now
    max_t = sched.breakpoints[end]
    while true
        Δt = rand(rng, Exponential(1.0 / sched.λ_max))
        t += Δt
        t >= max_t && return Inf   # schedule exhausted
        λ_t = rate_at(sched, t)
        # Accept with probability λ(t)/λ_max (thinning)
        rand(rng) < λ_t / sched.λ_max && return t
    end
end

# ── ForkJoin configuration ─────────────────────────────────────────────────────

"""
    ForkJoinConfig(sub_zones, final_zone)

Configuration for a fork-join station (DES-M-07).

When an entity arrives at a fork zone, it spawns `length(sub_zones)` parallel
sub-entities, one at each sub-zone. The entity's sojourn completes when ALL
sub-tasks finish. Validated by Baccelli-Makowski bound:
  `E[join_time] ≥ max(E[S₁], E[S₂], ..., E[Sₙ])`

# Fields
- `sub_zones::Vector{Int}`: IDs of parallel service zones
- `final_zone::Int`: zone to route the entity after join (0 = exit)
"""
struct ForkJoinConfig
    sub_zones  :: Vector{Int}
    final_zone :: Int
end

# ── ZoneConfig ─────────────────────────────────────────────────────────────────

"""
    ZoneConfig

Immutable configuration for a simulation zone (queue + server station).
Pass to `build_world!` before starting a simulation.

# Fields
- `id::Int`: zone identifier (must match a zone registered in `SimWorld`)
- `num_servers::Int`: number of parallel servers (1 → M/M/1, c → M/M/c)
- `capacity::Int`: max entities in system (queue + service); `typemax(Int)` = unlimited
- `service_dist::ServiceDist`: service time distribution
- `arrival_rate::Float64`: Poisson arrival rate λ [entities per time unit]; `0.0` if external
- `lookahead::Float64`: min inter-event transit time to downstream zones (Tier 2 use)
- `downstream::Vector{Int}`: IDs of zones that receive `TransferOut` events (legacy)
- `routing::RoutingPolicy`: what happens to entities after service (`ExitSystem`, `FixedRoute`, `ProbRoute`)
- `queue_discipline::Symbol`: `:fifo` (default) or `:priority` (non-preemptive HOL priority)
- `arrival_schedule`: `nothing` for homogeneous Poisson; `ArrivalRateSchedule` for NHPP
- `failure_rate::Float64`: α — machine failure rate [failures/time unit]; `0.0` = no failures
- `repair_rate::Float64`: β — repair rate [repairs/time unit]; only used if `failure_rate > 0`
- `fork_join`: `nothing` or `ForkJoinConfig` for fork-join stations

# Examples
```julia
# M/M/1 queue: λ=2/min, μ=3/min, unlimited buffer
cfg = ZoneConfig(id=1, service_dist=exponential_service(3.0), arrival_rate=2.0)

# Tandem node 1 → node 2 routing
cfg1 = ZoneConfig(id=1, service_dist=exponential_service(2.0), arrival_rate=1.0,
                  routing=FixedRoute(2))
cfg2 = ZoneConfig(id=2, service_dist=exponential_service(3.0))

# Non-preemptive priority queue
cfg  = ZoneConfig(id=1, service_dist=exponential_service(1.0),
                  queue_discipline=:priority)

# Machine with failures (α=0.1, β=1.0, availability≈0.909)
cfg = ZoneConfig(id=1, service_dist=exponential_service(2.0),
                 arrival_rate=1.5, failure_rate=0.1, repair_rate=1.0)
```
"""
struct ZoneConfig
    id               :: Int
    num_servers      :: Int
    capacity         :: Int
    service_dist     :: ServiceDist
    arrival_rate     :: Float64
    lookahead        :: Float64
    downstream       :: Vector{Int}
    routing          :: RoutingPolicy
    queue_discipline :: Symbol
    arrival_schedule :: Union{Nothing, ArrivalRateSchedule}
    failure_rate     :: Float64
    repair_rate      :: Float64
    fork_join        :: Union{Nothing, ForkJoinConfig}
end

function ZoneConfig(;
        id::Int,
        num_servers::Int  = 1,
        capacity::Int     = typemax(Int),
        service_dist::ServiceDist,
        arrival_rate::Float64  = 0.0,
        lookahead::Float64     = 0.0,
        downstream::Vector{Int}     = Int[],
        routing::RoutingPolicy      = ExitSystem(),
        queue_discipline::Symbol    = :fifo,
        arrival_schedule            = nothing,
        failure_rate::Float64       = 0.0,
        repair_rate::Float64        = 1.0,
        fork_join                   = nothing)

    num_servers > 0  || throw(ArgumentError("num_servers must be ≥ 1"))
    capacity > 0     || throw(ArgumentError("capacity must be ≥ 1"))
    arrival_rate >= 0.0 || throw(ArgumentError("arrival_rate must be ≥ 0"))
    failure_rate >= 0.0 || throw(ArgumentError("failure_rate must be ≥ 0"))
    repair_rate > 0.0   || throw(ArgumentError("repair_rate must be > 0"))
    queue_discipline in (:fifo, :priority) ||
        throw(ArgumentError("queue_discipline must be :fifo or :priority"))

    ZoneConfig(id, num_servers, capacity, service_dist, arrival_rate,
               lookahead, downstream, routing, queue_discipline,
               arrival_schedule, failure_rate, repair_rate, fork_join)
end

"""
    build_world!(world, configs...) -> world

Register all zones defined by `ZoneConfig` into a `SimWorld`.
Sets up `ZoneState` for each zone with the correct capacity and server count.
Also registers per-zone `SimStats` in `world.zone_stats` for multi-zone tracking.
"""
function build_world!(world::SimWorld, configs::ZoneConfig...)
    for cfg in configs
        add_zone!(world, cfg.id;
                  capacity    = cfg.capacity,
                  num_servers = cfg.num_servers)
        # Register per-zone stats for multi-zone scenarios
        world.zone_stats[cfg.id] = SimStats()
    end
    return world
end
