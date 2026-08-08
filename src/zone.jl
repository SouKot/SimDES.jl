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

# ── Arrival process abstraction ───────────────────────────────────────────────

"""
    ArrivalProcess

Abstract type for entity arrival processes at a zone.

| Subtype | Meaning |
|---------|--------|
| `NoArrival` | No external arrivals — pure sink or downstream-only node |
| `PoissonArrival(rate)` | Homogeneous Poisson process with constant rate λ |
| `NHPPArrival(schedule)` | Non-Homogeneous Poisson Process (thinning algorithm) |

# Backward compatibility
The `ZoneConfig` keyword constructor still accepts `arrival_rate::Float64` and
`arrival_schedule` kwargs and converts them automatically:
- `arrival_schedule=sched` → `NHPPArrival(sched)` (takes priority)
- `arrival_rate=λ > 0` → `PoissonArrival(λ)`
- neither → `NoArrival()`
"""
abstract type ArrivalProcess end

""" No external arrivals — zone is fed only by routed entities from upstream. """
struct NoArrival <: ArrivalProcess end

"""
    PoissonArrival(rate)

Homogeneous Poisson arrival process with constant rate λ [entities/time unit].
"""
struct PoissonArrival <: ArrivalProcess
    rate :: Float64
end

"""
    NHPPArrival(schedule)

Non-Homogeneous Poisson Process arrivals driven by an `ArrivalRateSchedule`.
Implemented via the thinning algorithm (Lewis & Shedler 1979).
"""
struct NHPPArrival <: ArrivalProcess
    schedule :: ArrivalRateSchedule
end

"""Construct an `ArrivalProcess` from legacy `arrival_rate`/`arrival_schedule` kwargs."""
function _arrival_from_kwargs(arrival_rate::Float64, arrival_schedule)
    arrival_schedule !== nothing && return NHPPArrival(arrival_schedule)   # NHPP takes priority
    arrival_rate > 0.0           && return PoissonArrival(arrival_rate)
    return NoArrival()
end

# ── Failure model abstraction ──────────────────────────────────────────────────

"""
    FailureModel

Abstract type for machine failure and repair models.

| Subtype | Meaning |
|---------|--------|
| `NoFailure` | Machine never fails (default) |
| `BernoulliFailure(α, β)` | Exponential TTF (rate α) + exponential repair (rate β); A = β/(α+β) |

# Backward compatibility
The `ZoneConfig` keyword constructor accepts `failure_rate` and `repair_rate` kwargs:
- `failure_rate=α > 0, repair_rate=β` → `BernoulliFailure(α, β)`
- `failure_rate=0.0` (default) → `NoFailure()`
"""
abstract type FailureModel end

""" Machine never fails. Default for all zones. """
struct NoFailure <: FailureModel end

"""
    BernoulliFailure(α, β)

Machine failure model:
- Time-to-failure ~ Exponential(1/α); mean time between failures = 1/α
- Repair time ~ Exponential(1/β); mean repair time = 1/β
- Steady-state availability A = β/(α+β)
"""
struct BernoulliFailure <: FailureModel
    α :: Float64   # failure rate  [failures per time unit]
    β :: Float64   # repair rate   [repairs per time unit]
end

"""Construct a `FailureModel` from legacy `failure_rate`/`repair_rate` kwargs."""
_failure_from_kwargs(failure_rate::Float64, repair_rate::Float64) =
    failure_rate > 0.0 ? BernoulliFailure(failure_rate, repair_rate) : NoFailure()

# ── Queue discipline enum ──────────────────────────────────────────────────────

"""
    QueueDiscipline

Type-safe enum selecting how entities wait in a zone's queue.
Zero runtime cost — the integer representation is optimised away by the compiler.

| Value | Meaning |
|-------|--------|
| `FIFO` | First-in-first-out (default) |
| `PRIORITY_HOL` | Non-preemptive Head-Of-Line priority (higher `priority` field served first) |

# Backward compatibility
The `ZoneConfig` keyword constructor also accepts Symbols `:fifo` and `:priority`
and converts them transparently, so existing code continues to work.
"""
@enum QueueDiscipline begin
    FIFO         = 1
    PRIORITY_HOL = 2
end

"""Convert a Symbol queue discipline name to the `QueueDiscipline` enum."""
function _discipline_from_symbol(s::Symbol)
    s === :fifo     && return FIFO
    s === :priority && return PRIORITY_HOL
    throw(ArgumentError("queue_discipline :$s unknown; use :fifo or :priority (or QueueDiscipline enum)"))
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
- `arrival::ArrivalProcess`: `NoArrival()`, `PoissonArrival(λ)`, or `NHPPArrival(sched)`
- `lookahead::Float64`: min inter-event transit time to downstream zones (Tier 2 use)
- `downstream::Vector{Int}`: IDs of zones that receive `TransferOut` events (Tier 2 only)
- `routing::RoutingPolicy`: entity routing after service (`ExitSystem`, `FixedRoute`, `ProbRoute`)
- `queue_discipline::QueueDiscipline`: `FIFO` (default) or `PRIORITY_HOL`
- `failures::FailureModel`: `NoFailure()` or `BernoulliFailure(α, β)`
- `fork_join`: `nothing` or `ForkJoinConfig` for fork-join stations

# Examples
```julia
# M/M/1 queue: λ=2/min, μ=3/min, unlimited buffer (legacy arrival_rate kwarg)
cfg = ZoneConfig(id=1, service_dist=exponential_service(3.0), arrival_rate=2.0)

# Preferred: use typed ArrivalProcess directly
cfg = ZoneConfig(id=1, service_dist=exponential_service(3.0),
                 arrival=PoissonArrival(2.0))

# Tandem node 1 → node 2 routing
cfg1 = ZoneConfig(id=1, service_dist=exponential_service(2.0),
                  arrival=PoissonArrival(1.0), routing=FixedRoute(2))
cfg2 = ZoneConfig(id=2, service_dist=exponential_service(3.0))

# Non-preemptive priority queue
cfg  = ZoneConfig(id=1, service_dist=exponential_service(1.0),
                  queue_discipline=PRIORITY_HOL)

# Machine with failures (α=0.1, β=1.0, availability≈0.909)
# Legacy kwargs still work:
cfg = ZoneConfig(id=1, service_dist=exponential_service(2.0),
                 arrival_rate=1.5, failure_rate=0.1, repair_rate=1.0)
# Preferred typed form:
cfg = ZoneConfig(id=1, service_dist=exponential_service(2.0),
                 arrival=PoissonArrival(1.5),
                 failures=BernoulliFailure(0.1, 1.0))
```
"""
struct ZoneConfig
    id               :: Int
    num_servers      :: Int
    capacity         :: Int
    service_dist     :: ServiceDist
    arrival          :: ArrivalProcess        # replaces arrival_rate + arrival_schedule
    lookahead        :: Float64
    downstream       :: Vector{Int}
    routing          :: RoutingPolicy
    queue_discipline :: QueueDiscipline
    failures         :: FailureModel          # replaces failure_rate + repair_rate
    fork_join        :: Union{Nothing, ForkJoinConfig}
end

function ZoneConfig(;
        id::Int,
        num_servers::Int  = 1,
        capacity::Int     = typemax(Int),
        service_dist::ServiceDist,
        # ── Legacy backward-compat kwargs (auto-converted to typed args) ──
        arrival_rate::Float64  = 0.0,
        arrival_schedule       = nothing,
        failure_rate::Float64  = 0.0,
        repair_rate::Float64   = 1.0,
        # ── Preferred typed kwargs ─────────────────────────────────────────
        arrival::ArrivalProcess = _arrival_from_kwargs(arrival_rate, arrival_schedule),
        failures::FailureModel  = _failure_from_kwargs(failure_rate, repair_rate),
        # ── Common kwargs ─────────────────────────────────────────────────
        lookahead::Float64      = 0.0,
        downstream::Vector{Int} = Int[],
        routing::RoutingPolicy  = ExitSystem(),
        queue_discipline        = FIFO,      # accepts QueueDiscipline enum or Symbol
        fork_join               = nothing)

    num_servers > 0     || throw(ArgumentError("num_servers must be ≥ 1"))
    capacity > 0        || throw(ArgumentError("capacity must be ≥ 1"))
    arrival_rate >= 0.0 || throw(ArgumentError("arrival_rate must be ≥ 0"))
    failure_rate >= 0.0 || throw(ArgumentError("failure_rate must be ≥ 0"))
    repair_rate > 0.0   || throw(ArgumentError("repair_rate must be > 0"))

    disc = queue_discipline isa Symbol ?
               _discipline_from_symbol(queue_discipline) :
               queue_discipline

    ZoneConfig(id, num_servers, capacity, service_dist, arrival,
               lookahead, downstream, routing, disc, failures, fork_join)
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
