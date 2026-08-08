"""
    zone.jl — ZoneConfig: static zone parameters

`ZoneConfig` holds the *immutable* configuration of a simulation zone (DES logical process).
It is read-only during a simulation run; all mutable state lives in `SimWorld.zone_states`.

Design ref: §7.5–7.6 (Zone as LP agent)
"""

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
- `downstream::Vector{Int}`: IDs of zones that receive `TransferOut` events from this zone

# Examples
```julia
# M/M/1 queue: λ=2/min, μ=3/min, unlimited buffer
cfg = ZoneConfig(id=1, num_servers=1, capacity=typemax(Int),
                 service_dist=exponential_service(3.0), arrival_rate=2.0)

# M/M/4 call center: 4 servers, λ=8/min, μ=3/min
cfg = ZoneConfig(id=2, num_servers=4, capacity=typemax(Int),
                 service_dist=exponential_service(3.0), arrival_rate=8.0)

# M/M/1/5 finite buffer: capacity K=5
cfg = ZoneConfig(id=3, num_servers=1, capacity=5,
                 service_dist=exponential_service(2.0), arrival_rate=2.0)
```
"""
struct ZoneConfig
    id           :: Int
    num_servers  :: Int
    capacity     :: Int
    service_dist :: ServiceDist
    arrival_rate :: Float64
    lookahead    :: Float64
    downstream   :: Vector{Int}
end

function ZoneConfig(;
        id::Int,
        num_servers::Int  = 1,
        capacity::Int     = typemax(Int),
        service_dist::ServiceDist,
        arrival_rate::Float64 = 0.0,
        lookahead::Float64    = 0.0,
        downstream::Vector{Int} = Int[])
    num_servers > 0  || throw(ArgumentError("num_servers must be ≥ 1"))
    capacity > 0     || throw(ArgumentError("capacity must be ≥ 1"))
    arrival_rate >= 0.0 || throw(ArgumentError("arrival_rate must be ≥ 0"))
    ZoneConfig(id, num_servers, capacity, service_dist, arrival_rate,
               lookahead, downstream)
end

"""
    build_world!(world, configs...) -> world

Register all zones defined by `ZoneConfig` into a `SimWorld`.
Sets up `ZoneState` for each zone with the correct capacity and server count.
"""
function build_world!(world::SimWorld, configs::ZoneConfig...)
    for cfg in configs
        add_zone!(world, cfg.id;
                  capacity    = cfg.capacity,
                  num_servers = cfg.num_servers)
    end
    return world
end
