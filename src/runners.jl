"""
    runners.jl — Convenience simulation runners

High-level functions that wire up a complete simulation from scratch:
build world → configure zones → seed first arrival → run loop → return stats.

Each runner corresponds to a specific queueing model used in validation:
- `run_mm1!`  → M/M/1 (DES-S-01, DES-S-02, DES-S-03)
- `run_mmc!`  → M/M/c (DES-S-04)
- `run_mm1k!` → M/M/1/K finite buffer (DES-S-05)
- `run_mg1!`  → M/G/1 general service (DES-S-07)
- `run_md1!`  → M/D/1 deterministic service (DES-S-06)

All runners use `SimClock(Inf)` (fastest mode) for validation runs.
"""

"""
    run_mm1!(λ, μ; n_arrivals, t_end, seed) -> SimStats

Run an M/M/1 queueing simulation.

# Theory (for validation)
```
ρ = λ/μ
L  = ρ/(1-ρ)          (mean number in system)
Wq = ρ/(μ(1-ρ))       (mean wait in queue)
W  = 1/(μ(1-ρ))       (mean sojourn time)
```

# Arguments
- `λ::Float64`: arrival rate (entities per time unit)
- `μ::Float64`: service rate (entities per time unit)
- `n_arrivals::Int`: stop after this many arrivals (default 200_000)
- `t_end::Float64`: stop at this simulated time (default Inf)
- `seed::Int`: RNG seed for reproducibility
"""
function run_mm1!(λ::Float64, μ::Float64;
                  n_arrivals::Int = 200_000,
                  t_end::Float64  = Inf,
                  seed::Int       = 42) :: SimStats
    rng   = MersenneTwister(seed)
    world = SimWorld()
    fel   = FutureEventList()
    cfg   = ZoneConfig(id=1,
                       num_servers  = 1,
                       capacity     = typemax(Int),
                       service_dist = exponential_service(μ),
                       arrival_rate = λ)
    configs = Dict(1 => cfg)
    build_world!(world, cfg)
    clock = SimClock(Inf)
    world.stats.warmup_complete = true   # skip warmup for validation runners

    # Seed first arrival
    t_first = rand(rng, Exponential(1.0/λ))
    schedule!(fel, EntityArrival(new_entity_id!(world), 1, t_first), t_first)

    # Run until n_arrivals OR t_end
    effective_t_end = isinf(t_end) ? float(n_arrivals) / max(λ, 1e-6) * 1.1 : t_end
    return sim_loop!(world, fel, configs, clock, rng; t_end=effective_t_end)
end

"""
    run_mmc!(λ, μ, c; kwargs...) -> SimStats

Run an M/M/c queueing simulation (c parallel servers).

# Theory (Erlang-C formula)
```
ρ = λ/(c·μ)
C(c,a) = Erlang C formula  (probability of waiting)
Wq = C(c,a) / (c·μ - λ)
```

# Arguments
- `c::Int`: number of parallel servers
"""
function run_mmc!(λ::Float64, μ::Float64, c::Int;
                  n_arrivals::Int = 200_000,
                  t_end::Float64  = Inf,
                  seed::Int       = 42) :: SimStats
    rng   = MersenneTwister(seed)
    world = SimWorld()
    fel   = FutureEventList()
    cfg   = ZoneConfig(id=1,
                       num_servers  = c,
                       capacity     = typemax(Int),
                       service_dist = exponential_service(μ),
                       arrival_rate = λ)
    configs = Dict(1 => cfg)
    build_world!(world, cfg)
    clock = SimClock(Inf)
    world.stats.warmup_complete = true   # skip warmup for validation runners

    t_first = rand(rng, Exponential(1.0/λ))
    schedule!(fel, EntityArrival(new_entity_id!(world), 1, t_first), t_first)


    effective_t_end = isinf(t_end) ? float(n_arrivals) / max(λ, 1e-6) * 1.1 : t_end
    return sim_loop!(world, fel, configs, clock, rng; t_end=effective_t_end)
end

"""
    run_mm1k!(λ, μ, K; kwargs...) -> SimStats

Run an M/M/1/K simulation — M/M/1 with finite buffer of capacity K.

# Theory (Erlang-B blocking probability)
```
πK = (1-ρ)·ρ^K / (1-ρ^(K+1))   for ρ ≠ 1
πK = 1/(K+1)                     for ρ = 1
```

# Arguments
- `K::Int`: total system capacity (queue + in-service)
"""
function run_mm1k!(λ::Float64, μ::Float64, K::Int;
                   n_arrivals::Int = 500_000,
                   t_end::Float64  = Inf,
                   seed::Int       = 42) :: SimStats
    rng   = MersenneTwister(seed)
    world = SimWorld()
    fel   = FutureEventList()
    cfg   = ZoneConfig(id=1,
                       num_servers  = 1,
                       capacity     = K,
                       service_dist = exponential_service(μ),
                       arrival_rate = λ)
    configs = Dict(1 => cfg)
    build_world!(world, cfg)
    clock = SimClock(Inf)
    world.stats.warmup_complete = true   # skip warmup for validation runners

    t_first = rand(rng, Exponential(1.0/λ))
    schedule!(fel, EntityArrival(new_entity_id!(world), 1, t_first), t_first)

    effective_t_end = isinf(t_end) ? float(n_arrivals) / max(λ, 1e-6) * 1.1 : t_end
    return sim_loop!(world, fel, configs, clock, rng; t_end=effective_t_end)
end

"""
    run_mg1!(λ, μ, k; kwargs...) -> SimStats

Run an M/G/1 simulation with Erlang-k service distribution.

# Theory (Pollaczek-Khinchine formula)
```
ρ = λ/μ
E[S²] = (k+1)/(k·μ²)   (second moment of Erlang-k)
Wq = λ·E[S²] / (2(1-ρ))
```

# Arguments
- `k::Int`: Erlang shape parameter (k=1 → M/M/1, k→∞ → M/D/1)
"""

function run_mg1!(λ::Float64, μ::Float64, k::Int;
                  n_arrivals::Int = 200_000,
                  t_end::Float64  = Inf,
                  seed::Int       = 42) :: SimStats
    rng   = MersenneTwister(seed)
    world = SimWorld()
    fel   = FutureEventList()
    cfg   = ZoneConfig(id=1,
                       num_servers  = 1,
                       capacity     = typemax(Int),
                       service_dist = erlang_service(k, μ),
                       arrival_rate = λ)
    configs = Dict(1 => cfg)
    build_world!(world, cfg)
    clock = SimClock(Inf)
    world.stats.warmup_complete = true   # skip warmup for validation runners

    t_first = rand(rng, Exponential(1.0/λ))
    schedule!(fel, EntityArrival(new_entity_id!(world), 1, t_first), t_first)

    effective_t_end = isinf(t_end) ? float(n_arrivals) / max(λ, 1e-6) * 1.1 : t_end
    return sim_loop!(world, fel, configs, clock, rng; t_end=effective_t_end)
end

"""
    run_md1!(λ, d; kwargs...) -> SimStats

Run an M/D/1 simulation with deterministic service time `d`.

# Theory (P-K formula for deterministic service)
```
ρ = λ·d
E[S²] = d²   (deterministic → zero variance)
Wq = λ·d² / (2(1-ρ)) = ρ·d / (2(1-ρ))
```
"""

function run_md1!(λ::Float64, d::Float64;
                  n_arrivals::Int = 200_000,
                  t_end::Float64  = Inf,
                  seed::Int       = 42) :: SimStats
    rng   = MersenneTwister(seed)
    world = SimWorld()
    fel   = FutureEventList()
    cfg   = ZoneConfig(id=1,
                       num_servers  = 1,
                       capacity     = typemax(Int),
                       service_dist = deterministic_service(d),
                       arrival_rate = λ)
    configs = Dict(1 => cfg)
    build_world!(world, cfg)
    clock = SimClock(Inf)
    world.stats.warmup_complete = true   # skip warmup for validation runners

    t_first = rand(rng, Exponential(1.0/λ))
    schedule!(fel, EntityArrival(new_entity_id!(world), 1, t_first), t_first)

    effective_t_end = isinf(t_end) ? float(n_arrivals) / max(λ, 1e-6) * 1.1 : t_end
    return sim_loop!(world, fel, configs, clock, rng; t_end=effective_t_end)

end
