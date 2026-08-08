"""
    SimDES

Serial Discrete Event Simulation engine for Hermes.jl (Tier 1 — single-threaded).

Provides:
- `FutureEventList` — typed wrapper around DataStructures.PriorityQueue
- `ZoneConfig`      — static zone parameters (servers, capacity, service dist)
- `sim_loop!`       — main event loop with SimClock throttle
- `dispatch!`       — multiple-dispatch event handler (extensible)
- DES primitive elements: M/M/1, M/M/c, M/M/1/K, M/G/1, M/D/1
- Machine failure / repair event handlers
- Welch-method warmup detection

Design refs: §7.1–7.5 (FEL, events), §7.8 (SimClock), §7.11–7.12 (dispatch)
Depends on: SimCore
"""
module SimDES

using SimCore
using DataStructures: PriorityQueue, enqueue!, dequeue_pair!, peek, isempty
using Distributions: Exponential, Erlang, Dirac, UnivariateDistribution, mean
using Random: AbstractRNG, MersenneTwister, default_rng

# ── Source files ──────────────────────────────────────────────────────────────
include("fel.jl")
include("zone.jl")
include("dispatch.jl")
include("warmup.jl")
include("loop.jl")
include("runners.jl")

# ── Exports ───────────────────────────────────────────────────────────────────

# FEL
export FutureEventList, schedule!, safe_dequeue!, peek_time

# Zone configuration
export ZoneConfig, ServiceDist, build_world!
export exponential_service, deterministic_service, erlang_service

# Dispatch (generic — users can extend with their own methods)
export dispatch!

# Warmup detector
export WelchDetector, update!, warmup_complete

# Main loop
export sim_loop!

# Convenience runners
export run_mm1!, run_mmc!, run_mm1k!, run_mg1!, run_md1!

end
