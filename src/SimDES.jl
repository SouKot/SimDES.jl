"""
    SimDES

Serial Discrete Event Simulation engine for Hermes.jl (Tier 1 — single-threaded).

Provides:
- `FutureEventList` — typed wrapper around DataStructures.PriorityQueue with FEL-local cancel set
- `ZoneConfig`      — static zone parameters (servers, capacity, service dist, routing)
- `sim_loop!`       — main event loop with SimClock throttle and optional auto-warmup
- `dispatch!`       — multiple-dispatch event handler (extensible)
- DES primitive elements: M/M/1, M/M/c, M/M/1/K, M/G/1, M/D/1
- Phase 2C: routing, priority queuing, NHPP, machine failures, fork-join
- Sprint 2D: ArrivalProcess hierarchy, FailureModel hierarchy, FEL-local cancellation
- Machine failure / repair event handlers
- Welch-method warmup detection

Design refs: §7.1–7.5 (FEL, events), §7.8 (SimClock), §7.11–7.12 (dispatch)
Depends on: SimCore
"""
module SimDES

using SimCore
import SimCore: cancel!   # explicit import so fel.jl extends SimCore.cancel!
                          # (not a new SimDES-local binding, which would cause ambiguity)
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
# cancel!(fel, id::UInt64) — FEL-local, no lock; extended onto SimCore.cancel! via
# 'import SimCore: cancel!' above. Same function object → no dual-export ambiguity.
export FutureEventList, schedule!, safe_dequeue!, peek_time, cancel!

# Zone configuration
export ZoneConfig, ServiceDist, build_world!
export exponential_service, deterministic_service, erlang_service

# Phase 2C: routing policies
export RoutingPolicy, ExitSystem, FixedRoute, ProbRoute
export sample_destination

# Phase 2C: NHPP
export ArrivalRateSchedule, rate_at, next_nhpp_arrival

# Phase 2C: fork-join
export ForkJoinConfig

# Queue discipline (type-safe enum; Symbol :fifo/:priority also accepted for backward compat)
export QueueDiscipline, FIFO, PRIORITY_HOL

# Sprint 2D: ArrivalProcess hierarchy (replaces flat arrival_rate/arrival_schedule fields)
export ArrivalProcess, NoArrival, PoissonArrival, NHPPArrival

# Sprint 2D: FailureModel hierarchy (replaces flat failure_rate/repair_rate fields)
export FailureModel, NoFailure, BernoulliFailure

# Dispatch (generic — users can extend with their own methods)
export dispatch!

# Warmup detector
export WelchDetector, update!, warmup_complete

# Main loop
export sim_loop!

# Statistics summary
export sim_summary

# Convenience runners — Phase 2A+2B
export run_mm1!, run_mmc!, run_mm1k!, run_mg1!, run_md1!

# Convenience runners — Phase 2C
export run_tandem!, run_jackson!, run_priority!
export run_with_failures!, run_nhpp!, run_forkjoin!

end
