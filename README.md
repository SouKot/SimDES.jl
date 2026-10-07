# SimDES.jl

[![CI](https://github.com/SouKot/SimDES.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/SouKot/SimDES.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![Documentation](https://img.shields.io/badge/docs-stable-blue.svg)](https://SouKot.github.io/SimDES.jl/stable/)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPLv3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Version](https://img.shields.io/badge/version-0.1.0-green.svg)](https://github.com/SouKot/SimDES.jl/releases)

**A discrete-event simulation engine in Julia for queueing networks, material handling, and manufacturing systems, rigorously verified against closed-form analytical queueing theory.**

---

## 1. Problem Statement & Architecture

`SimDES.jl` provides an extensible, high-performance discrete-event simulation (DES) framework built on the zero-allocation simulation primitives of `SimCore.jl`. It is engineered for industrial engineering, manufacturing logistics, and queueing network analysis.

Key architectural characteristics:
* **Binary Heap Event Scheduler**: $O(\log N)$ event insertion and extraction with $O(1)$ lazy event cancellation.
* **Closed-Form Analytical Grounding**: Every dispatch mechanism and queue discipline is tested against canonical queueing theory equations.
* **Lossless Material Handling**: Physical accumulation kinematics (Zero-Pressure Accumulation conveyors, blocking, slug formation, and merge priority).
* **Zero Heavy Dependencies**: Pure Julia core with zero graphics overhead. Visualizations are provided via an optional package extension (`SimDESMakieExt`).

---

## 2. Queueing Theory Formulations & Validation Suite

The core simulation engine in `SimDES.jl` is systematically benchmarked against classical queueing theory models. The table below outlines the analytical equations and empirical test tolerances verified in [`test/runtests.jl`](test/runtests.jl):

| Model / Phenomenon | Analytical Formulation | Empirical Test Metric | Benchmark Tolerance |
| :--- | :--- | :--- | :--- |
| **M/M/1 System** | $L = \frac{\rho}{1 - \rho}$, $\quad W = \frac{1}{\mu(1 - \rho)}$, $\quad W_q = \frac{\rho}{\mu(1 - \rho)}$ | Mean items in system $L$, mean wait $W_q$ | $\pm 15\%$ ($\rho = 0.50$)<br>$\pm 25\%$ ($\rho = 0.90$) |
| **M/M/c Multi-Server** | $C(c, a) = \frac{\frac{a^c}{c!(1 - \rho)}}{\sum_{k=0}^{c-1} \frac{a^k}{k!} + \frac{a^c}{c!(1 - \rho)}}$, $\quad W_q = \frac{C(c, a)}{c\mu - \lambda}$ | Multi-server queue delay vs. load | Monotone reduction vs. $M/M/1$ |
| **M/M/1/K Finite Buffer** | $P_b = \frac{(1 - \rho)\rho^K}{1 - \rho^{K+1}}$ | Buffer loss / blocking probability $P_b$ | $\pm 3\%$ ($K=5, \rho=1.0$) |
| **M/D/1 Deterministic** | $W_q = \frac{\lambda d^2}{2(1 - \lambda d)}$ (Pollaczek-Khinchine) | Deterministic service wait time $W_q$ | $\pm 15\%$ ($\rho = 0.80$) |
| **M/G/1 Erlang-2** | $W_q = \frac{\lambda E[S^2]}{2(1 - \rho)}, \quad E[S^2] = \frac{k+1}{k\mu^2}$ | General service queue wait time $W_q$ | $\pm 15\%$ |
| **Little's Law Invariant** | $L = \lambda_{\text{eff}} \cdot W$ | Invariant hold across all topologies | $\le 10\%$ discrepancy |
| **NHPP Arrival Process** | Lewis-Shedler thinning algorithm | Integrated rate $\int_0^T \lambda(t) dt$ | $\pm 15\%$ of expected arrivals |
| **Fork-Join Sync** | $E[T_{\text{join}}] \ge \max_{i} E[S_i]$ (Baccelli-Makowski bound) | Concurrency barrier delay | Strict mathematical lower bound |
| **Jackson 4-Node Network**| $\pi(n_1, \dots, n_k) = \prod_{i=1}^k \pi_i(n_i)$ | Stationary marginal utilizations $\rho_i$ | $\pm 20\%$ |

---

## 3. Physical Conveyor Kinematics

In industrial material handling, items cannot overlap, and conveyors possess physical length and accumulation behavior. `SimDES.jl` models:
* **Zero-Pressure Accumulation (ZPA)**: When a downstream station is blocked, items halt at fixed minimum separation distances without volume destruction.
* **Head-of-Line Backpressure**: Blocked upstream stations retain completed items until downstream capacity opens, preventing item drops.
* **Dynamic Re-routing**: Lossless routing across diverts, merges, and secondary zones.

---

## 4. Makie Visualization Recipes (Optional Extension)

`SimDES.jl` does not force heavy graphics dependencies. When `Makie` (or `GLMakie` / `CairoMakie`) is loaded in your environment, `SimDESMakieExt` automatically activates:

```julia
using SimDES
using CairoMakie  # Automatically loads SimDESMakieExt

# 1. Plot static network topology with station and conveyor nodes
fig = simplot(network)
save("network_schematic.png", fig)

# 2. Plot queue length trajectory over time
fig_q = plot_queue_history(zone_history)

# 3. Plot Gantt schedule of server utilization
fig_gantt = plot_gantt(server_records)
```

---

## 5. Quickstart

Here is a minimal, complete example setting up an $M/M/1$ queue:

```julia
using SimDES
using Distributions

# 1. Define simulation world with an arrival source and a single server zone
world = SimWorld()
source_id = register_zone!(world, ZoneConfig(
    name = "ArrivalSource",
    arrival_dist = Exponential(1.0 / 2.0)  # λ = 2.0 arrivals/second
))

server_id = register_zone!(world, ZoneConfig(
    name = "ServiceStation",
    capacity = 1,                          # Single server
    service_dist = Exponential(1.0 / 4.0), # μ = 4.0 services/second (ρ = 0.50)
))

connect_zones!(world, source_id, server_id)

# 2. Run simulation until t_end = 1000.0
run_sim!(world; t_end = 1000.0)

# 3. Extract verified statistics
stats = get_zone_stats(world, server_id)
println("Server Utilization: ", stats.utilization)  # Expected ≈ 0.50
println("Mean System Wait W:  ", stats.mean_sojourn) # Expected ≈ 0.50
```

---

## 6. Running Examples & Interactive Runtime Inspection

`SimDES.jl` provides 5 runnable example scripts in the [`examples/`](examples/) directory covering analytical benchmarks, accumulation kinematics, and live visualization:

| Script | Mathematical / Physical Focus |
| :--- | :--- |
| [`01_mm1_analytical_audit.jl`](examples/01_mm1_analytical_audit.jl) | $M/M/1$ queue benchmarked against closed-form theory ($L, W, W_q, \rho$). |
| [`02_jackson_open_network.jl`](examples/02_jackson_open_network.jl) | 3-station open Jackson network with feedback routing matrix ($< 1\%$ relative error). |
| [`03_conveyor_accumulation.jl`](examples/03_conveyor_accumulation.jl) | Zero-Pressure Accumulation (ZPA) physical conveyor with zero collisions under bottleneck. |
| [`04_nhpp_call_center.jl`](examples/04_nhpp_call_center.jl) | Time-varying arrival demand via Lewis–Shedler rejection thinning. |
| [`05_makie_visualization.jl`](examples/05_makie_visualization.jl) | 2D flow schematic (`simplot`), queue step-chart, server Gantt timeline, and trajectory animation. |

### Running from the Command Line
```bash
# Activate the package environment and run any example
julia --project=. examples/01_mm1_analytical_audit.jl
julia --project=. examples/05_makie_visualization.jl
```

### Running from the Julia REPL
```julia
using Pkg
Pkg.activate(".")

# Execute any example script directly in your active session
include("examples/01_mm1_analytical_audit.jl")
include("examples/05_makie_visualization.jl")
```

### Interactive Runtime Execution & Live Observation

In discrete-event simulation, observing runtime dynamics (queues growing, servers switching states, products accumulating) is essential. `SimDES.jl` supports interactive execution in three complementary ways:

#### A. Event-by-Event Stepping (`step_sim!`)
Advance the simulation by exactly one event to inspect internal state transitions:
```julia
using SimDES, SimCore, Random

world   = SimWorld()
fel     = FutureEventList()
cfg     = ZoneConfig(id=1, arrival_rate=1.5, num_servers=1)
configs = Dict(1 => cfg)
build_world!(world, cfg)
rng     = MersenneTwister(42)

# Schedule initial arrival
schedule!(fel, EntityArrival(UInt64(1), 1, 1.0), 1.0)

# Step one event at a time:
step_info = step_sim!(world, fel, configs, rng)
println("Processed $(typeof(step_info.event)) at simulated time t = $(step_info.t)")
```

#### B. Wall-Clock Throttling & Playback (`SimClock`)
Control playback speed relative to real time (e.g. 1.0 = real-time, 5.0 = 5× accelerated):
```julia
clock = SimClock(1.0)     # 1 simulated second = 1 wall-clock second
set_speed!(clock, 5.0)    # Accelerate to 5x speed
set_speed!(clock, Inf)    # Unthrottled maximum CPU throughput
pause!(clock)             # Freeze simulation advancement
```

#### C. Live Streaming Visualization with GLMakie & Observables
Connect the simulation loop to interactive Makie `Observable`s to watch queues and item flow evolve live on screen:
```julia
using GLMakie
using SimDES

# Setup live-updating step chart
t_obs = Observable(Float64[0.0])
q_obs = Observable(Int[0])

fig = Figure(size=(850, 400))
ax  = Axis(fig[1, 1], title="Live Queue Dynamics Q(t)", xlabel="Time (s)", ylabel="Queue Length")
stairs!(ax, t_obs, q_obs, step=:pre, color=:royalblue, linewidth=2)
display(fig)

# In your simulation stepping loop:
# As events fire, update the Observable:
# push!(t_obs[], world.time)
# push!(q_obs[], world.zone_states[1].queue_length)
# notify(t_obs); notify(q_obs)
# sleep(0.02)  # smooth visual playback
```

---

## 7. Early-Stage Development Advisory (v0.1.0) & Scope


`SimDES.jl` is released at **`v0.1.0`**.
* **Maturity**: Core queueing primitives, event dispatching, and conveyor kinematics are mathematically validated against closed-form formulas in the test suite.
* **Evolution**: High-level network configuration structs and builder APIs may change as community feedback is gathered.
* **Limitations**: The discrete-event queue runs single-threaded on the CPU; parallel execution is currently supported across independent replications (`replicate_parallel` in `SimCore`).

---

## 8. AI Pair-Programming & Human Oversight Disclosure

`SimDES.jl` was authored through human-directed pair programming using Google DeepMind's Antigravity assistant. Every dispatch rule, queueing mechanic, and state machine has been verified by the human maintainers against canonical theoretical formulas from queueing and material flow literature.

---

## 9. License

`SimDES.jl` is licensed under the [GNU Affero General Public License v3.0 (AGPLv3)](LICENSE).

