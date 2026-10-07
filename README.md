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

## 6. Early-Stage Development Advisory (v0.1.0) & Scope

`SimDES.jl` is released at **`v0.1.0`**.
* **Maturity**: Core queueing primitives, event dispatching, and conveyor kinematics are mathematically validated against closed-form formulas in the test suite.
* **Evolution**: High-level network configuration structs and builder APIs may change as community feedback is gathered.
* **Limitations**: The discrete-event queue runs single-threaded on the CPU; parallel execution is currently supported across independent replications (`replicate_parallel` in `SimCore`).

---

## 7. AI Pair-Programming & Human Oversight Disclosure

`SimDES.jl` was authored through human-directed pair programming using Google DeepMind's Antigravity assistant. Every dispatch rule, queueing mechanic, and state machine has been verified by the human maintainers against canonical theoretical formulas from queueing and material flow literature.

---

## 8. License

`SimDES.jl` is licensed under the [GNU Affero General Public License v3.0 (AGPLv3)](LICENSE).
