# SimDES.jl

*Mathematically verified discrete-event simulation engine with analytical queueing benchmarks, conveyor kinematics, and Makie visualization recipes.*

[![CI](https://github.com/SouKot/SimDES.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/SouKot/SimDES.jl/actions/workflows/CI.yml)
[![Version](https://juliahub.com/docs/packages/SimDES/version.svg)](https://juliahub.com/ui/Packages/SimDES/0.1.0)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

---

## Overview

`SimDES.jl` is a high-performance discrete-event simulation library in pure Julia designed for operations research, manufacturing logistics, and queueing network analysis.

Every component in `SimDES.jl` is grounded in closed-form numerical mathematics and backed by continuous integration tests that verify simulation output against analytical queueing theory.

### Key Capabilities

1. **Analytical Queueing Benchmark Verification:**
   - Single and multi-server queues ($M/M/1$, $M/M/c$, $M/M/1/K$, $M/D/1$, $M/G/1$).
   - Multi-node networks: Jackson open product-form networks with routing probability matrices.
   - Non-homogeneous arrival processes: NHPP via Lewis-Shedler thinning.
   - Non-preemptive Head-Of-Line (HOL) priority queues.
   - Server availability degradation with stochastic failure/repair cycles.
   - Fork-Join synchronization systems audited against Baccelli-Makowski bounds.
2. **Physical Conveyor Kinematics:**
   - Free-flow, zero-pressure accumulation (ZPA), and indexing conveyors.
   - Physical non-overlapping headway constraints with strict zero-collision guarantees.
3. **Makie Package Extension (`SimDESMakieExt`):**
   - 2D process network schematics with flow vectors (`simplot`).
   - Discrete step-charts of queue occupancy over simulated time (`plot_queue_history`).
   - Server state schedules and Gantt timelines (`plot_gantt`).
   - Observable-backed entity flow animation (`animate_sim`).

---

## Installation

```julia
using Pkg
Pkg.add("SimDES")
```

---

## Early Stage Advisory (`v0.1.0`)

> **Notice:** `SimDES.jl` is released at version `v0.1.0`. While all queueing invariants and analytical models pass 100% automated verification, the public API may experience refinements prior to `v1.0.0`. Users in numerical mathematics and operations research are encouraged to audit their models and file feedback on [GitHub Issues](https://github.com/SouKot/SimDES.jl/issues).

---

## AI Collaboration Disclosure

This codebase was developed with the assistance of **Google DeepMind Antigravity**, an agentic AI pair programmer. All queueing theorems (Pollaczek-Khinchine, Erlang-B/C, Jackson's product form, Baccelli-Makowski bounds) and kinematic algorithms are empirically audited against closed-form mathematical equations.

---

## Runnable Verification Examples

`SimDES.jl` ships with 6 standalone, executable examples in its `examples/` directory:

| Script | Theoretical / Algorithmic Scope |
| :--- | :--- |
| `01_mm1_analytical_audit.jl` | $M/M/1$ queue benchmarked against closed-form theory. |
| `02_jackson_open_network.jl` | 3-station open Jackson network with product-form routing. |
| `03_conveyor_accumulation.jl` | Zero-Pressure Accumulation (ZPA) physical line under bottleneck. |
| `04_nhpp_call_center.jl` | Time-varying Poisson demand via Lewis-Shedler thinning. |
| `05_makie_visualization.jl` | Makie recipes, live Observable animation, and schedule timelines. |
| `06_interactive_conveyor_makie.jl` | Interactive GLMakie GUI with animated conveyor, obstruction gate, and ZPA/rigid modes. |

### Running from the Terminal
```bash
julia --project=. examples/01_mm1_analytical_audit.jl
julia --project=. examples/06_interactive_conveyor_makie.jl
```

### Running from the Julia REPL
```julia
using Pkg; Pkg.activate(".")
include("examples/01_mm1_analytical_audit.jl")
include("examples/05_makie_visualization.jl")
include("examples/06_interactive_conveyor_makie.jl")
```

---

## Manual Contents


```@contents
Pages = [
    "queueing_theory.md",
    "conveyors.md",
    "makie_recipes.md",
    "api.md",
]
Depth = 2
```
