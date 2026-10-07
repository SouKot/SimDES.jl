# Makie Visualization Recipes

## Overview

Visualization recipes in `SimDES.jl` are provided through the package extension `SimDESMakieExt`. Loading any Makie backend (`CairoMakie`, `GLMakie`, or `WGLMakie`) automatically activates the extension with zero overhead when Makie is unused.

---

## 2D Network Schematic (`simplot`)

`simplot(configs...; layout = :horizontal)` renders a 2D process network diagram. Stations are color-coded by role:
- **Green:** Arrival Source zones
- **Blue:** Processing / Workstation servers
- **Orange:** Conveyor zones
- **Purple:** Exit / Sink zones

Directed arrows illustrate routing and transfer topology.

```julia
using SimDES
using CairoMakie

source      = ZoneConfig(id = 1, arrival = PoissonArrival(1.0), routing = FixedRoute(2))
conveyor    = ZoneConfig(id = 2, is_conveyor = true, path_length = 6.0, routing = FixedRoute(3))
workstation = ZoneConfig(id = 3, num_servers = 2, routing = ExitSystem())

fig = simplot(source, conveyor, workstation; title = "Manufacturing Cell Flow")
save("network.png", fig)
```

---

## Queue Occupancy Step-Chart (`plot_queue_history`)

Renders a step-chart of discrete queue occupancy $Q(t)$ over simulated time with a horizontal dashed reference line showing the time-average mean.

```julia
times = [0.0, 1.2, 2.5, 3.1, 4.0, 5.8, 7.2]
q_len = [0,   1,   3,   2,   4,   2,   0]

fig = plot_queue_history(times, q_len; title = "Buffer Occupancy Q(t)")
save("queue_history.png", fig)
```

---

## Resource State Schedule (`plot_gantt`)

Renders horizontal timeline bars illustrating server states:
- **Green:** Busy (processing an entity)
- **Light Gray:** Idle (waiting for arrival)
- **Orange:** Blocked (waiting for downstream capacity)
- **Red:** Failed (breakdown or repair)

```julia
records = [
    (server_id = 1, state = :busy,    t_start = 0.0, t_end = 4.0),
    (server_id = 1, state = :blocked, t_start = 4.0, t_end = 5.5),
    (server_id = 1, state = :idle,    t_start = 5.5, t_end = 8.0),
    (server_id = 2, state = :busy,    t_start = 1.0, t_end = 6.0),
    (server_id = 2, state = :failed,  t_start = 6.0, t_end = 8.0),
]

fig = plot_gantt(records; title = "Machine Operational States")
save("gantt.png", fig)
```

---

## Observable Flow Animation (`animate_sim`)

Creates an Observable-backed canvas for interactive trajectory playback:

```julia
using SimDES
using GLMakie  # Opens an interactive window

snapshots = [
    (t = 0.0, items = [(x = 0.0, y = 0.0), (x = 2.0, y = 0.0)]),
    (t = 1.0, items = [(x = 1.0, y = 0.0), (x = 3.0, y = 0.0)]),
    (t = 2.0, items = [(x = 2.0, y = 0.0), (x = 4.0, y = 0.0)]),
    (t = 3.0, items = [(x = 3.0, y = 0.0), (x = 5.0, y = 0.0)]),
]

fig, step_obs = animate_sim(snapshots; resolution = (800, 350))
display(fig)

# Playback trajectory smoothly:
for i in 1:length(snapshots)
    step_obs[] = i
    sleep(0.1) # 100 ms per step
end
```

---

## Live Interactive Streaming with `step_sim!`

In discrete-event simulation, you can stream simulation state directly into an interactive Makie chart during the event loop:

```julia
using GLMakie
using SimDES, SimCore, Random

# 1. Setup Observable vectors
t_obs = Observable(Float64[0.0])
q_obs = Observable(Int[0])

fig = Figure(size = (850, 400))
ax  = Axis(fig[1, 1], title = "Live Streaming Buffer Occupancy Q(t)",
           xlabel = "Simulated Time (s)", ylabel = "Queue Length")
stairs!(ax, t_obs, q_obs, step = :pre, color = :royalblue, linewidth = 2)
display(fig)

# 2. Setup simulation world
world   = SimWorld()
fel     = FutureEventList()
cfg     = ZoneConfig(id = 1, arrival_rate = 1.5, num_servers = 1)
configs = Dict(1 => cfg)
build_world!(world, cfg)
rng     = MersenneTwister(42)
schedule!(fel, EntityArrival(UInt64(1), 1, 0.5), 0.5)

# 3. Stream events interactively:
for _ in 1:50
    step = step_sim!(world, fel, configs, rng)
    step === nothing && break

    push!(t_obs[], step.t)
    push!(q_obs[], length(world.zones[1].queue))
    notify(t_obs)
    notify(q_obs)
    sleep(0.05) # visual throttle
end
```

---

## Running the Complete Visualization Example

A complete runnable script exercising all four recipes is provided in:
- [`examples/05_makie_visualization.jl`](https://github.com/SouKot/SimDES.jl/blob/main/examples/05_makie_visualization.jl)

### From the Terminal
```bash
julia --project=. examples/05_makie_visualization.jl
```

### From the Julia REPL
```julia
using Pkg; Pkg.activate(".")
include("examples/05_makie_visualization.jl")
```

