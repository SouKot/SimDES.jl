# 05_makie_visualization.jl — Makie Visualization Recipes Demo
# Demonstrates 2D Network Schematics, Queue Step-Charts, and Server Gantt Timelines.

using SimDES
using SimCore

println("=== SimDES Example 5: Makie Visualization Recipes ===")

function run_makie_visualization_demo()
    # Check if Makie / backend is available in the current environment
    makie_loaded = isdefined(Main, :Makie)
    if !makie_loaded
        try
            @eval using Makie: Makie, save
            makie_loaded = true
        catch
            makie_loaded = false
        end
    end

    if !makie_loaded
        println("""
[INFO] Makie is not loaded in the active environment.
To enable graphical recipes, load any Makie backend in your Julia session:

    using Pkg
    Pkg.add("CairoMakie")   # or GLMakie for interactive windows
    using CairoMakie
    using SimDES

Once Makie is loaded, the `SimDESMakieExt` extension activates automatically,
providing `simplot()`, `plot_queue_history()`, `plot_gantt()`, and `animate_sim()`.
""")
        return
    end

    println("Makie detected! Generating visualization figures...")

    # 1. Network Schematic (simplot)
    println("\n[1/3] Generating Network Schematic (`simplot`)...")
    source = ZoneConfig(id = 1, arrival = PoissonArrival(2.0), routing = FixedRoute(2))
    conveyor = ZoneConfig(id = 2, is_conveyor = true, conveyor_mode = :accumulating,
                          path_length = 5.0, nominal_speed = 1.0, routing = FixedRoute(3))
    workstation = ZoneConfig(id = 3, num_servers = 2, capacity = 5, routing = ExitSystem())

    fig_network = simplot([source, conveyor, workstation];
                          title = "Production Line Flow Network",
                          resolution = (900, 450))
    Base.invokelatest(Makie.save, "production_network_schematic.png", fig_network)
    println("Saved -> production_network_schematic.png")

    # 2. Queue Occupancy Step-Chart (plot_queue_history)
    println("\n[2/3] Generating Queue History Step-Chart (`plot_queue_history`)...")
    sim_times   = [0.0, 1.2, 2.5, 3.1, 4.0, 5.8, 7.2, 8.0, 10.0]
    queue_depth = [0,   1,   3,   2,   4,   2,   1,   0,   0]
    fig_queue   = plot_queue_history(sim_times, queue_depth;
                                     title = "Buffer Zone 3 Queue Occupancy Q(t)",
                                     resolution = (850, 400))
    Base.invokelatest(Makie.save, "queue_occupancy_chart.png", fig_queue)
    println("Saved -> queue_occupancy_chart.png")

    # 3. Server State Schedule (plot_gantt)
    println("\n[3/3] Generating Server State Gantt Timeline (`plot_gantt`)...")
    server_records = [
        (server_id = 1, state = :busy,    t_start = 0.0,  t_end = 4.5),
        (server_id = 1, state = :blocked, t_start = 4.5,  t_end = 6.0),
        (server_id = 1, state = :busy,    t_start = 6.0,  t_end = 9.0),
        (server_id = 1, state = :idle,    t_start = 9.0,  t_end = 12.0),
        (server_id = 2, state = :idle,    t_start = 0.0,  t_end = 2.0),
        (server_id = 2, state = :busy,    t_start = 2.0,  t_end = 7.5),
        (server_id = 2, state = :failed,  t_start = 7.5,  t_end = 10.0),
        (server_id = 2, state = :busy,    t_start = 10.0, t_end = 12.0),
    ]
    fig_gantt = plot_gantt(server_records;
                           title = "Multi-Server Workstation State Timeline",
                           resolution = (900, 400))
    Base.invokelatest(Makie.save, "server_gantt_timeline.png", fig_gantt)
    println("Saved -> server_gantt_timeline.png")

    println("\nAll Makie visualization recipes executed and saved successfully!")
end

run_makie_visualization_demo()
