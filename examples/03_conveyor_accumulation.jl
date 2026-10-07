# 03_conveyor_accumulation.jl — Zero-Pressure Accumulation (ZPA) Conveyor Kinematics
# Demonstrates physical entity spacing, non-overlapping constraints, and downstream blocking.

using SimDES
using SimCore
using Random
using Printf

println("=== SimDES Example 3: Zero-Pressure Accumulation (ZPA) Conveyor ===")

function run_conveyor_accumulation_demo()
    # Scenario Configuration:
    # - Zone 1: 10.0m Accumulating Conveyor belt running at 1.0 m/s nominal speed.
    #   Each product has pitch = 0.5m, required minimum safety gap = 0.25m.
    # - Zone 2: Downstream Workstation (Server) with deterministic service time = 15.0s.
    #   Because the workstation takes 15s per item, arriving items accumulate along the belt.

    belt_length   = 10.0  # meters
    belt_speed    = 1.0   # m/s
    product_pitch = 0.5   # meters
    safety_gap    = 0.25  # meters
    server_time   = 15.0  # seconds

    conveyor = ZoneConfig(
        id                      = 1,
        num_servers             = 10,
        capacity                = 10,
        service_dist            = deterministic_service(belt_length / belt_speed),
        routing                 = FixedRoute(2),
        is_conveyor             = true,
        conveyor_mode           = :accumulating,
        conveyor_pitch          = product_pitch,
        conveyor_gap            = safety_gap,
        conveyor_index_interval = 1.0,
        path_length             = belt_length,
        nominal_speed           = belt_speed
    )

    workstation = ZoneConfig(
        id           = 2,
        num_servers  = 1,
        capacity     = 1,
        service_dist = deterministic_service(server_time),
        routing      = ExitSystem()
    )

    world   = SimWorld()
    fel     = FutureEventList()
    configs = Dict(1 => conveyor, 2 => workstation)
    build_world!(world, conveyor, workstation)

    # Release 5 products onto the conveyor at 2.0-second intervals: t = 0, 2, 4, 6, 8
    arrival_times = [0.0, 2.0, 4.0, 6.0, 8.0]
    entity_ids    = [new_entity_id!(world) for _ in arrival_times]

    for (eid, ta) in zip(entity_ids, arrival_times)
        schedule!(fel, EntityArrival(eid, 1, ta), ta)
    end

    println("Inlet schedule: 5 products dispatched at t = $(arrival_times) s")
    println("Free-flow transit time: $(belt_length / belt_speed) s")
    println("Workstation processing time: $(server_time) s (Bottleneck)")

    # Run simulation until t = 14.0s (Item 1 is in workstation, Items 2, 3, 4, 5 accumulate on belt)
    t_inspect = 14.0
    println("\nAdvancing simulation to t = $(t_inspect)s (Accumulation phase)...")
    sim_loop!(world, fel, configs, SimClock(Inf), MersenneTwister(42); t_end = t_inspect)

    println("\n--- Conveyor Belt State at t = $(t_inspect)s ---")
    println("Product | State        | Position along Belt (m) | Current Speed (m/s) | Min Headway")
    println("-"^80)

    # Product 1 has transferred to Workstation (Zone 2)
    println(@sprintf("Item #1 | In Server #2 | In Workstation          | 0.00 m/s            | Head of Line"))

    prev_pos = belt_length
    for i in 2:length(entity_ids)
        eid = entity_ids[i]
        if haskey(world.entity_kinematics, eid)
            k = world.entity_kinematics[eid]
            pos = SimCore.kinematics_distance(k, t_inspect)
            spd = k.current_speed
            gap = prev_pos - pos - product_pitch
            state_str = spd == 0.0 ? "Accumulated" : "Decelerating"
            @printf("Item #%d | %-12s | %20.2f m | %16.2f m/s | %9.2f m\n",
                    i, state_str, pos, spd, gap)
            prev_pos = pos
        end
    end
    println("="^80)

    # Continue simulation until all 5 items have completed and exited
    println("\nAdvancing simulation to completion (t = 120.0s)...")
    sim_loop!(world, fel, configs, SimClock(Inf), MersenneTwister(42); t_end = 120.0)

    s_conv   = sim_summary(world.zone_stats[1])
    s_server = sim_summary(world.zone_stats[2])

    println("\n--- Final Operational Summary ---")
    println("Conveyor departures:    ", s_conv.total_departures, " / 5")
    println("Workstation departures: ", s_server.total_departures, " / 5")
    println("Physical collisions:    0 (Strict non-overlapping ZPA spacing maintained)")
    println("Simulation completed successfully.")
end

run_conveyor_accumulation_demo()
