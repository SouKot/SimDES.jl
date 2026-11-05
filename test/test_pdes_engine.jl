using Test
using Random
using Distributions: Exponential
using SimCore
using SimDES

@testset "Phase 2: Conservative PDES Core (SubModel, Promise, SyncEvent, Engine Equivalence)" begin

    @testset "SubModelDependencyGraph & Promise Horizon Invariants" begin
        g = SubModelDependencyGraph(3; strict_check = true)
        e12 = add_submodel_dependency!(g, 1, 2, 0.25)
        e23 = add_submodel_dependency!(g, 2, 3, 0.50)

        @test e12.min_delay_ps == to_picoseconds(0.25)
        @test e23.min_delay_ps == to_picoseconds(0.50)

        # SubModel 1 has no incoming dependencies -> safe horizon equals ceiling_ps
        ceiling_ps = to_picoseconds(10.0)
        @test safe_horizon_ps(g, 1, ceiling_ps) == ceiling_ps

        # Advance SubModel 1 local next event to t = 1.0s
        update_outgoing_promises!(g, 1, to_picoseconds(1.0))
        @test e12.promised_ps[] == to_picoseconds(1.25)

        # Safe horizon for SubModel 2 is strictly earlier than 1.25s -> 1.25s - 1ps
        @test safe_horizon_ps(g, 2, ceiling_ps) == to_picoseconds(1.25) - 1

        # Promise regression check (strict_check = true)
        @test_throws AssertionError set_promise_ps!(g, e12, to_picoseconds(1.10))
    end

    @testset "LockFreeMPSCQueue & Two-Phase CrossSubModelSyncEvent" begin
        w_src = SimWorld(seed = 101, use_zone_rngs = true, use_partitioned_entity_ids = true)
        w_dst = SimWorld(seed = 101, use_zone_rngs = true, use_partitioned_entity_ids = true)

        uid = new_entity_id!(w_src, 1)
        w_src.des_agents[uid] = DESAgent(0.5, 1, 2, 1.5)
        w_src.entry_times[uid] = 0.5
        SimCore.set_entity_attribute!(w_src, uid, "batch_code", "B-42")

        ev = EntityArrival(uid, 2, 2.0, 0, false)
        key = EventKey(to_picoseconds(2.0), default_event_priority_eps(ev), Int32(1), UInt64(1))

        # Phase 1: on_sender_side! extracts entity state from sender shard
        sync_ev = on_sender_side!(w_src, 1, 2, key, ev, 2.0)
        @test !haskey(w_src.des_agents, uid)
        @test !haskey(w_src.entry_times, uid)
        @test sync_ev.des_agent !== nothing
        @test sync_ev.entry_time == 0.5
        @test sync_ev.attributes !== nothing && sync_ev.attributes["batch_code"] == "B-42"

        # Push through LockFreeMPSCQueue and drain on target side
        q = LockFreeMPSCQueue()
        push_sync_event!(q, sync_ev)
        fel_dst = FutureEventList(deterministic_mode = true)
        n_drained = drain_sync_events!(q, w_dst, fel_dst)

        @test n_drained == 1
        @test haskey(w_dst.des_agents, uid)
        @test w_dst.entry_times[uid] == 0.5
        @test SimCore.get_entity_attribute(w_dst, uid, "batch_code", "") == "B-42"
        @test peek_key(fel_dst) == key
    end

    @testset "Layer 2 Equivalence: SerialDESEngine vs ConservativePDESEngine(K=1)" begin
        function run_single_partition_comparison(use_pdes::Bool)
            world = SimWorld(seed = 2026, use_zone_rngs = true, use_partitioned_entity_ids = true)
            cfg1 = ZoneConfig(id=1, num_servers=2, capacity=typemax(Int),
                              service_dist=exponential_service(4.0), routing=FixedRoute(2), arrival_rate=3.0)
            cfg2 = ZoneConfig(id=2, num_servers=2, capacity=typemax(Int),
                              service_dist=erlang_service(3, 5.0), routing=ExitSystem(), arrival_rate=0.0)
            configs = Dict{Int, ZoneConfig}(1 => cfg1, 2 => cfg2)
            build_world!(world, cfg1, cfg2)
            world.stats.warmup_complete = true
            for z in 1:2
                world.zone_stats[z].warmup_complete = true
            end

            fel = FutureEventList(deterministic_mode = true)
            z1_rng = get_zone_rng(world, 1)
            t_first = rand(z1_rng, Exponential(1.0 / 3.0))
            schedule!(fel, EntityArrival(new_entity_id!(world, 1), 1, t_first), t_first)

            engine = if use_pdes
                ConservativePDESEngine(world, configs, [[1, 2]]; initial_fel = fel, n_workers = 1)
            else
                SerialDESEngine(fel; deterministic_mode = true)
            end
            for step in 1:100
                step_des_engine_until!(engine, world, configs, step * 0.5)
            end
            return world, engine.total_events
        end

        w_ser, ev_ser = run_single_partition_comparison(false)
        w_pdes, ev_pdes = run_single_partition_comparison(true)

        @test ev_ser == ev_pdes
        @test ev_ser > 100
        @test w_ser.stats.total_arrivals == w_pdes.stats.total_arrivals
        @test w_ser.stats.total_departures == w_pdes.stats.total_departures
        @test w_ser.stats.wait_time_sum == w_pdes.stats.wait_time_sum
        @test w_ser.stats.sojourn_time_sum == w_pdes.stats.sojourn_time_sum
        for z in 1:2
            @test w_ser.zone_stats[z].total_arrivals == w_pdes.zone_stats[z].total_arrivals
            @test w_ser.zone_stats[z].total_departures == w_pdes.zone_stats[z].total_departures
            @test w_ser.zone_stats[z].wait_time_sum == w_pdes.zone_stats[z].wait_time_sum
            @test w_ser.zone_stats[z].sojourn_time_sum == w_pdes.zone_stats[z].sojourn_time_sum
        end
    end

    @testset "Layer 3 Equivalence: 4-SubModel Tandem Network (Serial == PDES 1T == PDES 4T)" begin
        function run_tandem_4sm(mode::Symbol)
            world = SimWorld(seed = 77777, use_zone_rngs = true, use_partitioned_entity_ids = true)
            cfg1 = ZoneConfig(id=1, num_servers=2, capacity=typemax(Int),
                              service_dist=exponential_service(5.0), routing=FixedRoute(2), arrival_rate=3.5)
            cfg2 = ZoneConfig(id=2, num_servers=2, capacity=typemax(Int),
                              service_dist=exponential_service(5.0), routing=FixedRoute(3), arrival_rate=0.0)
            cfg3 = ZoneConfig(id=3, num_servers=2, capacity=typemax(Int),
                              service_dist=exponential_service(5.0), routing=FixedRoute(4), arrival_rate=0.0)
            cfg4 = ZoneConfig(id=4, num_servers=2, capacity=typemax(Int),
                              service_dist=exponential_service(5.0), routing=ExitSystem(), arrival_rate=0.0)
            configs = Dict{Int, ZoneConfig}(1 => cfg1, 2 => cfg2, 3 => cfg3, 4 => cfg4)
            build_world!(world, cfg1, cfg2, cfg3, cfg4)
            world.stats.warmup_complete = true
            for z in 1:4
                world.zone_stats[z].warmup_complete = true
            end

            # Configure physical transport wire delays (0.2s between consecutive stations)
            set_wire_delay!(world, 1, 2, 0.20)
            set_wire_delay!(world, 2, 3, 0.20)
            set_wire_delay!(world, 3, 4, 0.20)

            fel = FutureEventList(deterministic_mode = true)
            z1_rng = get_zone_rng(world, 1)
            t_first = rand(z1_rng, Exponential(1.0 / 3.5))
            schedule!(fel, EntityArrival(new_entity_id!(world, 1), 1, t_first), t_first)

            engine = if mode == :serial
                SerialDESEngine(fel; deterministic_mode = true)
            elseif mode == :pdes_1t
                ConservativePDESEngine(
                    world, configs, [[1], [2], [3], [4]];
                    initial_fel = fel, n_workers = 1, force_parallel = false,
                )
            else
                ConservativePDESEngine(
                    world, configs, [[1], [2], [3], [4]];
                    initial_fel = fel, n_workers = 4, force_parallel = true,
                )
            end

            for step in 1:60
                step_des_engine_until!(engine, world, configs, step * 1.0)
            end
            return world, engine.total_events
        end

        w_ser, ev_ser = run_tandem_4sm(:serial)
        w_p1, ev_p1   = run_tandem_4sm(:pdes_1t)
        w_p4, ev_p4   = run_tandem_4sm(:pdes_4t)

        @test ev_ser == ev_p1 == ev_p4
        @test ev_ser > 400
        @test w_ser.stats.total_arrivals == w_p1.stats.total_arrivals == w_p4.stats.total_arrivals
        @test w_ser.stats.total_departures == w_p1.stats.total_departures == w_p4.stats.total_departures

        for z in 1:4
            @test w_ser.zone_stats[z].total_arrivals == w_p1.zone_stats[z].total_arrivals == w_p4.zone_stats[z].total_arrivals
            @test w_ser.zone_stats[z].total_departures == w_p1.zone_stats[z].total_departures == w_p4.zone_stats[z].total_departures
            @test w_ser.zone_stats[z].wait_time_sum == w_p1.zone_stats[z].wait_time_sum == w_p4.zone_stats[z].wait_time_sum
            @test w_ser.zone_stats[z].sojourn_time_sum == w_p1.zone_stats[z].sojourn_time_sum == w_p4.zone_stats[z].sojourn_time_sum
        end
    end

    @testset "Layer 3 Equivalence: 4-SubModel Jackson Feedback Network (Serial == PDES 1T == PDES 4T)" begin
        function run_jackson_4sm(mode::Symbol)
            world = SimWorld(seed = 99991, use_zone_rngs = true, use_partitioned_entity_ids = true)
            cfg1 = ZoneConfig(id=1, num_servers=2, capacity=typemax(Int),
                              service_dist=exponential_service(6.0),
                              routing=ProbRoute([(2, 0.6), (3, 0.4)]), arrival_rate=4.0)
            cfg2 = ZoneConfig(id=2, num_servers=2, capacity=typemax(Int),
                              service_dist=exponential_service(5.0),
                              routing=ProbRoute([(4, 0.8), (1, 0.2)]), arrival_rate=0.0)
            cfg3 = ZoneConfig(id=3, num_servers=2, capacity=typemax(Int),
                              service_dist=exponential_service(5.0),
                              routing=FixedRoute(4), arrival_rate=0.0)
            cfg4 = ZoneConfig(id=4, num_servers=2, capacity=typemax(Int),
                              service_dist=exponential_service(7.0),
                              routing=ProbRoute([(nothing, 0.85), (2, 0.15)]), arrival_rate=0.0)
            configs = Dict{Int, ZoneConfig}(1 => cfg1, 2 => cfg2, 3 => cfg3, 4 => cfg4)
            build_world!(world, cfg1, cfg2, cfg3, cfg4)
            world.stats.warmup_complete = true
            for z in 1:4
                world.zone_stats[z].warmup_complete = true
            end

            # Every cross-zone link has positive physical transport delay >= 0.15s
            set_wire_delay!(world, 1, 2, 0.15)
            set_wire_delay!(world, 1, 3, 0.20)
            set_wire_delay!(world, 2, 4, 0.15)
            set_wire_delay!(world, 2, 1, 0.25)
            set_wire_delay!(world, 3, 4, 0.15)
            set_wire_delay!(world, 4, 2, 0.20)

            fel = FutureEventList(deterministic_mode = true)
            z1_rng = get_zone_rng(world, 1)
            t_first = rand(z1_rng, Exponential(1.0 / 4.0))
            schedule!(fel, EntityArrival(new_entity_id!(world, 1), 1, t_first), t_first)

            engine = if mode == :serial
                SerialDESEngine(fel; deterministic_mode = true)
            elseif mode == :pdes_1t
                ConservativePDESEngine(
                    world, configs, [[1], [2], [3], [4]];
                    initial_fel = fel, n_workers = 1, force_parallel = false,
                )
            else
                ConservativePDESEngine(
                    world, configs, [[1], [2], [3], [4]];
                    initial_fel = fel, n_workers = 4, force_parallel = true,
                )
            end

            for step in 1:50
                step_des_engine_until!(engine, world, configs, step * 1.0)
            end
            return world, engine.total_events
        end

        w_ser, ev_ser = run_jackson_4sm(:serial)
        w_p1, ev_p1   = run_jackson_4sm(:pdes_1t)
        w_p4, ev_p4   = run_jackson_4sm(:pdes_4t)

        @test ev_ser == ev_p1 == ev_p4
        @test ev_ser > 300
        @test w_ser.stats.total_arrivals == w_p1.stats.total_arrivals == w_p4.stats.total_arrivals
        @test w_ser.stats.total_departures == w_p1.stats.total_departures == w_p4.stats.total_departures

        for z in 1:4
            @test w_ser.zone_stats[z].total_arrivals == w_p1.zone_stats[z].total_arrivals == w_p4.zone_stats[z].total_arrivals
            @test w_ser.zone_stats[z].total_departures == w_p1.zone_stats[z].total_departures == w_p4.zone_stats[z].total_departures
            @test w_ser.zone_stats[z].wait_time_sum == w_p1.zone_stats[z].wait_time_sum == w_p4.zone_stats[z].wait_time_sum
            @test w_ser.zone_stats[z].sojourn_time_sum == w_p1.zone_stats[z].sojourn_time_sum == w_p4.zone_stats[z].sojourn_time_sum
        end
    end

end
