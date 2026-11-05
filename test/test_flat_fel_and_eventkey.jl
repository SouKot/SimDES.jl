using Test
using Random
using SimCore
using SimDES

@testset "Phase 1 — FlatBinaryMinHeap, 4-Tuple EventKey, & Per-Zone RNG Streams" begin

    @testset "1.1 EventKey picosecond quantization & 4-tuple lexicographic ordering" begin
        t_sum  = 0.1 + 0.1 + 0.1      # 0.30000000000000004 (1 ULP above 0.3)
        t_mult = 3.0 * 0.1            # 0.30000000000000004
        t_lit  = 0.3                  # 0.29999999999999999
        @test t_sum != t_lit          # IEEE-754 Float64 differs by 1 ULP
        @test to_picoseconds(t_sum) == to_picoseconds(t_lit) == Int64(300_000_000_000)
        @test isapprox(from_picoseconds(to_picoseconds(t_lit)), 0.3; atol=1e-15)

        # Lexicographic priority: t_ps -> priority_eps -> source_zone -> seq
        k1 = EventKey(Int64(1000), Int32(-10), Int32(2), UInt64(99))
        k2 = EventKey(Int64(1000), Int32(10),  Int32(1), UInt64(1))
        @test isless(k1, k2)          # priority_eps -10 < +10 wins over zone/seq

        k3 = EventKey(Int64(1000), Int32(0), Int32(1), UInt64(50))
        k4 = EventKey(Int64(1000), Int32(0), Int32(2), UInt64(1))
        @test isless(k3, k4)          # source_zone 1 < 2 wins at equal (t_ps, priority_eps)

        k5 = EventKey(Int64(1000), Int32(0), Int32(2), UInt64(10))
        k6 = EventKey(Int64(1000), Int32(0), Int32(2), UInt64(11))
        @test isless(k5, k6)          # seq 10 < 11 breaks final tie deterministically
    end

    @testset "1.2 FlatBinaryMinHeap strict FIFO on 1-ULP sum-path ties & lazy cancellation" begin
        fel = FutureEventList()
        # Entity 101 arrives via 3 steps of 0.1s; Entity 202 arrives via 1 step of 0.3s;
        # Entity 303 arrives via 0.15 + 0.15. All 3 must dequeue in strict FIFO order [101, 202, 303]!
        id1 = schedule!(fel, EntityArrival(UInt64(101), 1, 0.1 + 0.1 + 0.1), 0.1 + 0.1 + 0.1)
        id2 = schedule!(fel, EntityArrival(UInt64(202), 1, 0.3),             0.3)
        id3 = schedule!(fel, EntityArrival(UInt64(303), 1, 0.15 + 0.15),     0.15 + 0.15)
        id4 = schedule!(fel, EntityArrival(UInt64(404), 1, 0.3),             0.3)

        # Cancel the 3rd event (303)
        cancel!(fel, id3)

         popped_ids = UInt64[]
        while (res = safe_dequeue!(fel)) !== nothing
            cev, _ = res
            push!(popped_ids, cev.inner.entity_id)
        end
        @test popped_ids == UInt64[101, 202, 404]
    end

    @testset "1.3 FlatBinaryMinHeap explicit 4-tuple tie-breaking across zones" begin
        fel = FutureEventList()
        # Schedule Arrival (+10) at t=1.0s before ProcessComplete (-10) at t=1.0s
        schedule!(fel, EntityArrival(UInt64(1), 2, 1.0),    1.0; priority_eps=default_event_priority_eps(EntityArrival(UInt64(1), 2, 1.0)),    source_zone=2, seq=UInt64(1))
        schedule!(fel, ProcessComplete(UInt64(2), 1, 1.0),  1.0; priority_eps=default_event_priority_eps(ProcessComplete(UInt64(2), 1, 1.0)),  source_zone=1, seq=UInt64(5))
        schedule!(fel, ScheduledChange{:Repair}(3, 1.0),    1.0; priority_eps=default_event_priority_eps(ScheduledChange{:Repair}(3, 1.0)),    source_zone=3, seq=UInt64(9))

        n1 = safe_dequeue_node!(fel)
        n2 = safe_dequeue_node!(fel)
        n3 = safe_dequeue_node!(fel)
        @test n1.event.inner isa ScheduledChange{:Repair}   # priority_eps = -20
        @test n2.event.inner isa ProcessComplete            # priority_eps = -10
        @test n3.event.inner isa EntityArrival              # priority_eps = +10
    end

    @testset "1.4 Per-Zone RNG stream isolation & partitioned Entity IDs" begin
        # Two worlds with the same seed=777 and `enable_zone_rngs!`.
        # In World A, Zone 1 draws 50 random service times before Zone 2 draws 1.
        # In World B, Zone 2 draws 1 random service time before Zone 1 draws any.
        # Both Zone 1 and Zone 2 must get the exact same random numbers in World A and World B!
        wA = SimWorld()
        wB = SimWorld()
        add_zone!(wA, 1); add_zone!(wA, 2)
        add_zone!(wB, 1); add_zone!(wB, 2)
        enable_zone_rngs!(wA; seed=777, partitioned_entity_ids=true)
        enable_zone_rngs!(wB; seed=777, partitioned_entity_ids=true)

        # World A: draw 50 values from Zone 1, then 5 from Zone 2
        z1_A = [rand(get_zone_rng(wA, 1)) for _ in 1:50]
        z2_A = [rand(get_zone_rng(wA, 2)) for _ in 1:5]

        # World B: draw 5 values from Zone 2 first, then 50 from Zone 1
        z2_B = [rand(get_zone_rng(wB, 2)) for _ in 1:5]
        z1_B = [rand(get_zone_rng(wB, 1)) for _ in 1:50]

        @test z1_A == z1_B
        @test z2_A == z2_B

        # Partitioned Entity IDs: disjoint per zone and fit inside positive Int32 (EntityHandle safe)
        id_z1_1 = new_entity_id!(wA, 1)
        id_z1_2 = new_entity_id!(wA, 1)
        id_z2_1 = new_entity_id!(wA, 2)
        @test id_z1_1 != id_z2_1
        @test id_z1_1 < UInt64(typemax(Int32))
        @test id_z2_1 < UInt64(typemax(Int32))
        @test EntityHandle(Int32(id_z2_1), UInt8(2)).id == Int32(id_z2_1)
    end
end

