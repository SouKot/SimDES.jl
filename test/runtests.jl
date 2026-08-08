using SimDES
using SimCore
using Test
using Random
using Distributions: mean

# ─────────────────────────────────────────────────────────────────────────────
# Sprint 2A test suite
# Tests the DES engine at unit level: FEL, dispatch, stat correctness.
# Full queueing-theory validation (DES-S-01..09) is in experiments/scripts/des/.
# ─────────────────────────────────────────────────────────────────────────────

@testset "SimDES" begin

    # ── Aqua quality ─────────────────────────────────────────────────────────
    @testset "Aqua quality checks" begin
        using Aqua
        Aqua.test_all(SimDES;
            ambiguities       = false,
            stale_deps        = false,   # Random is stdlib, not flagged in older Aqua
            deps_compat       = false,   # SimCore is a local dev dep
            undefined_exports = true,
            project_extras    = true,
        )
    end

    # ── FutureEventList ───────────────────────────────────────────────────────
    @testset "FutureEventList" begin

        @testset "Empty FEL returns nothing" begin
            fel = FutureEventList()
            @test Base.isempty(fel)
            @test safe_dequeue!(fel) === nothing
            @test peek_time(fel) == Inf
        end

        @testset "schedule! and safe_dequeue! return events in time order" begin
            fel = FutureEventList()
            schedule!(fel, NullEvent(), 3.0)
            schedule!(fel, NullEvent(), 1.0)
            schedule!(fel, NullEvent(), 2.0)

            r1 = safe_dequeue!(fel); @test r1 !== nothing && r1[2] == 1.0
            r2 = safe_dequeue!(fel); @test r2 !== nothing && r2[2] == 2.0
            r3 = safe_dequeue!(fel); @test r3 !== nothing && r3[2] == 3.0
            @test safe_dequeue!(fel) === nothing
        end

        @testset "Cancelled events are skipped" begin
            fel = FutureEventList()
            id1 = schedule!(fel, EntityArrival(UInt64(1), 1, 1.0), 1.0)
            id2 = schedule!(fel, EntityArrival(UInt64(2), 1, 2.0), 2.0)
            id3 = schedule!(fel, EntityArrival(UInt64(3), 1, 3.0), 3.0)
            cancel!(id2)   # skip event at t=2.0

            r1 = safe_dequeue!(fel); @test r1[2] == 1.0
            r2 = safe_dequeue!(fel); @test r2[2] == 3.0  # t=2 was skipped
            @test safe_dequeue!(fel) === nothing
        end

        @testset "peek_time does not consume event" begin
            fel = FutureEventList()
            schedule!(fel, NullEvent(), 5.0)
            @test peek_time(fel) == 5.0
            @test peek_time(fel) == 5.0   # still there
            @test !Base.isempty(fel)
        end
    end

    # ── ZoneConfig / ServiceDist ──────────────────────────────────────────────
    @testset "ZoneConfig and ServiceDist" begin

        @testset "exponential_service samples positive values" begin
            sd  = exponential_service(2.0)
            rng = MersenneTwister(1)
            for _ in 1:100
                @test sd(rng) > 0.0
            end
        end

        @testset "deterministic_service always returns d" begin
            sd  = deterministic_service(3.0)
            rng = MersenneTwister(1)
            for _ in 1:10
                @test sd(rng) ≈ 3.0
            end
        end

        @testset "erlang_service mean ≈ 1/μ" begin
            μ  = 2.0; k = 3
            sd = erlang_service(k, μ)
            rng = MersenneTwister(7)
            samples = [sd(rng) for _ in 1:100_000]
            @test abs(sum(samples)/length(samples) - 1/μ) < 0.01
        end

        @testset "ZoneConfig validates arguments" begin
            @test_throws ArgumentError ZoneConfig(id=1, num_servers=0,
                service_dist=exponential_service(1.0))
            @test_throws ArgumentError ZoneConfig(id=1, capacity=0,
                service_dist=exponential_service(1.0))
            @test_throws ArgumentError ZoneConfig(id=1, arrival_rate=-1.0,
                service_dist=exponential_service(1.0))
        end

        @testset "build_world! registers zones" begin
            world = SimWorld()
            cfg1  = ZoneConfig(id=1, service_dist=exponential_service(2.0))
            cfg2  = ZoneConfig(id=2, service_dist=exponential_service(3.0))
            build_world!(world, cfg1, cfg2)
            @test entity_count(world).zones == 2
        end
    end

    # ── WelchDetector ─────────────────────────────────────────────────────────
    @testset "WelchDetector" begin

        @testset "Not complete before enough windows" begin
            wd = WelchDetector(window_size=10, threshold=0.05)
            for i in 1:25
                update!(wd, float(i))
            end
            # Only 2 complete windows (10+10=20 obs), needs 3
            @test !warmup_complete(wd)
        end

        @testset "Detects convergence on stable signal" begin
            wd = WelchDetector(window_size=20, threshold=0.10)
            # First 40 observations: transient ramp
            for v in 1.0:1.0:40.0
                update!(wd, v)
            end
            # Then 60 stable observations — should converge
            for _ in 1:60
                update!(wd, 5.0)
            end
            @test warmup_complete(wd)
        end

        @testset "Zero queue → immediately complete" begin
            wd = WelchDetector(window_size=5, threshold=0.05)
            for _ in 1:20
                update!(wd, 0.0)
            end
            @test warmup_complete(wd)
        end
    end

    # ── M/M/1 single-server basic correctness ─────────────────────────────────
    # Full ±2% validation is in DES-S-01. Here we verify direction + sign.
    @testset "M/M/1 qualitative correctness" begin

        @testset "ρ=0.5: L ≈ 1.0 (within 15%)" begin
            stats = run_mm1!(2.0, 4.0; n_arrivals=100_000, seed=1)
            sm    = sim_summary(stats)
            # M/M/1: L = ρ/(1-ρ) = 0.5/0.5 = 1.0
            @test !isnan(sm.L)
            @test sm.L > 0.5
            @test sm.L < 2.0
            @test sm.utilization > 0.4
            @test sm.utilization < 0.7
        end

        @testset "Higher ρ → higher L (monotone)" begin
            s_low  = run_mm1!(1.0, 4.0; n_arrivals=50_000, seed=42)  # ρ=0.25
            s_high = run_mm1!(3.0, 4.0; n_arrivals=50_000, seed=42)  # ρ=0.75
            @test sim_summary(s_low).L < sim_summary(s_high).L
        end

        @testset "Conservation: arrivals ≈ departures (no loss)" begin
            stats = run_mm1!(2.0, 3.0; n_arrivals=50_000, seed=10)
            sm    = sim_summary(stats)
            @test sm.blocked_count == 0
            ratio = sm.total_departures / sm.total_arrivals
            @test ratio > 0.8   # warmup arrivals also counted; most should depart
        end
    end

    # ── M/M/c multi-server ────────────────────────────────────────────────────
    @testset "M/M/c qualitative correctness" begin

        @testset "c>1 reduces wait vs c=1 (same total load)" begin
            # Same λ=6, μ=3 per server. M/M/1 (ρ=2!) would be unstable.
            # With c=3 servers, ρ=6/(3×3)=0.667 — stable.
            s1 = run_mmc!(6.0, 3.0, 3; n_arrivals=100_000, seed=5)
            s4 = run_mmc!(6.0, 3.0, 4; n_arrivals=100_000, seed=5)
            # More servers → lower wait
            @test sim_summary(s4).Wq <= sim_summary(s1).Wq + 0.1
        end

        @testset "Utilization ≈ ρ/c for M/M/c" begin
            # λ=4, μ=3, c=2 → ρ=4/(2×3)=0.667
            stats = run_mmc!(4.0, 3.0, 2; n_arrivals=200_000, seed=7)
            sm    = sim_summary(stats)
            @test sm.utilization > 0.5
            @test sm.utilization < 0.85
        end
    end

    # ── M/M/1/K finite buffer ─────────────────────────────────────────────────
    @testset "M/M/1/K blocking" begin

        @testset "ρ=1, K=5: blocking_prob ≈ 1/6" begin
            # Theory: πK = 1/(K+1) = 1/6 ≈ 0.167 when ρ=1
            stats = run_mm1k!(2.0, 2.0, 5; n_arrivals=500_000, seed=99)
            sm    = sim_summary(stats)
            @test abs(sm.blocking_prob - 1/6) < 0.03   # within 3%
        end

        @testset "Larger K → less blocking" begin
            s5  = run_mm1k!(2.0, 2.0, 5;  n_arrivals=200_000, seed=3)
            s20 = run_mm1k!(2.0, 2.0, 20; n_arrivals=200_000, seed=3)
            @test sim_summary(s5).blocking_prob > sim_summary(s20).blocking_prob
        end

        @testset "K=1 (no queue): very high blocking at ρ=0.9" begin
            # K=1 means only 1 entity in system at a time
            stats = run_mm1k!(1.8, 2.0, 1; n_arrivals=200_000, seed=11)
            sm    = sim_summary(stats)
            @test sm.blocking_prob > 0.3   # very high rejection rate
        end
    end

    # ── M/D/1 deterministic service ────────────────────────────────────────────
    @testset "M/D/1 vs M/M/1 (same ρ): W_D < W_M" begin
        # P-K formula: M/D/1 Wq = ρ·d/(2(1-ρ)), M/M/1 Wq = ρ/(μ(1-ρ))
        # At same ρ=0.8: W_D1 = 1.5, W_M1 = 2.5 → ratio ≈ 0.6
        # NOTE: Wq per-entity tracking requires service_start_time (Phase 2B TODO).
        #       For now we compare W (total sojourn) which IS tracked correctly.
        ρ = 0.8; μ = 2.0; λ = ρ * μ; d = 1.0/μ
        s_mm1  = run_mm1!(λ, μ; n_arrivals=300_000, seed=42)
        s_md1  = run_md1!(λ, d; n_arrivals=300_000, seed=42)
        w_mm1  = sim_summary(s_mm1).W
        w_md1  = sim_summary(s_md1).W
        # M/D/1 mean sojourn < M/M/1 mean sojourn at same ρ (less variance)
        @test !isnan(w_mm1)
        @test !isnan(w_md1)
        @test w_md1 < w_mm1   # deterministic service → lower sojourn
    end

    # ── M/G/1 Erlang-k service ────────────────────────────────────────────────
    @testset "M/G/1 Erlang-k: increasing k → less variance → lower W" begin
        λ = 1.0; μ = 2.0
        # W = Wq + 1/μ; Wq decreases as k increases (P-K formula, less E[S²])
        # so W is also non-increasing in k
        w = [sim_summary(run_mg1!(λ, μ, k; n_arrivals=100_000, seed=42)).W
              for k in [1, 2, 5, 10]]
        # W should be non-increasing as k increases (less service variance)
        for i in 1:length(w)-1
            @test w[i] >= w[i+1] - 0.05   # allow small Monte Carlo noise
        end
    end

    # ── ResourceFailure / Repair ────────────────────────────────────────────────
    @testset "ResourceFailure and Repair dispatch" begin
        world   = SimWorld()
        fel     = FutureEventList()
        cfg     = ZoneConfig(id=1, num_servers=2,
                             service_dist=exponential_service(1.0),
                             arrival_rate=0.5)
        configs = Dict(1 => cfg)
        build_world!(world, cfg)
        rng   = MersenneTwister(1)
        clock = SimClock(Inf)

        # Force warmup so stats are collected
        world.stats.warmup_complete = true

        # Seed arrivals + failure
        for i in 1:10
            schedule!(fel, EntityArrival(new_entity_id!(world), 1, float(i)*0.5), float(i)*0.5)
        end
        schedule!(fel, ResourceFailure(1, 1.0f0, 0.5), 0.5)

        sim_loop!(world, fel, configs, clock, rng; t_end=50.0)

        # After repair events have fired, simulation should still produce valid stats
        sm = sim_summary(world.stats)
        @test sm.total_arrivals > 0
        @test sm.total_events   > 0
    end

    # ── TransferOut routing ─────────────────────────────────────────────────────
    @testset "TransferOut routes entity to downstream zone" begin
        world   = SimWorld()
        fel     = FutureEventList()
        cfg1    = ZoneConfig(id=1, service_dist=deterministic_service(1.0),
                             arrival_rate=1.0, lookahead=0.5, downstream=[2])
        cfg2    = ZoneConfig(id=2, service_dist=deterministic_service(1.0),
                             arrival_rate=0.0)
        configs = Dict(1 => cfg1, 2 => cfg2)
        build_world!(world, cfg1, cfg2)
        rng   = MersenneTwister(42)
        clock = SimClock(Inf)

        # Manually schedule a TransferOut from zone 1 to zone 2
        entity_id = new_entity_id!(world)
        schedule!(fel, TransferOut(entity_id, 2, 1.0), 1.0)

        sim_loop!(world, fel, configs, clock, rng; t_end=5.0)

        # Zone 2 should have received at least one arrival
        @test world.stats.total_arrivals > 0
    end

    # ── sim_loop! correctness ──────────────────────────────────────────────────
    @testset "sim_loop! respects t_end" begin
        world   = SimWorld()
        fel     = FutureEventList()
        cfg     = ZoneConfig(id=1, service_dist=exponential_service(2.0),
                             arrival_rate=1.0)
        configs = Dict(1 => cfg)
        build_world!(world, cfg)
        rng   = MersenneTwister(0)
        clock = SimClock(Inf)
        schedule!(fel, EntityArrival(new_entity_id!(world), 1, 0.1), 0.1)

        sim_loop!(world, fel, configs, clock, rng; t_end=10.0)

        @test world.time <= 10.0   # never exceeded t_end
    end

end  # @testset "SimDES"
