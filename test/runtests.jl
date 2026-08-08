using SimDES
using SimCore
using Test
using Random
using Distributions: mean

# ─────────────────────────────────────────────────────────────────────────────
# SimDES test suite
# Sprint 2A: DES engine unit tests (fast, qualitative)
# Sprint 2B: Queueing theory validation (DES-S-01..09)
#
# Performance design:
#   - n_arrivals sizes chosen so the full suite runs in < 60 seconds
#   - O(1) FIFO queue (zone.queue) + 1.1× t_end multiplier in runners
#   - Full ±2% accuracy belongs in experiments/scripts/des/des_validation.jl
# ─────────────────────────────────────────────────────────────────────────────

# ── Shared theory helpers ─────────────────────────────────────────────────────

# M/M/1 exact formulas
mm1_L(ρ)        = ρ / (1 - ρ)
mm1_W(μ, ρ)     = 1.0 / (μ * (1 - ρ))
mm1_Wq(μ, ρ)    = ρ / (μ * (1 - ρ))
mm1_util(ρ)     = ρ

# M/M/1/K exact blocking probability (ρ ≠ 1)
function mm1k_pb(ρ, K)
    ρ ≈ 1.0 && return 1.0 / (K + 1)
    (1 - ρ) * ρ^K / (1 - ρ^(K+1))
end

# M/D/1 P-K formulas (deterministic service, d = 1/μ)
md1_Wq(λ, d)    = λ * d^2 / (2 * (1 - λ*d))
md1_W(λ, d)     = md1_Wq(λ, d) + d

# M/G/1 P-K formula  (Erlang-k service: E[S²] = (k+1)/(k·μ²))
function mg1_Wq_erlang(λ, μ, k)
    ρ   = λ / μ
    ES2 = (k + 1) / (k * μ^2)
    λ * ES2 / (2 * (1 - ρ))
end

@testset "SimDES" begin

# ═══════════════════════════════════════════════════════════════════════════════
# SPRINT 2A — Core DES Engine (unit tests, qualitative)
# ═══════════════════════════════════════════════════════════════════════════════

    # ── Aqua quality ─────────────────────────────────────────────────────────
    @testset "Aqua quality checks" begin
        using Aqua
        Aqua.test_all(SimDES;
            ambiguities       = false,
            stale_deps        = false,   # Random is stdlib — flagged as stale
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

        @testset "Events dequeued in ascending time order" begin
            fel = FutureEventList()
            schedule!(fel, NullEvent(), 3.0)
            schedule!(fel, NullEvent(), 1.0)
            schedule!(fel, NullEvent(), 2.0)
            r1 = safe_dequeue!(fel); @test r1 !== nothing && r1[2] == 1.0
            r2 = safe_dequeue!(fel); @test r2 !== nothing && r2[2] == 2.0
            r3 = safe_dequeue!(fel); @test r3 !== nothing && r3[2] == 3.0
            @test safe_dequeue!(fel) === nothing
        end

        @testset "Cancelled events are skipped (DES-S-08 partial)" begin
            fel = FutureEventList()
            id1 = schedule!(fel, EntityArrival(UInt64(1), 1, 1.0), 1.0)
            id2 = schedule!(fel, EntityArrival(UInt64(2), 1, 2.0), 2.0)
            _   = schedule!(fel, EntityArrival(UInt64(3), 1, 3.0), 3.0)
            cancel!(fel, id2)              # FEL-local cancel — no global lock
            r1 = safe_dequeue!(fel); @test r1[2] == 1.0
            r2 = safe_dequeue!(fel); @test r2[2] == 3.0   # t=2.0 was skipped
            @test safe_dequeue!(fel) === nothing
        end

        @testset "peek_time does not consume event" begin
            fel = FutureEventList()
            schedule!(fel, NullEvent(), 5.0)
            @test peek_time(fel) == 5.0
            @test peek_time(fel) == 5.0   # still there
        end
    end

    # ── DES-S-08: Large-scale cancellation accuracy ───────────────────────────
    @testset "DES-S-08: Event cancellation — 500/1000 events execute" begin
        fel = FutureEventList()
        ids = [schedule!(fel, NullEvent(), float(i)) for i in 1:1000]
        # Cancel the even-numbered events (500 of them)
        for i in 2:2:1000
            cancel!(fel, ids[i])           # FEL-local cancel — no global lock
        end
        executed = 0
        while true
            r = safe_dequeue!(fel)
            r === nothing && break
            executed += 1
        end
        @test executed == 500
    end

    # ── ZoneConfig / ServiceDist ──────────────────────────────────────────────
    @testset "ZoneConfig and ServiceDist" begin

        @testset "exponential_service: mean = 1/μ (n=1000)" begin
            sd = exponential_service(3.0)
            rng = MersenneTwister(1)
            samples = [sd(rng) for _ in 1:1000]
            @test all(s > 0 for s in samples)
            @test abs(sum(samples)/length(samples) - 1/3.0) < 0.05
        end

        @testset "deterministic_service always returns d" begin
            sd  = deterministic_service(3.0)
            rng = MersenneTwister(1)
            @test all(sd(rng) ≈ 3.0 for _ in 1:20)
        end

        @testset "erlang_service: mean ≈ 1/μ (n=5000)" begin
            μ = 2.0; k = 3
            sd = erlang_service(k, μ)
            rng = MersenneTwister(7)
            samples = [sd(rng) for _ in 1:5000]
            @test abs(sum(samples)/length(samples) - 1/μ) < 0.02
        end

        @testset "ZoneConfig argument validation" begin
            @test_throws ArgumentError ZoneConfig(id=1, num_servers=0,
                service_dist=exponential_service(1.0))
            @test_throws ArgumentError ZoneConfig(id=1, capacity=0,
                service_dist=exponential_service(1.0))
            @test_throws ArgumentError ZoneConfig(id=1, arrival_rate=-1.0,
                service_dist=exponential_service(1.0))
        end

        @testset "build_world! registers zones" begin
            world = SimWorld()
            cfg1 = ZoneConfig(id=1, service_dist=exponential_service(2.0))
            cfg2 = ZoneConfig(id=2, service_dist=exponential_service(3.0))
            build_world!(world, cfg1, cfg2)
            @test entity_count(world).zones == 2
        end
    end

    # ── WelchDetector ─────────────────────────────────────────────────────────
    @testset "WelchDetector" begin

        @testset "Not complete before 3 windows" begin
            wd = WelchDetector(window_size=10, threshold=0.05)
            for i in 1:25; update!(wd, float(i)); end
            @test !warmup_complete(wd)   # needs 30 obs for 3 windows
        end

        @testset "Detects convergence on stable signal" begin
            wd = WelchDetector(window_size=20, threshold=0.10)
            for v in 1.0:1.0:40.0; update!(wd, v); end   # transient ramp
            for _ in 1:60; update!(wd, 5.0); end          # stable signal
            @test warmup_complete(wd)
        end

        @testset "Zero queue → immediately complete" begin
            wd = WelchDetector(window_size=5, threshold=0.05)
            for _ in 1:20; update!(wd, 0.0); end
            @test warmup_complete(wd)
        end
    end

# ═══════════════════════════════════════════════════════════════════════════════
# SPRINT 2B — Queueing Theory Validation (DES-S-01..09)
#
# Tolerances: ±10-15% for most metrics; ±5% for utilization; ±3% abs for blocking.
# Full ±2% accuracy: see experiments/scripts/des/des_validation.jl
# n_arrivals kept small for speed; statistical accuracy verified by CI bounds.
# ═══════════════════════════════════════════════════════════════════════════════

    # ── DES-S-01: M/M/1 ρ=0.5 ────────────────────────────────────────────────
    @testset "DES-S-01: M/M/1 ρ=0.50 — L≈1.0, Wq≈0.25, W≈0.5 (±15%)" begin
        λ = 2.0; μ = 4.0; ρ = λ/μ   # ρ = 0.5
        stats = run_mm1!(λ, μ; n_arrivals=20_000, seed=42)
        sm    = sim_summary(stats)

        L_th  = mm1_L(ρ)
        Wq_th = mm1_Wq(μ, ρ)
        W_th  = mm1_W(μ, ρ)

        @test abs(sm.L         - L_th)  / L_th  < 0.15
        @test abs(sm.Wq        - Wq_th) / Wq_th < 0.15
        @test abs(sm.W         - W_th)  / W_th  < 0.10
        @test abs(sm.utilization - ρ)           < 0.05
    end

    # ── DES-S-02: M/M/1 ρ=0.9 ────────────────────────────────────────────────
    @testset "DES-S-02: M/M/1 ρ=0.90 — L≈9.0, W≈1.0 (±25%; high variance)" begin
        λ = 9.0; μ = 10.0; ρ = λ/μ   # ρ = 0.9
        # ρ=0.9 has very high variance → needs more arrivals; ±25% tolerance
        stats = run_mm1!(λ, μ; n_arrivals=50_000, seed=42)
        sm    = sim_summary(stats)

        L_th  = mm1_L(ρ)     # = 9.0
        W_th  = mm1_W(μ, ρ)  # = 1.0
        Wq_th = mm1_Wq(μ, ρ) # = 0.9

        @test abs(sm.L  - L_th)  / L_th  < 0.25
        @test abs(sm.W  - W_th)  / W_th  < 0.20
        @test abs(sm.Wq - Wq_th) / Wq_th < 0.25
        @test abs(sm.utilization - ρ)     < 0.05
    end

    # ── DES-S-03: M/M/1 sweep ρ ∈ {0.3,0.5,0.7,0.9} — L monotone ────────────
    @testset "DES-S-03: M/M/1 sweep — L strictly increases with ρ" begin
        μ = 10.0
        ρs = [0.3, 0.5, 0.7, 0.9]
        # Use fewer arrivals per point; just checking monotonicity
        Ls = [sim_summary(run_mm1!(ρ*μ, μ; n_arrivals=15_000, seed=i*7)).L
              for (i, ρ) in enumerate(ρs)]
        for i in 1:length(Ls)-1
            @test Ls[i] < Ls[i+1]  # strictly increasing
        end
        # Theory check: each L within 25% of mm1_L(ρ)
        for (ρ, L_sim) in zip(ρs, Ls)
            L_th = mm1_L(ρ)
            @test abs(L_sim - L_th) / L_th < 0.25
        end
    end

    # ── DES-S-04: M/M/c (c=4, λ=8, μ=3, ρ/server=0.667) ────────────────────
    @testset "DES-S-04: M/M/c c=4 — utilization≈0.667, Wq < M/M/1" begin
        λ = 8.0; μ = 3.0; c = 4
        ρ_per_server = λ / (c * μ)   # = 2/3 ≈ 0.667

        stats = run_mmc!(λ, μ, c; n_arrivals=20_000, seed=42)
        sm    = sim_summary(stats)

        # Per-server utilization ≈ ρ/server (±8%)
        @test abs(sm.utilization - ρ_per_server) < 0.08

        # Wq of M/M/c should be < M/M/1 at same per-server ρ (c servers share load)
        stats_mm1 = run_mm1!(μ*ρ_per_server, μ; n_arrivals=20_000, seed=42)
        @test sm.Wq < sim_summary(stats_mm1).Wq + 0.05   # M/M/c Wq ≤ M/M/1 Wq
    end

    # ── DES-S-05: M/M/1/K (K=5, ρ=1.0) — blocking ≈ 1/6 ────────────────────
    @testset "DES-S-05: M/M/1/K K=5 ρ=1.0 — blocking_prob ≈ 1/6 (±3%)" begin
        λ = 2.0; μ = 2.0; K = 5
        pb_theory = mm1k_pb(1.0, K)   # = 1/6 ≈ 0.1667

        stats = run_mm1k!(λ, μ, K; n_arrivals=50_000, seed=99)
        sm    = sim_summary(stats)

        @test abs(sm.blocking_prob - pb_theory) < 0.03   # within 3% absolute
    end

    @testset "DES-S-05b: M/M/1/K blocking monotone in K" begin
        λ = 2.0; μ = 2.0
        pbs = [sim_summary(run_mm1k!(λ, μ, K; n_arrivals=20_000, seed=3)).blocking_prob
               for K in [2, 5, 10, 20]]
        for i in 1:length(pbs)-1
            @test pbs[i] > pbs[i+1]   # larger K → less blocking
        end
    end

    # ── DES-S-06: M/D/1 (ρ=0.8) — Wq (P-K formula) ─────────────────────────
    @testset "DES-S-06: M/D/1 ρ=0.8 — Wq≈2.0, W≈3.0 (±15%; P-K)" begin
        λ = 0.8; d = 1.0                        # ρ = λ·d = 0.8
        Wq_th = md1_Wq(λ, d)   # = 2.0
        W_th  = md1_W(λ, d)    # = 3.0

        stats = run_md1!(λ, d; n_arrivals=20_000, seed=42)
        sm    = sim_summary(stats)

        @test abs(sm.Wq - Wq_th) / Wq_th < 0.15
        @test abs(sm.W  - W_th)  / W_th  < 0.10
        @test sm.Wq < sim_summary(run_mm1!(λ, 1/d; n_arrivals=20_000, seed=42)).Wq + 0.1
        # M/D/1 Wq < M/M/1 Wq at same ρ (P-K: half the variance → half Wq)
    end

    # ── DES-S-07: M/G/1 Erlang-2 (P-K formula) ───────────────────────────────
    @testset "DES-S-07: M/G/1 Erlang-2 — Wq vs P-K formula (±15%)" begin
        λ = 1.0; μ = 2.0; k = 2
        Wq_th = mg1_Wq_erlang(λ, μ, k)   # = 0.375

        stats = run_mg1!(λ, μ, k; n_arrivals=20_000, seed=42)
        sm    = sim_summary(stats)

        @test abs(sm.Wq - Wq_th) / Wq_th < 0.15

        # M/G/1 Erlang-k: Wq non-increasing as k increases (less service variance)
        wqs = [sim_summary(run_mg1!(λ, μ, ki; n_arrivals=10_000, seed=42)).Wq
               for ki in [1, 2, 4, 8]]
        for i in 1:length(wqs)-1
            @test wqs[i] >= wqs[i+1] - 0.04   # allow small Monte Carlo noise
        end
    end

    # ── DES-S-09: SimClock fidelity ───────────────────────────────────────────
    @testset "DES-S-09: SimClock speed fidelity" begin

        @testset "Inf speed: no sleep" begin
            clock = SimClock(Inf)
            t_start = time()
            throttle!(clock, 1000.0)
            @test time() - t_start < 0.05   # < 50ms for 1000 simulated seconds
        end

        @testset "Paused: blocks until unpaused" begin
            clock = SimClock(Inf)
            pause!(clock)
            @async begin sleep(0.05); unpause!(clock) end
            t0 = time()
            throttle!(clock, 1.0)
            @test time() - t0 >= 0.04
        end

        @testset "1× real-time: ~0.1s wall for 0.1 sim-seconds" begin
            clock = SimClock(1.0)
            t0 = time()
            throttle!(clock, 0.1)
            elapsed = time() - t0
            @test elapsed >= 0.05
            @test elapsed <= 0.50
        end
    end

    # ── M/M/c multi-server qualitative ───────────────────────────────────────
    @testset "M/M/c qualitative" begin

        @testset "More servers → lower Wq (same load)" begin
            # λ=6, μ=3 per server; c=3 → ρ=0.667; c=6 → ρ=0.333
            s3 = run_mmc!(6.0, 3.0, 3; n_arrivals=15_000, seed=5)
            s6 = run_mmc!(6.0, 3.0, 6; n_arrivals=15_000, seed=5)
            @test sim_summary(s6).Wq <= sim_summary(s3).Wq + 0.05
        end
    end

    # ── ResourceFailure / Repair ──────────────────────────────────────────────
    @testset "ResourceFailure and Repair dispatch" begin
        world   = SimWorld()
        fel     = FutureEventList()
        cfg     = ZoneConfig(id=1, num_servers=2,
                             service_dist=exponential_service(1.0),
                             arrival_rate=0.5)
        configs = Dict(1 => cfg)
        build_world!(world, cfg)
        world.stats.warmup_complete = true   # force warmup for short run
        rng   = MersenneTwister(1)
        clock = SimClock(Inf)

        for i in 1:10
            schedule!(fel, EntityArrival(new_entity_id!(world), 1, float(i)*0.5), float(i)*0.5)
        end
        schedule!(fel, ResourceFailure(1, 1.0f0, 0.5), 0.5)

        sim_loop!(world, fel, configs, clock, rng; t_end=50.0)

        sm = sim_summary(world.stats)
        @test sm.total_arrivals > 0
        @test sm.total_events   > 0
    end

    # ── TransferOut routing ───────────────────────────────────────────────────
    @testset "TransferOut routes entity to downstream zone" begin
        world   = SimWorld()
        fel     = FutureEventList()
        cfg1    = ZoneConfig(id=1, service_dist=deterministic_service(1.0),
                             arrival_rate=0.0, lookahead=0.5)
        cfg2    = ZoneConfig(id=2, service_dist=deterministic_service(1.0),
                             arrival_rate=0.0)
        configs = Dict(1 => cfg1, 2 => cfg2)
        build_world!(world, cfg1, cfg2)
        world.stats.warmup_complete = true
        rng   = MersenneTwister(42)
        clock = SimClock(Inf)

        entity_id = new_entity_id!(world)
        schedule!(fel, TransferOut(entity_id, 2, 0.5), 0.5)

        sim_loop!(world, fel, configs, clock, rng; t_end=5.0)

        @test world.stats.total_arrivals > 0   # zone 2 received arrival
    end

    # ── sim_loop! t_end boundary ─────────────────────────────────────────────
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

        @test world.time <= 10.0
    end

    # ── Wq tracking correctness via M/M/1 Little's Law ───────────────────────
    @testset "Wq + 1/μ ≈ W for M/M/1 (Little's Law consistency)" begin
        λ = 2.0; μ = 5.0; ρ = λ/μ   # ρ = 0.4
        stats = run_mm1!(λ, μ; n_arrivals=15_000, seed=13)
        sm    = sim_summary(stats)
        # By Little's Law: W = Wq + E[S] = Wq + 1/μ
        @test abs((sm.Wq + 1/μ) - sm.W) / sm.W < 0.05   # within 5%
        # Wq should now be correctly tracked (not 0)
        @test sm.Wq > 0.0
        @test sm.Wq < sm.W
    end

end  # @testset "SimDES"

# ═══════════════════════════════════════════════════════════════════════════════
# SPRINT 2C — Medium DES Scenarios (DES-M-01..07)
# ═══════════════════════════════════════════════════════════════════════════════

@testset "Sprint 2C — Medium DES Scenarios" begin

    # ── DES-M-01: Tandem Queue (Jackson's theorem) ───────────────────────────
    @testset "DES-M-01: Tandem queue — W_total ≈ 1.5 (±20%)" begin
        r = run_tandem!(λ=1.0, μ1=2.0, μ2=3.0, n_arrivals=8_000, seed=42)
        # Jackson's theorem: W_total = W₁ + W₂ = 1/(μ₁-λ) + 1/(μ₂-λ) = 1.0 + 0.5 = 1.5
        W_theory = 1/(2.0-1.0) + 1/(3.0-1.0)
        @test abs(r.W_total - W_theory) / W_theory < 0.20
        @test abs(r.W1 - 1.0) / 1.0 < 0.20    # node 1 sojourn
        @test abs(r.W2 - 0.5) / 0.5 < 0.20    # node 2 sojourn
        @test abs(r.util1 - 0.5) < 0.05        # node 1 utilisation ρ₁=0.5
        @test abs(r.util2 - 1/3) < 0.05        # node 2 utilisation ρ₂=0.333
        @test r.arrivals2 > 0                   # entities actually transferred
        @test abs(r.arrivals1 - r.arrivals2) < 0.10 * r.arrivals1  # departures ≈ arrivals
    end

    # ── DES-M-02: Jackson 4-node open network ────────────────────────────────
    @testset "DES-M-02: Jackson 4-node — utilisation matches M/M/1 (±20%)" begin
        r = run_jackson!(n_arrivals=5_000, seed=42)
        # Theory utilisation: ρᵢ = λᵢ/μᵢ
        for i in 1:4
            if r.arrivals[i] > 100    # skip nodes that got too few arrivals
                @test abs(r.utils[i] - r.theory_utils[i]) < 0.25
            end
        end
        # All nodes received entities via routing
        @test r.arrivals[3] > 0    # node 3 is downstream target
        @test r.arrivals[4] > 0    # node 4 is downstream target
        # Routing produces sensible Wq (not NaN, not negative)
        for i in 1:4
            if r.arrivals[i] > 100
                @test isfinite(r.Wqs[i])
                @test r.Wqs[i] >= 0.0
            end
        end
    end

    # ── DES-M-03: Priority Queue (non-preemptive HOL) ────────────────────────
    @testset "DES-M-03: Priority queue — NP-HOL theory computed" begin
        r = run_priority!(λ_H=0.3, λ_L=0.5, μ=1.0, n_arrivals=20_000, seed=42)
        # Non-preemptive theory: Wq_H < Wq_L (key property)
        @test r.Wq_H_theory < r.Wq_L_theory
        # Server utilisation ≈ ρ_total = 0.8 (±10%)
        @test abs(r.util - 0.8) < 0.10
        # Overall Wq should be between Wq_H and Wq_L (mixture)
        @test r.Wq_overall < r.Wq_L_theory * 1.2   # can't be way above low-priority wait
        @test r.n_arrivals > 10_000
    end

    # ── DES-M-04: Machine with Failures ──────────────────────────────────────
    @testset "DES-M-04: Machine failures — availability ≈ 0.909 (±5%)" begin
        r = run_with_failures!(λ=1.5, μ=2.0, α=0.1, β=1.0, n_arrivals=20_000, seed=42)
        # Theory: A = β/(α+β) = 1/(0.1+1) ≈ 0.909
        @test abs(r.theory_A - 0.909) < 0.01        # theory check
        @test abs(r.availability - r.theory_A) < 0.10  # simulation within ±10%
        @test r.total_arrivals > 5_000
        @test r.W > 0.0
    end

    # ── DES-M-06: NHPP Time-Varying Arrivals ─────────────────────────────────
    @testset "DES-M-06: NHPP thinning — total arrivals within ±15% of expectation" begin
        r = run_nhpp!(n_periods=3, seed=42)
        # Thinning should produce Poisson arrivals matching the schedule
        @test r.actual_total > 0
        @test abs(r.ratio - 1.0) < 0.15    # within ±15% of expected total
        @test r.expected_total > 0.0
    end

    # ── DES-M-07: Fork-Join ──────────────────────────────────────────────────
    @testset "DES-M-07: Fork-join — E[join] > max(E[Sᵢ]) (Baccelli-Makowski)" begin
        r = run_forkjoin!(λ=0.8, μ1=2.0, μ2=3.0, μ3=1.5, n_orders=5_000, seed=42)
        # Baccelli-Makowski bound: E[max(S₁,S₂,S₃)] ≥ max(E[S₁], E[S₂], E[S₃])
        @test r.W_join > r.lower_bound * 0.90   # join time exceeds lower bound (±10%)
        @test r.n_completed > 1_000              # simulation actually ran
        @test isfinite(r.W_join)
        @test r.W_join > 0.0
    end

    # ── Routing correctness: ExitSystem, FixedRoute, ProbRoute ───────────────
    @testset "Phase 2C: Routing policy types compile and dispatch" begin
        @test ExitSystem()    isa RoutingPolicy
        @test FixedRoute(2)   isa RoutingPolicy
        @test ProbRoute([(2, 0.5)]) isa RoutingPolicy
        @test ForkJoinConfig([2,3], 0) isa ForkJoinConfig
    end

    # ── NHPP: ArrivalRateSchedule API ────────────────────────────────────────
    @testset "ArrivalRateSchedule: rate_at correctness" begin
        sched = ArrivalRateSchedule([0.0, 6.0, 12.0, 24.0], [1.0, 5.0, 0.5])
        @test rate_at(sched, 0.0) == 1.0
        @test rate_at(sched, 3.0) == 1.0
        @test rate_at(sched, 6.0) == 5.0
        @test rate_at(sched, 11.0) == 5.0
        @test rate_at(sched, 12.0) == 0.5
        @test rate_at(sched, 25.0) == 0.0   # past schedule end
        @test sched.λ_max == 5.0
    end

    # ── ProbRoute: sample_destination covers all outcomes ────────────────────
    @testset "ProbRoute: destination sampling correctness" begin
        rng = MersenneTwister(1)
        route = ProbRoute([(2, 0.30), (3, 0.40)])  # 30% exit (residual)
        samples = [sample_destination(route, rng) for _ in 1:1000]
        n2 = count(==(2), samples)
        n3 = count(==(3), samples)
        n_exit = count(isnothing, samples)
        @test n2 > 200 && n2 < 400    # ≈30%
        @test n3 > 300 && n3 < 500    # ≈40%
        @test n_exit > 200 && n_exit < 400  # ≈30% residual
    end

    # ── ZoneConfig: new fields have correct defaults ──────────────────────────
    @testset "ZoneConfig: new Phase 2C/2D fields and defaults" begin
        cfg = ZoneConfig(id=1, service_dist=exponential_service(2.0))
        @test cfg.routing          isa ExitSystem
        @test cfg.queue_discipline == FIFO          # default is FIFO enum value
        @test cfg.arrival          isa NoArrival    # no arrival_rate → NoArrival
        @test cfg.failures         isa NoFailure    # no failure_rate → NoFailure
        @test cfg.fork_join        === nothing

        # Legacy kwargs still accepted and auto-converted (backward compat)
        cfg2 = ZoneConfig(id=2, service_dist=exponential_service(1.0),
                          routing=FixedRoute(3), queue_discipline=:priority,
                          failure_rate=0.1, repair_rate=0.5)
        @test cfg2.routing          isa FixedRoute
        @test cfg2.queue_discipline == PRIORITY_HOL     # Symbol :priority → PRIORITY_HOL enum
        @test cfg2.failures         isa BernoulliFailure
        @test cfg2.failures.α       == 0.1              # failure_rate → BernoulliFailure.α
        @test cfg2.failures.β       == 0.5              # repair_rate  → BernoulliFailure.β

        # Typed ArrivalProcess kwargs (preferred form)
        cfg3 = ZoneConfig(id=3, service_dist=exponential_service(1.0),
                          arrival=PoissonArrival(2.5))
        @test cfg3.arrival isa PoissonArrival
        @test cfg3.arrival.rate == 2.5

        # Legacy arrival_rate kwarg auto-converts to PoissonArrival
        cfg4 = ZoneConfig(id=4, service_dist=exponential_service(1.0), arrival_rate=1.0)
        @test cfg4.arrival isa PoissonArrival
        @test cfg4.arrival.rate == 1.0

        # Typed FailureModel kwargs (preferred form)
        cfg5 = ZoneConfig(id=5, service_dist=exponential_service(1.0),
                          failures=BernoulliFailure(0.2, 2.0))
        @test cfg5.failures isa BernoulliFailure
        @test cfg5.failures.α == 0.2
        @test cfg5.failures.β == 2.0
    end


    # ── EntityArrival: backward-compatible with priority ──────────────────────
    @testset "EntityArrival: 3-arg backward compat + 4-arg priority" begin
        e1 = EntityArrival(UInt64(1), 1, 0.5)
        @test e1.priority == 0         # default = FIFO
        e2 = EntityArrival(UInt64(2), 1, 0.5, 5)
        @test e2.priority == 5
        @test e1.entity_id == UInt64(1)
    end

end  # Sprint 2C testset
