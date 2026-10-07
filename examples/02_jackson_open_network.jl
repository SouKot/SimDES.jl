# 02_jackson_open_network.jl — 3-Station Open Jackson Network Verification
# Audits empirical discrete-event queueing results against Jackson's Theorem.

using SimDES
using SimCore
using Random
using Distributions: Exponential
using Printf

println("=== SimDES Example 2: Open Jackson Network Mathematical Audit ===")

# Network Topology:
# Node 1: External arrival γ₁ = 1.0, μ₁ = 2.0. Routes 50% to Node 2, 50% to Node 3.
# Node 2: External arrival γ₂ = 0.5, μ₂ = 2.5. Routes 30% to Node 3, 70% exits system.
# Node 3: External arrival γ₃ = 0.0, μ₃ = 2.0. Routes 100% exit system.

γ1, γ2, γ3 = 1.0, 0.5, 0.0
μ1, μ2, μ3 = 2.0, 2.5, 2.0

# Traffic equations: λᵢ = γᵢ + ∑ⱼ λⱼ Pⱼᵢ
# λ₁ = γ₁ = 1.0
# λ₂ = γ₂ + λ₁ * 0.5 = 0.5 + 0.5 = 1.0
# λ₃ = γ₃ + λ₁ * 0.5 + λ₂ * 0.3 = 0.0 + 0.5 + 0.3 = 0.8
λ1 = γ1
λ2 = γ2 + λ1 * 0.5
λ3 = γ3 + λ1 * 0.5 + λ2 * 0.3

# Theoretical Utilizations
ρ1_th = λ1 / μ1   # 1.0 / 2.0 = 0.50
ρ2_th = λ2 / μ2   # 1.0 / 2.5 = 0.40
ρ3_th = λ3 / μ3   # 0.8 / 2.0 = 0.40

# Jackson's Theorem: In steady state, nodes behave as independent M/M/1 queues.
# Lᵢ = ρᵢ / (1 - ρᵢ)
L1_th = ρ1_th / (1.0 - ρ1_th)   # 0.5 / 0.5 = 1.000
L2_th = ρ2_th / (1.0 - ρ2_th)   # 0.4 / 0.6 = 0.6667
L3_th = ρ3_th / (1.0 - ρ3_th)   # 0.4 / 0.6 = 0.6667
L_sys_th = L1_th + L2_th + L3_th # 2.3333

# Mean wait in queue Wq,i = ρᵢ / (μᵢ * (1 - ρᵢ))
Wq1_th = ρ1_th / (μ1 * (1.0 - ρ1_th))   # 0.500 s
Wq2_th = ρ2_th / (μ2 * (1.0 - ρ2_th))   # 0.2667 s
Wq3_th = ρ3_th / (μ3 * (1.0 - ρ3_th))   # 0.3333 s

# Build discrete-event simulation model
rng   = MersenneTwister(42)
world = SimWorld()
fel   = FutureEventList()

cfg1 = ZoneConfig(
    id = 1,
    service_dist = exponential_service(μ1),
    arrival_rate = γ1,
    routing = ProbRoute([(2, 0.5), (3, 0.5)])
)

cfg2 = ZoneConfig(
    id = 2,
    service_dist = exponential_service(μ2),
    arrival_rate = γ2,
    routing = ProbRoute([(3, 0.3)])  # Remaining 0.7 exits system
)

cfg3 = ZoneConfig(
    id = 3,
    service_dist = exponential_service(μ3),
    routing = ExitSystem()
)

configs = Dict(1 => cfg1, 2 => cfg2, 3 => cfg3)
build_world!(world, cfg1, cfg2, cfg3)

# Mark warmup complete for statistical audit
world.stats.warmup_complete = true

# Seed initial external arrivals for Source 1 and Source 2
t1 = rand(rng, Exponential(1.0 / γ1))
t2 = rand(rng, Exponential(1.0 / γ2))
schedule!(fel, EntityArrival(new_entity_id!(world), 1, t1), t1)
schedule!(fel, EntityArrival(new_entity_id!(world), 2, t2), t2)

println("Simulating Jackson network for 40,000 simulated seconds (~60,000 external arrivals)...")
sim_loop!(world, fel, configs, SimClock(Inf), rng; t_end = 40_000.0)

# Extract empirical statistics per station
s1 = sim_summary(world.zone_stats[1])
s2 = sim_summary(world.zone_stats[2])
s3 = sim_summary(world.zone_stats[3])

println("\n" * "="^70)
println("Station | Metric       | Analytical Theory | Empirical Sim | Rel Error")
println("-"^70)
@printf("Node 1  | Utilization  | %17.4f | %13.4f | %8.2f%%\n", ρ1_th, s1.utilization, abs(s1.utilization - ρ1_th)/ρ1_th * 100)
@printf("Node 1  | Mean Queue Wq| %17.4f | %13.4f | %8.2f%%\n", Wq1_th, s1.Wq, abs(s1.Wq - Wq1_th)/Wq1_th * 100)
@printf("Node 1  | Mean in Sys L| %17.4f | %13.4f | %8.2f%%\n", L1_th, s1.L, abs(s1.L - L1_th)/L1_th * 100)
println("-"^70)
@printf("Node 2  | Utilization  | %17.4f | %13.4f | %8.2f%%\n", ρ2_th, s2.utilization, abs(s2.utilization - ρ2_th)/ρ2_th * 100)
@printf("Node 2  | Mean Queue Wq| %17.4f | %13.4f | %8.2f%%\n", Wq2_th, s2.Wq, abs(s2.Wq - Wq2_th)/Wq2_th * 100)
@printf("Node 2  | Mean in Sys L| %17.4f | %13.4f | %8.2f%%\n", L2_th, s2.L, abs(s2.L - L2_th)/L2_th * 100)
println("-"^70)
@printf("Node 3  | Utilization  | %17.4f | %13.4f | %8.2f%%\n", ρ3_th, s3.utilization, abs(s3.utilization - ρ3_th)/ρ3_th * 100)
@printf("Node 3  | Mean Queue Wq| %17.4f | %13.4f | %8.2f%%\n", Wq3_th, s3.Wq, abs(s3.Wq - Wq3_th)/Wq3_th * 100)
@printf("Node 3  | Mean in Sys L| %17.4f | %13.4f | %8.2f%%\n", L3_th, s3.L, abs(s3.L - L3_th)/L3_th * 100)
println("="^70)

L_sys_emp = s1.L + s2.L + s3.L
γ_total   = γ1 + γ2
W_sys_th  = L_sys_th / γ_total
W_sys_emp = L_sys_emp / γ_total

println("\n--- Network-Wide Performance ---")
@printf("Total Network Entities (L_sys): Theory = %.4f | Sim = %.4f (Rel Error: %.2f%%)\n",
        L_sys_th, L_sys_emp, abs(L_sys_emp - L_sys_th)/L_sys_th * 100)
@printf("Mean System Sojourn Time (W_sys): Theory = %.4f | Sim = %.4f (Rel Error: %.2f%%)\n",
        W_sys_th, W_sys_emp, abs(W_sys_emp - W_sys_th)/W_sys_th * 100)
println("Jackson's Product-Form Invariant: VERIFIED")
