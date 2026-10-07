# 01_mm1_analytical_audit.jl — M/M/1 Closed-Form Mathematical Audit
# Audits empirical discrete-event queueing results against theoretical formulas.

using SimDES
using SimCore
using Printf

println("=== SimDES Example 1: M/M/1 Queue Mathematical Audit ===")

# Parameters: Arrival rate λ = 1.0, Service rate μ = 2.0 (Load factor ρ = 0.50)
λ = 1.0
μ = 2.0
ρ = λ / μ

# Closed-form queueing theory expectations
L_th  = ρ / (1.0 - ρ)           # Mean number in system = 1.0
W_th  = 1.0 / (μ * (1.0 - ρ))   # Mean time in system   = 1.0
Wq_th = ρ / (μ * (1.0 - ρ))     # Mean wait in queue    = 0.5
ρ_th  = ρ                       # Server utilization    = 0.5

# Setup statistics pipeline with fixed warmup exclusion
pipeline = StatsPipeline(warmup = WARMUP_FIXED, warmup_n = 5_000)

println("Executing M/M/1 simulation (100,000 arrivals, 5,000 warmup)...")
_ = run_mm1!(λ, μ; n_arrivals = 100_000, seed = 42, pipeline = pipeline)
stats = sim_summary(pipeline)

# Audit table
println("\n" * "="^65)
println("Metric               | Analytical Theory | Empirical Simulation | Rel Error")
println("-"^65)
@printf("Utilization (ρ)      | %17.4f | %20.4f | %8.2f%%\n", ρ_th, stats.utilization, abs(stats.utilization - ρ_th)/ρ_th * 100)
@printf("Mean Wait in Q (Wq)  | %17.4f | %20.4f | %8.2f%%\n", Wq_th, stats.Wq, abs(stats.Wq - Wq_th)/Wq_th * 100)
@printf("Mean Sojourn (W)     | %17.4f | %20.4f | %8.2f%%\n", W_th, stats.W, abs(stats.W - W_th)/W_th * 100)
@printf("Mean in System (L)   | %17.4f | %20.4f | %8.2f%%\n", L_th, stats.L, abs(stats.L - L_th)/L_th * 100)
println("="^65)

# Verify Little's Law
ok_little, err_little = check_littles_law(stats; tol = 0.05)
println("\nLittle's Law Invariant (L = λ * W): ", ok_little ? "VERIFIED (Holds)" : "VIOLATED")
println("Warmup detection status:            ", stats.warmup_complete ? "Complete" : "Incomplete")
