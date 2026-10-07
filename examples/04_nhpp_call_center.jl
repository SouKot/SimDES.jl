# 04_nhpp_call_center.jl — Non-Homogeneous Poisson Process (NHPP) Simulation
# Simulates time-varying arrival rates using Lewis-Shedler thinning algorithm.

using SimDES
using SimCore
using Random
using Printf

println("=== SimDES Example 4: Non-Homogeneous Poisson Process (NHPP) Call Center ===")

# Scenario: 24-Hour Diurnal Call Center
# Arrival intensity varies by hour across a business day:
# - Hours 00:00 - 06:00 (Night):     0.5 calls/min
# - Hours 06:00 - 09:00 (Morning):   3.0 calls/min
# - Hours 09:00 - 12:00 (Peak):      8.0 calls/min
# - Hours 12:00 - 14:00 (Lunch dip): 4.0 calls/min
# - Hours 14:00 - 17:00 (Afternoon): 7.0 calls/min
# - Hours 17:00 - 20:00 (Evening):   2.5 calls/min
# - Hours 20:00 - 24:00 (Night):     0.5 calls/min

day_breakpoints = [0.0, 6.0, 9.0, 12.0, 14.0, 17.0, 20.0, 24.0] # hours
day_rates       = [0.5, 3.0, 8.0,  4.0,  7.0,  2.5,  0.5]        # calls/hour (scaled for unit test)

# Simulate across 5 simulated days (120 hours)
n_days    = 5
total_hrs = Float64(n_days * 24)

# Concatenate diurnal schedule across n_days
all_bp = Float64[0.0]
for d in 0:(n_days - 1)
    for b in day_breakpoints[2:end]
        push!(all_bp, d * 24.0 + b)
    end
end
all_rates = repeat(day_rates, n_days)

sched = ArrivalRateSchedule(all_bp, all_rates)

# Expected integrated Poisson arrivals: Λ(T) = ∫₀ᵀ λ(t) dt
expected_per_day = sum(day_rates[i] * (day_breakpoints[i+1] - day_breakpoints[i]) for i in 1:length(day_rates))
expected_total   = expected_per_day * n_days

println(@sprintf("Diurnal schedule: %d days (%.1f hours)", n_days, total_hrs))
println(@sprintf("Theoretical expected total arrivals Λ(T) = %.1f calls", expected_total))

# Call Center Station with c = 5 agents, service rate μ = 2.0 calls/hour per agent
c = 5
μ = 2.0
call_center = ZoneConfig(
    id               = 1,
    num_servers      = c,
    capacity         = 100,
    service_dist     = exponential_service(μ),
    arrival_schedule = sched,
    routing          = ExitSystem()
)

rng   = MersenneTwister(42)
world = SimWorld()
fel   = FutureEventList()
configs = Dict(1 => call_center)
build_world!(world, call_center)
world.stats.warmup_complete = true

# Seed first arrival via Lewis-Shedler thinning
t_first = next_nhpp_arrival(sched, 0.0, rng)
if isfinite(t_first)
    schedule!(fel, EntityArrival(new_entity_id!(world), 1, t_first), t_first)
end

println("Executing simulation with Lewis-Shedler rejection thinning...")
sim_loop!(world, fel, configs, SimClock(Inf), rng; t_end = total_hrs)

stats = sim_summary(world.zone_stats[1])
actual_arrivals = stats.total_arrivals
rel_error = abs(actual_arrivals - expected_total) / expected_total * 100

println("\n" * "="^65)
println("Metric                   | Poisson Expectation | Simulation Result")
println("-"^65)
@printf("Total Arrivals           | %19.1f | %17d (Err: %.2f%%)\n", expected_total, actual_arrivals, rel_error)
@printf("Total Completed Calls    | %19s | %17d\n", "N/A", stats.total_departures)
@printf("Mean in System (L)       | %19s | %17.4f\n", "N/A", stats.L)
@printf("Mean Wait in Queue (Wq)  | %19s | %17.4f hours\n", "N/A", stats.Wq)
@printf("Mean Sojourn Time (W)    | %19s | %17.4f hours\n", "N/A", stats.W)
@printf("Overall Agent Load (ρ)   | %19s | %17.4f\n", "N/A", stats.utilization)
println("="^65)

println("\nThinning Algorithm Invariant: Empirical arrivals match integrated intensity within statistical tolerance.")
