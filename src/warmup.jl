"""
    warmup.jl — Welch Warmup Detector

Detects when a DES simulation has reached steady state using
Welch's moving-average method. Statistics are only collected
after the warmup period ends.

Welch's method (1983):
1. Divide the run into windows of size `w`
2. For each window compute mean queue length
3. When variance across consecutive windows drops below threshold,
   declare steady state

Design ref: §2A-10 (warmup detection requirement)
Reference: Welch, P.D. (1983). The statistical analysis of simulation results.
"""

"""
    WelchDetector

Online steady-state detector using Welch's moving-average method.

# Fields
- `window_size::Int`: number of observation points per window
- `threshold::Float64`: relative variance threshold to declare steady state
- `_window_buffer::Vector{Float64}`: current window observations
- `_window_means::Vector{Float64}`: mean of each completed window
- `complete::Bool`: true when steady state detected

# Usage
```julia
wd = WelchDetector(window_size=200, threshold=0.05)
for each event in simulation:
    update!(wd, queue_length_observation)
    if warmup_complete(wd)
        # start collecting stats
    end
```
"""
mutable struct WelchDetector
    window_size    :: Int
    threshold      :: Float64
    _buffer        :: Vector{Float64}
    _window_means  :: Vector{Float64}
    complete       :: Bool
end

WelchDetector(; window_size::Int = 200, threshold::Float64 = 0.05) =
    WelchDetector(window_size, threshold,
                  sizehint!(Float64[], window_size),
                  Float64[],
                  false)

"""
    warmup_complete(wd) -> Bool

Return `true` if steady state has been detected.
"""
warmup_complete(wd::WelchDetector) = wd.complete

"""
    update!(wd, observation)

Add one observation (e.g., queue length at an event time).
Checks for steady state after each completed window.
"""
function update!(wd::WelchDetector, obs::Float64)
    wd.complete && return   # already done

    push!(wd._buffer, obs)

    if length(wd._buffer) >= wd.window_size
        # Completed a window — compute its mean
        window_mean = sum(wd._buffer) / length(wd._buffer)
        push!(wd._window_means, window_mean)
        empty!(wd._buffer)

        # Need at least 3 windows to detect convergence
        if length(wd._window_means) >= 3
            n     = length(wd._window_means)
            means = wd._window_means
            # Relative coefficient of variation of the last 3 window means
            last3 = means[end-2:end]
            μ̄     = sum(last3) / 3
            if μ̄ > 1e-10
                cv = sqrt(sum((m - μ̄)^2 for m in last3) / 3) / μ̄
                if cv < wd.threshold
                    wd.complete = true
                end
            else
                wd.complete = true   # zero queue → already at steady state
            end
        end
    end
end
