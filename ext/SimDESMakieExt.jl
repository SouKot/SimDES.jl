module SimDESMakieExt

using SimDES
using Makie

import SimDES: simplot, animate_sim, plot_queue_history, plot_gantt

# Helper to draw 2D directed arrows across Makie version boundaries without deprecation warnings
function _render_arrow2d!(ax, sx, sy, dx, dy; color = :gray50, linewidth = 2.0, tipsize = 14.0)
    if isdefined(Makie, :arrows2d!)
        Makie.arrows2d!(ax, sx, sy, dx, dy;
                        color = color, shaftwidth = linewidth,
                        tipwidth = tipsize, tiplength = tipsize)
    else
        Makie.arrows!(ax, sx, sy, dx, dy;
                      color = color, linewidth = linewidth,
                      arrowsize = tipsize)
    end
end

const _PALETTES = Dict{Symbol, NamedTuple}(
    :hermes_dark => (
        bg = RGBf(0.09, 0.09, 0.11),
        text = :white,
        border = RGBf(0.22, 0.25, 0.30),
        source = RGBf(0.20, 0.65, 0.35),
        server = RGBf(0.25, 0.45, 0.85),
        conveyor = RGBf(0.95, 0.55, 0.15),
        sink = RGBf(0.60, 0.30, 0.70)
    ),
    :hermes_light => (
        bg = RGBf(0.95, 0.96, 0.98),
        text = RGBf(0.12, 0.12, 0.15),
        border = RGBf(0.80, 0.82, 0.86),
        source = RGBf(0.15, 0.60, 0.30),
        server = RGBf(0.20, 0.40, 0.80),
        conveyor = RGBf(0.90, 0.50, 0.10),
        sink = RGBf(0.55, 0.25, 0.65)
    ),
    :nord => (
        bg = RGBf(0.18, 0.20, 0.25),
        text = RGBf(0.93, 0.94, 0.96),
        border = RGBf(0.30, 0.34, 0.42),
        source = RGBf(0.64, 0.75, 0.55),
        server = RGBf(0.51, 0.63, 0.76),
        conveyor = RGBf(0.82, 0.53, 0.44),
        sink = RGBf(0.71, 0.56, 0.68)
    )
)

"""
    simplot(configs::Vector{ZoneConfig}; layout=:horizontal, resolution=(900, 450), title="Network Schematic", theme=:hermes_dark) -> Figure

Render a 2D network schematic of discrete-event simulation zones.
Nodes are color-coded by role (source, server, conveyor, exit) with directed flow connections.
"""
function SimDES.simplot(configs::Vector{ZoneConfig};
                        layout::Symbol = :horizontal,
                        resolution::Tuple{Int, Int} = (900, 450),
                        title::String = "Discrete-Event Network Schematic",
                        theme::Symbol = :hermes_dark)
    pal = get(_PALETTES, theme, _PALETTES[:hermes_dark])
    fig = Figure(size = resolution, backgroundcolor = pal.bg)
    ax  = Axis(fig[1, 1], title = title,
               xlabel = "System X (m)", ylabel = "System Y (m)",
               aspect = DataAspect(),
               titlecolor = pal.text, xlabelcolor = pal.text, ylabelcolor = pal.text,
               backgroundcolor = pal.bg)
    hidedecorations!(ax, grid = false)

    n = length(configs)
    if n == 0
        return fig
    end

    # Determine coordinates
    xs = Float64[]
    ys = Float64[]
    if layout == :horizontal
        for i in 1:n
            push!(xs, (i - 1) * 3.0)
            push!(ys, 0.0)
        end
    elseif layout == :grid
        cols = ceil(Int, sqrt(n))
        for i in 1:n
            r = div(i - 1, cols)
            c = mod(i - 1, cols)
            push!(xs, c * 3.5)
            push!(ys, -r * 2.5)
        end
    else
        for i in 1:n
            push!(xs, Float64(i))
            push!(ys, 0.0)
        end
    end

    # Mapping from zone id to index
    id_to_idx = Dict{Int, Int}(configs[i].id => i for i in 1:n)

    # Draw directed connection arrows between adjacent or routed zones
    for i in 1:n
        zc = configs[i]
        target_indices = Int[]
        
        # Check explicit downstream connections
        for d in zc.downstream
            if haskey(id_to_idx, d)
                push!(target_indices, id_to_idx[d])
            end
        end

        # Check routing policy targets
        if zc.routing isa FixedRoute
            dest = zc.routing.to
            if haskey(id_to_idx, dest)
                push!(target_indices, id_to_idx[dest])
            end
        elseif zc.routing isa ProbRoute
            for (dest, _) in zc.routing.choices
                if dest !== nothing && haskey(id_to_idx, dest)
                    push!(target_indices, id_to_idx[dest])
                end
            end
        elseif isempty(zc.downstream) && i < n && !(zc.routing isa ExitSystem)
            push!(target_indices, i + 1)
        end

        for target in unique(target_indices)
            target == i && continue
            x1, y1 = xs[i], ys[i]
            x2, y2 = xs[target], ys[target]
            dx = x2 - x1
            dy = y2 - y1
            dist = sqrt(dx^2 + dy^2)
            if dist > 0.1
                ux, uy = dx / dist, dy / dist
                sx, sy = x1 + ux * 0.8, y1 + uy * 0.4
                ex, ey = x2 - ux * 0.8, y2 - uy * 0.4
                _render_arrow2d!(ax, [sx], [sy], [ex - sx], [ey - sy];
                                 color = :gray50, linewidth = 2.0, tipsize = 14.0)
            end
        end
    end

    # Draw node badges
    box_w = 1.6
    box_h = 0.8
    for i in 1:n
        zc = configs[i]
        cx, cy = xs[i], ys[i]

        # Determine color by zone type from theme palette
        node_color = if !(zc.arrival isa NoArrival)
            pal.source # Arrival Source
        elseif zc.is_conveyor || zc.conveyor_mode != :free_flow
            pal.conveyor # Conveyor
        elseif zc.routing isa ExitSystem
            pal.sink # Sink / Exit
        else
            pal.server # Service Station
        end

        # Node rectangle
        poly!(ax, Rect2f(cx - box_w/2, cy - box_h/2, box_w, box_h),
              color = node_color, strokecolor = pal.border, strokewidth = 1.5)

        # Node labels
        label_text = "Zone $(zc.id)"
        cap_text   = zc.capacity < 99999 ? "Cap: $(zc.capacity)" : "Inf Cap"
        text!(ax, cx, cy + 0.12, text = label_text,
              align = (:center, :center), color = pal.text,
              font = :bold, fontsize = 12)
        text!(ax, cx, cy - 0.18, text = cap_text,
              align = (:center, :center), color = pal.text,
              fontsize = 10)
    end

    return fig
end

SimDES.simplot(configs::ZoneConfig...; kwargs...) =
    SimDES.simplot(collect(configs); kwargs...)

"""
    plot_queue_history(times::AbstractVector{<:Real}, queue_lengths::AbstractVector{<:Integer};
                       title="Queue Occupancy Q(t)", resolution=(850, 400)) -> Figure

Render a step chart of queue length over simulated time.
"""
function SimDES.plot_queue_history(times::AbstractVector{<:Real},
                                   queue_lengths::AbstractVector{<:Integer};
                                   title::String = "Queue Occupancy Q(t)",
                                   resolution::Tuple{Int, Int} = (850, 400),
                                   label::String = "Q(t)")
    fig = Figure(size = resolution)
    ax  = Axis(fig[1, 1], title = title,
               xlabel = "Simulated Time (s)",
               ylabel = "Queue Length (items)")

    if !isempty(times) && !isempty(queue_lengths)
        stairs!(ax, times, queue_lengths, step = :pre,
                color = RGBf(0.18, 0.42, 0.78), linewidth = 2.0, label = label)

        mean_q = sum(queue_lengths) / length(queue_lengths)
        hlines!(ax, [mean_q], color = :crimson, linestyle = :dash,
                linewidth = 1.8, label = "Mean = $(round(mean_q, digits=2))")
        axislegend(ax, position = :rt)
    end

    return fig
end

"""
    plot_gantt(records::AbstractVector{<:NamedTuple};
               title="Server State Schedule (Gantt)",
               resolution=(900, 400)) -> Figure

Render a Gantt chart representing resource state intervals over simulated time.
Each record must provide `(server_id, state, t_start, t_end)` where state is
`:busy`, `:idle`, `:blocked`, or `:failed`.
"""
function SimDES.plot_gantt(records::AbstractVector{<:NamedTuple};
                           title::String = "Server State Schedule (Gantt)",
                           resolution::Tuple{Int, Int} = (900, 400))
    fig = Figure(size = resolution)
    ax  = Axis(fig[1, 1], title = title,
               xlabel = "Simulated Time (s)",
               ylabel = "Resource ID")

    state_colors = Dict(
        :busy    => RGBf(0.20, 0.65, 0.35),  # Green
        :idle    => RGBf(0.85, 0.85, 0.85),  # Light gray
        :blocked => RGBf(0.95, 0.55, 0.15),  # Orange
        :failed  => RGBf(0.85, 0.20, 0.20)   # Red
    )

    server_ids = unique([r.server_id for r in records if haskey(r, :server_id)])
    sort!(server_ids)

    bar_height = 0.5
    for r in records
        c = get(state_colors, get(r, :state, :busy), RGBf(0.5, 0.5, 0.5))
        y = Float64(r.server_id)
        t0 = Float64(r.t_start)
        t1 = Float64(r.t_end)
        dt = max(0.001, t1 - t0)

        poly!(ax, Rect2f(t0, y - bar_height/2, dt, bar_height),
              color = c, strokecolor = :black, strokewidth = 0.5)
    end

    if !isempty(server_ids)
        ax.yticks = (Float64.(server_ids), string.(server_ids))
    end

    # Legend elements
    elements = [
        PolyElement(color = state_colors[:busy], strokecolor = :black),
        PolyElement(color = state_colors[:idle], strokecolor = :black),
        PolyElement(color = state_colors[:blocked], strokecolor = :black),
        PolyElement(color = state_colors[:failed], strokecolor = :black)
    ]
    labels = ["Busy", "Idle", "Blocked", "Failed"]
    Legend(fig[1, 2], elements, labels, "State")

    return fig
end

"""
    animate_sim(trajectory_snapshots::AbstractVector{<:NamedTuple};
                resolution=(800, 500)) -> (Figure, Observable)

Creates an Observable-backed animation canvas for discrete item flow trajectories.
"""
function SimDES.animate_sim(trajectory_snapshots::AbstractVector{<:NamedTuple};
                            resolution::Tuple{Int, Int} = (800, 500))
    fig = Figure(size = resolution)
    ax  = Axis(fig[1, 1], title = "Simulation Flow Animation",
               xlabel = "X (m)", ylabel = "Y (m)", aspect = DataAspect())

    step_idx = Observable(1)

    points = lift(step_idx) do idx
        if isempty(trajectory_snapshots) || idx > length(trajectory_snapshots)
            return Point2f[]
        end
        snap = trajectory_snapshots[idx]
        if haskey(snap, :items)
            return [Point2f(it.x, it.y) for it in snap.items]
        end
        return Point2f[]
    end

    scatter!(ax, points, color = :royalblue, markersize = 14)
    return (fig, step_idx)
end

end # module SimDESMakieExt
