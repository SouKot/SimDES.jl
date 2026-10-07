# 06_interactive_conveyor_makie.jl — Interactive Conveyor Flow Simulation
# Emulates physical conveyor kinematics (Accumulating ZPA vs Non-Accumulating Rigid Bed)
# with a rich interactive GLMakie GUI, live Observable animation, and SimDES kinematics.

using SimDES
using SimCore
using Random

# Check if Makie / backend is available
const MAKIE_AVAILABLE = try
    @eval using Makie
    true
catch
    false
end

const GLMAKIE_AVAILABLE = try
    @eval using GLMakie: GLMakie
    true
catch
    false
end

println("=== SimDES Example 6: Interactive Conveyor Kinematics (Makie) ===")

if !MAKIE_AVAILABLE
    println("""
[INFO] Makie is not loaded in the active environment.
To run this interactive simulation, load GLMakie:

    using Pkg; Pkg.activate(".")
    using GLMakie
    using SimDES
    include("packages/SimDES/examples/06_interactive_conveyor_makie.jl")
""")
    exit(0)
end

# ── Simulation Data Structures ───────────────────────────────────────────────

mutable struct ConveyorItem
    id::Int
    x::Float64          # Position along conveyor (0.0 to belt_length)
    is_stopped::Bool
end

function run_interactive_conveyor()
    # Physical Belt Parameters
    belt_length   = 10.0   # meters
    nominal_speed = 1.5    # meters/second
    item_size     = 0.6    # meters (box width & height)
    safety_gap    = 0.15   # meters minimum inter-item spacing
    gate_x        = 8.8    # meters (obstruction point)

    # State Observables
    is_accumulating = Observable(true)   # true = Accumulating ZPA, false = Rigid bed
    is_blocked      = Observable(false)  # Obstruction gate status
    is_playing      = Observable(true)   # Play/Pause
    spawn_interval  = Observable(1.2)    # seconds between incoming arrivals
    items_count_str = Observable("0")
    status_str      = Observable("Flowing")
    mode_desc_str   = Observable("MODE: Accumulating Conveyor (ZPA) — Trailing items queue independently")

    # Animation Frame Observables
    belt_offset_obs = Observable(0.0)    # Texture scroll offset
    gate_color_obs  = Observable(RGBf(0.12, 0.65, 0.32)) # Green open, Red blocked
    gate_y_obs      = Observable(1.5)    # Gate barrier arm elevation

    # Item Polygon & Label Observables
    box_polys_obs   = Observable(Rect2f[])
    box_colors_obs  = Observable(RGBf[])
    box_labels_obs  = Observable(Tuple{Point2f, String}[])

    # Simulation Internal State
    items = ConveyorItem[]
    next_id = 1
    spawn_timer = 0.0

    # ── Makie GUI Layout ───────────────────────────────────────────────────────

    fig = Figure(size = (1000, 580))

    # Header Title
    Label(fig[1, 1], "SimDES Conveyor Kinematics Simulation",
          fontsize = 20, font = :bold, halign = :center)
    Label(fig[2, 1], mode_desc_str,
          fontsize = 12, color = :gray40, halign = :center)

    # Top Control Bar (Grid of buttons & toggles)
    ctrl_grid = fig[3, 1] = GridLayout()

    btn_block  = Button(ctrl_grid[1, 1], label = "Toggle Obstruction Gate")
    tog_mode   = Toggle(ctrl_grid[1, 2], active = true)
    lbl_mode   = Label(ctrl_grid[1, 3], "Accumulating (ZPA)")
    btn_play   = Button(ctrl_grid[1, 4], label = "Pause")
    btn_reset  = Button(ctrl_grid[1, 5], label = "Reset Simulation")

    # Rate Slider
    slider_grid = fig[4, 1] = GridLayout()
    Label(slider_grid[1, 1], "Arrival Interval (s):", fontsize = 12, halign = :right)
    sl_interval = Slider(slider_grid[1, 2], range = 0.6:0.1:3.0, startvalue = 1.2)
    lbl_sl_val  = Label(slider_grid[1, 3], "1.2s", fontsize = 12, halign = :left)

    on(sl_interval.value) do val
        spawn_interval[] = val
        lbl_sl_val.text[] = "$(round(val, digits=1))s"
    end

    # HUD Banner
    hud_grid = fig[5, 1] = GridLayout()
    Label(hud_grid[1, 1], lift(c -> "Items on Belt: $c", items_count_str),
          font = :bold, fontsize = 13, halign = :center)
    Label(hud_grid[1, 2], lift(s -> "Belt Status: $s", status_str),
          font = :bold, fontsize = 13, halign = :center)

    # Main Conveyor Viewport
    ax = Axis(fig[6, 1],
              title = "Physical Material Handling Line (Scale in Meters)",
              xlabel = "Conveyor Position (m)",
              ylabel = "Elevation (m)",
              aspect = AxisAspect(3.2))
    ylims!(ax, -0.6, 2.8)
    xlims!(ax, -0.5, belt_length + 0.8)

    # 1. Conveyor Structure (Legs & Bed)
    leg_x_coords = [1.2, 3.8, 6.4, 9.0]
    for lx in leg_x_coords
        poly!(ax, Rect2f(lx - 0.08, -0.5, 0.16, 0.5), color = RGBf(0.82, 0.84, 0.88), strokecolor = :gray60, strokewidth = 1)
    end

    # Belt Bed Surface
    poly!(ax, Rect2f(0.0, 0.0, belt_length, 0.2),
          color = RGBf(0.25, 0.26, 0.28), strokecolor = :black, strokewidth = 1.5)

    # Belt Rollers (Texture lines)
    roller_lines = lift(belt_offset_obs) do offset
        pts = Point2f[]
        step = 0.5
        for x in (0.2 + offset):step:(belt_length - 0.2)
            push!(pts, Point2f(x, 0.03))
            push!(pts, Point2f(x, 0.17))
        end
        return pts
    end
    lines!(ax, roller_lines, color = RGBf(0.55, 0.57, 0.62), linewidth = 2.0)

    # 2. Obstruction Gate Frame & Barrier
    gate_width = 0.2
    # Frame uprights
    poly!(ax, Rect2f(gate_x - 0.05, 0.2, 0.3, 1.8), color = RGBf(0.7, 0.72, 0.76), strokecolor = :black, strokewidth = 1)
    # Moving barrier gate arm
    gate_poly = lift(gate_y_obs) do gy
        Rect2f(gate_x, 0.2, 0.18, gy)
    end
    poly!(ax, gate_poly, color = gate_color_obs, strokecolor = :black, strokewidth = 1.5)

    # Gate Indicator LED
    scatter!(ax, [Point2f(gate_x + 0.09, 2.1)], color = gate_color_obs, markersize = 18, strokewidth = 1.5)

    # Flow Velocity Vector Arrow
    arrows!(ax, [0.5], [-0.25], [1.2], [0.0], color = RGBf(0.18, 0.45, 0.85), linewidth = 2.5)
    text!(ax, "Belt Flow (1.5 m/s)", position = Point2f(1.8, -0.25), align = (:left, :center), fontsize = 11, color = :gray30)

    # 3. Dynamic Boxes (Items on Conveyor)
    poly!(ax, box_polys_obs, color = box_colors_obs, strokecolor = :black, strokewidth = 1.2)

    # Label text for box IDs
    item_labels_str = lift(box_labels_obs) do lbls
        [l[2] for l in lbls]
    end
    item_labels_pos = lift(box_labels_obs) do lbls
        [l[1] for l in lbls]
    end
    text!(ax, item_labels_str, position = item_labels_pos,
          align = (:center, :center), fontsize = 10, font = :bold, color = :white)

    # ── Interactive Callbacks ──────────────────────────────────────────────────

    on(btn_block.clicks) do _
        is_blocked[] = !is_blocked[]
    end

    on(is_blocked) do blk
        gate_color_obs[] = blk ? RGBf(0.85, 0.22, 0.22) : RGBf(0.12, 0.65, 0.32)
        gate_y_obs[]     = blk ? 0.05 : 1.5 # Barrier drops to belt level when blocked
        update_hud()
    end

    on(tog_mode.active) do active
        is_accumulating[] = active
        lbl_mode.text[] = active ? "Accumulating (ZPA)" : "Non-Accumulating (Rigid)"
        mode_desc_str[] = active ?
            "MODE: Accumulating Conveyor (ZPA) — Trailing items queue independently" :
            "MODE: Non-Accumulating Conveyor (Rigid Bed) — Entire belt halts simultaneously upon obstruction"
        update_hud()
    end

    on(btn_play.clicks) do _
        is_playing[] = !is_playing[]
        btn_play.label[] = is_playing[] ? "Pause" : "Play"
    end

    on(btn_reset.clicks) do _
        empty!(items)
        items_count_str[] = "0"
        box_polys_obs[] = Rect2f[]
        box_colors_obs[] = RGBf[]
        box_labels_obs[] = Tuple{Point2f, String}[]
        spawn_timer = 0.0
        update_hud()
    end

    function update_hud()
        st = "Flowing"
        if is_blocked[]
            st = is_accumulating[] ? "Queued (Accumulating ZPA)" : "Line Halted (Rigid Bed)"
        end
        status_str[] = st
        items_count_str[] = string(length(items))
    end

    # ── Kinematic Stepping Loop (SimDES Physics Model) ─────────────────────────

    dt = 0.03 # 30 ms frame time

    function sim_step!()
        !is_playing[] && return

        # 1. Belt Speed Determination
        # In non-accumulating (rigid) mode, obstruction stops the entire belt drive
        belt_speed = nominal_speed
        if !is_accumulating[] && is_blocked[]
            belt_speed = 0.0
        end

        # Texture scrolling
        if belt_speed > 0.0
            belt_offset_obs[] = (belt_offset_obs[] + belt_speed * dt) % 0.5
        end

        # 2. Inlet Spawn Logic with Clearance Check
        spawn_timer += dt
        if spawn_timer >= spawn_interval[]
            # Check inlet clearance: leftmost item must have cleared the entry zone
            min_x = isempty(items) ? Inf : minimum(it.x for it in items)
            if min_x > (item_size + safety_gap)
                push!(items, ConveyorItem(next_id, 0.0, false))
                next_id += 1
                spawn_timer = 0.0
            end
        end

        # Sort items descending by position (lead item first)
        sort!(items, by = it -> it.x, rev = true)

        # 3. Motion & Accumulation Kinematics
        for i in 1:length(items)
            it = items[i]

            if !is_accumulating[]
                # Rigid bed: items move strictly with the belt surface
                if is_blocked[]
                    it.is_stopped = true
                else
                    it.is_stopped = false
                    it.x += belt_speed * dt
                end
            else
                # Accumulating mode (Zero-Pressure Accumulation / ZPA)
                target_x = belt_length + 2.0

                if i == 1
                    # Lead item checks gate obstruction
                    if is_blocked[]
                        target_x = gate_x - item_size
                    end
                else
                    # Trailing item checks position of item ahead
                    ahead = items[i - 1]
                    target_x = ahead.x - item_size - safety_gap
                end

                if it.x < target_x
                    new_x = it.x + belt_speed * dt
                    if new_x >= target_x
                        it.x = target_x
                        it.is_stopped = true
                    else
                        it.x = new_x
                        it.is_stopped = false
                    end
                else
                    it.x = target_x
                    it.is_stopped = true
                end
            end
        end

        # Remove items that have cleared the discharge end
        filter!(it -> it.x <= belt_length, items)

        # 4. Update Makie Visual Observables
        polys  = Rect2f[]
        colors = RGBf[]
        lbls   = Tuple{Point2f, String}[]

        for it in items
            y_base = 0.2
            push!(polys, Rect2f(it.x, y_base, item_size, item_size))
            c = it.is_stopped ? RGBf(0.92, 0.52, 0.15) : RGBf(0.18, 0.45, 0.85)
            push!(colors, c)
            push!(lbls, (Point2f(it.x + item_size/2, y_base + item_size/2), "#$(it.id)"))
        end

        box_polys_obs[]  = polys
        box_colors_obs[] = colors
        box_labels_obs[] = lbls
        items_count_str[] = string(length(items))
    end

    # Return figure and stepper function
    return fig, sim_step!
end

# ── Launch Interactive Window or Save Headless Demonstration ─────────────────

fig, step_fn! = run_interactive_conveyor()

# Advance 60 frames so initial items are on the belt, then save snapshot
for _ in 1:60
    step_fn!()
end
Makie.save("conveyor_simulation_makie.png", fig)
println("Saved conveyor demonstration snapshot -> conveyor_simulation_makie.png")

if GLMAKIE_AVAILABLE && (isinteractive() || haskey(ENV, "DISPLAY"))
    println("Displaying interactive GLMakie window...")
    display(fig)

    if isinteractive()
        # In interactive REPL: run in background async task so user keeps REPL prompt
        @async begin
            while true
                step_fn!()
                sleep(0.03)
            end
        end
    else
        # From CLI: run live loop for 10 seconds to show window
        println("Running live animation loop (press Ctrl+C to stop)...")
        for _ in 1:200
            step_fn!()
            sleep(0.03)
        end
    end
end

