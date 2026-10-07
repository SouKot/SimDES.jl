using Test
using SimDES

@testset "SimDESMakieExt (Optional Extension)" begin
    makie_available = false
    try
        using Makie: Makie
        makie_available = true
    catch
        makie_available = false
    end

    if makie_available
        cfg1 = ZoneConfig(id=1, arrival=PoissonArrival(1.0))
        cfg2 = ZoneConfig(id=2, num_servers=2)
        cfg3 = ZoneConfig(id=3, routing=ExitSystem())

        fig = simplot(cfg1, cfg2, cfg3)
        @test fig isa Makie.Figure

        times = [0.0, 1.2, 2.5, 3.1, 4.0]
        q_lens = [0, 1, 2, 1, 0]
        fig_q = plot_queue_history(times, q_lens)
        @test fig_q isa Makie.Figure

        records = [
            (server_id=1, state=:busy, t_start=0.0, t_end=2.0),
            (server_id=1, state=:idle, t_start=2.0, t_end=3.5),
            (server_id=2, state=:busy, t_start=1.0, t_end=3.0)
        ]
        fig_g = plot_gantt(records)
        @test fig_g isa Makie.Figure

        # Observable trajectory animation canvas
        snapshots = [
            (t=0.0, items=[(x=0.0, y=0.0)]),
            (t=1.0, items=[(x=1.5, y=0.0)])
        ]
        fig_anim, obs = animate_sim(snapshots)
        @test fig_anim isa Makie.Figure
        @test obs isa Makie.Observable
    else
        @info "Makie not loaded — skipping SimDESMakieExt tests (clean headless behavior)"
        @test true
    end
end
