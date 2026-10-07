using Documenter
using SimCore
using SimDES

makedocs(
    sitename = "SimDES.jl",
    authors = "Sourabh Kotnala <sauravkotnala@gmail.com>",
    modules = [SimDES],
    format = Documenter.HTML(
        prettyurls = get(ENV, "CI", nothing) == "true",
        canonical = "https://SouKot.github.io/SimDES.jl",
        sidebar_sitename = true,
    ),
    pages = [
        "Home" => "index.md",
        "Queueing Theory Reference" => "queueing_theory.md",
        "Conveyor Kinematics" => "conveyors.md",
        "Makie Visualization Recipes" => "makie_recipes.md",
        "API Reference" => "api.md",
    ],
    checkdocs = :exports,
    warnonly = [:missing_docs, :cross_references],
)

deploydocs(
    repo = "github.com/SouKot/SimDES.jl.git",
    devbranch = "main",
    push_preview = true,
)
