"""
    dependency_graph.jl — SubModel Dependency Graph & Conservative Promise Horizons

Implements InControl ERS / Chandy–Misra–Bryant style directed dependency edges
and conservative lookahead promises (`SetPromise` / `T_safe(B)`) between `SubModel`
partitions in `SimDES`.

For each directed dependency edge `A -> B` with minimum cross-SubModel delay
`min_delay_ps >= 0` (in picoseconds):
- `SubModel` `A` promises that it will not emit any event to `SubModel` `B`
  with a timestamp earlier than `promised_ps(A -> B) = T_next(A) + min_delay_ps`.
- `SubModel` `B` can safely execute all local events with timestamp strictly less
  than every incoming promise (`t_ps <= promised_ps(A -> B) - 1`) and `<= ceiling_ps`:
  `T_safe(B) = min( min_{A in In(B)} (promised_ps(A -> B) - 1), T_ceiling )`
  completely in parallel with zero locks on local zone state.
"""

"""
    SubModelDependency

Directed dependency edge from `source_sm` to `target_sm`.

# Fields
- `source_sm::Int`: 1-based index of the upstream `SubModelPartition`.
- `target_sm::Int`: 1-based index of the downstream `SubModelPartition`.
- `min_delay_ps::Int64`: Lower-bound delay `τ_min(A -> B)` in picoseconds (`10^-12 s`).
- `promised_ps::Threads.Atomic{Int64}`: Latest promised horizon `T_promise(A -> B)` in picoseconds.
"""
mutable struct SubModelDependency
    source_sm    :: Int
    target_sm    :: Int
    min_delay_ps :: Int64
    promised_ps  :: Threads.Atomic{Int64}
end

function SubModelDependency(source_sm::Int, target_sm::Int, min_delay_s::Real; initial_promise_ps::Int64 = Int64(0))
    delay_ps = max(Int64(0), to_picoseconds(min_delay_s))
    return SubModelDependency(source_sm, target_sm, delay_ps, Threads.Atomic{Int64}(initial_promise_ps))
end

"""
    SubModelDependencyGraph

Directed graph of `SubModelDependency` edges across `n_submodels` partitions.
"""
struct SubModelDependencyGraph
    n_submodels  :: Int
    outgoing     :: Vector{Vector{SubModelDependency}}
    incoming     :: Vector{Vector{SubModelDependency}}
    edge_lookup  :: Dict{Tuple{Int, Int}, SubModelDependency}
    strict_check :: Bool
end

function SubModelDependencyGraph(n_submodels::Int; strict_check::Bool = true)
    n_submodels >= 1 || throw(ArgumentError("n_submodels must be >= 1, got $n_submodels"))
    return SubModelDependencyGraph(
        n_submodels,
        [SubModelDependency[] for _ in 1:n_submodels],
        [SubModelDependency[] for _ in 1:n_submodels],
        Dict{Tuple{Int, Int}, SubModelDependency}(),
        strict_check,
    )
end

"""
    add_submodel_dependency!(graph, source_sm, target_sm, min_delay_s) -> SubModelDependency

Register or update a directed dependency `source_sm -> target_sm` with lower-bound delay `min_delay_s` (in seconds).
If multiple zone-to-zone wires connect `source_sm` to `target_sm`, retains the minimum delay across those wires.
"""
function add_submodel_dependency!(
    graph::SubModelDependencyGraph,
    source_sm::Int,
    target_sm::Int,
    min_delay_s::Real,
)::SubModelDependency
    (1 <= source_sm <= graph.n_submodels && 1 <= target_sm <= graph.n_submodels) ||
        throw(BoundsError("SubModel indices ($source_sm, $target_sm) out of range 1:$(graph.n_submodels)"))
    source_sm == target_sm && throw(ArgumentError("Cannot add self-dependency on SubModel $source_sm"))

    delay_ps = max(Int64(0), to_picoseconds(min_delay_s))
    key = (source_sm, target_sm)
    if haskey(graph.edge_lookup, key)
        edge = graph.edge_lookup[key]
        if delay_ps < edge.min_delay_ps
            edge.min_delay_ps = delay_ps
        end
        return edge
    end

    edge = SubModelDependency(source_sm, target_sm, min_delay_s; initial_promise_ps = delay_ps)
    push!(graph.outgoing[source_sm], edge)
    push!(graph.incoming[target_sm], edge)
    graph.edge_lookup[key] = edge
    return edge
end

"""
    set_promise_ps!(edge::SubModelDependency, new_promise_ps::Int64; strict::Bool = true) -> Int64

Update the promised timestamp on `edge` to `new_promise_ps` (in picoseconds).
Promises are monotonically non-decreasing during a forward simulation run.
"""
@inline function set_promise_ps!(edge::SubModelDependency, new_promise_ps::Int64; strict::Bool = true)::Int64
    prev = edge.promised_ps[]
    if new_promise_ps < prev
        if strict
            throw(AssertionError(
                "PDES Promise Regression on SubModel $(edge.source_sm) -> $(edge.target_sm): " *
                "new_promise_ps ($new_promise_ps) < previous promised_ps ($prev)"
            ))
        end
        return prev
    end
    edge.promised_ps[] = new_promise_ps
    return new_promise_ps
end

@inline function set_promise_ps!(
    graph::SubModelDependencyGraph,
    edge::SubModelDependency,
    new_promise_ps::Int64,
)::Int64
    return set_promise_ps!(edge, new_promise_ps; strict = graph.strict_check)
end

"""
    update_outgoing_promises!(graph, source_sm, earliest_send_base_ps)

Update all outgoing dependency promises `source_sm -> B` using:
`T_promise(source_sm -> B) = earliest_send_base_ps + τ_min(source_sm -> B)`.
"""
@inline function update_outgoing_promises!(
    graph::SubModelDependencyGraph,
    source_sm::Int,
    earliest_send_base_ps::Int64,
)
    for edge in @inbounds graph.outgoing[source_sm]
        promised = earliest_send_base_ps >= typemax(Int64) - edge.min_delay_ps - Int64(1) ?
                   typemax(Int64) :
                   earliest_send_base_ps + edge.min_delay_ps
        edge.promised_ps[] = promised
    end
    return nothing
end

"""
    propagate_promises!(graph::SubModelDependencyGraph, local_next_ps::Vector{Int64})

Compute transitive Chandy–Misra–Bryant / ERS promises across `graph` given each
SubModel's current local earliest event timestamp `local_next_ps[sm]`.
Even if a downstream `SubModel` `B` currently has an empty local FEL (`local_next_ps[B] == typemax(Int64)`),
it can receive an event from upstream `A` at `T_promise(A -> B)` and subsequently emit to `C` at
`T_promise(A -> B) + τ_min(B -> C)`. Bellman–Ford relaxation over the `K` SubModels
computes the exact transitive lower bound `effective_next_ps[sm]` in `O(K * |E|)` integer ops.
"""
function propagate_promises!(
    graph::SubModelDependencyGraph,
    local_next_ps::Vector{Int64},
)
    K = graph.n_submodels
    eff = copy(local_next_ps)
    for _ in 1:K
        changed = false
        for sm in 1:K
            base_ps = @inbounds eff[sm]
            base_ps == typemax(Int64) && continue
            for edge in @inbounds graph.outgoing[sm]
                cand = base_ps >= typemax(Int64) - edge.min_delay_ps - Int64(1) ?
                       typemax(Int64) :
                       base_ps + edge.min_delay_ps
                if cand < @inbounds eff[edge.target_sm]
                    @inbounds eff[edge.target_sm] = cand
                    changed = true
                end
            end
        end
        changed || break
    end
    for sm in 1:K
        update_outgoing_promises!(graph, sm, @inbounds eff[sm])
    end
    return eff
end

"""
    safe_horizon_ps(graph, target_sm, ceiling_ps) -> Int64

Compute the conservative inclusive safe execution horizon `T_safe(target_sm)` in picoseconds:
`T_safe(target_sm) = min( min_{e in In(target_sm)} (e.promised_ps[] - 1), ceiling_ps )`.
Because any future event arriving from upstream `A` has timestamp `>= e.promised_ps[]`,
every local event in `target_sm` with `t_ps <= e.promised_ps[] - 1` is strictly earlier
than any possible future arrival from `A`.
"""
@inline function safe_horizon_ps(
    graph::SubModelDependencyGraph,
    target_sm::Int,
    ceiling_ps::Int64,
)::Int64
    horizon = ceiling_ps
    for edge in @inbounds graph.incoming[target_sm]
        p = edge.promised_ps[]
        safe_p = p == typemax(Int64) ? typemax(Int64) : p - Int64(1)
        if safe_p < horizon
            horizon = safe_p
        end
    end
    return horizon
end

"""
    min_positive_promise_ps(graph::SubModelDependencyGraph) -> Int64

Return the smallest `min_delay_ps` across all dependency edges in `graph`,
or `typemax(Int64)` if the graph has no edges.
"""
function min_positive_promise_ps(graph::SubModelDependencyGraph)::Int64
    isempty(graph.edge_lookup) && return typemax(Int64)
    min_ps = typemax(Int64)
    for edge in values(graph.edge_lookup)
        if edge.min_delay_ps < min_ps
            min_ps = edge.min_delay_ps
        end
    end
    return min_ps
end
