"""
    fel.jl — Future Event List

The FEL is the central data structure of any DES engine.
Every pending event is stored here, ordered by simulated time.
The scheduler always dequeues the minimum-timestamp event.

Design ref: §7.1–7.3 (FEL data structures)

Implementation: `DataStructures.PriorityQueue{CancellableEvent, Float64}` (binary heap)
- Insert: O(log n)   — acceptable for Tier 1 (< 100k events)
- Dequeue-min: O(log n)
- Cancel: O(1) via lazy deletion (skipped in `safe_dequeue!`)

For Tier 2 (per-LP parallel DES), each ZoneState gets its own `FutureEventList`.
"""

"""
    FutureEventList

Thread-local (Tier 1) or per-LP (Tier 2) priority queue of simulation events.
Carries its own cancellation set — no global lock needed in single-threaded Tier 1.
For Tier 2, each LP owns its FEL so the set is still unshared (no lock needed).

# Usage
```julia
fel = FutureEventList()
id  = schedule!(fel, EntityArrival(1, 1, 0.5), 0.5)
ev, t = safe_dequeue!(fel)   # returns the event + its time
cancel!(fel, id)             # lazily cancel — next dequeue will skip it
```
"""
struct FutureEventList
    queue     :: PriorityQueue{CancellableEvent, Float64}
    cancelled :: Set{UInt64}   # per-FEL; no lock needed (Tier 1 = single thread,
                               # Tier 2 = each LP owns its own FEL)
end

FutureEventList() = FutureEventList(PriorityQueue{CancellableEvent, Float64}(),
                                    Set{UInt64}())

"""
    cancel!(fel, id) -> nothing

Lazily cancel the event with ID `id`. The next time `safe_dequeue!` encounters
it in the FEL, the event will be silently skipped.

This is the preferred cancellation API in SimDES (no global lock, O(1) set insert).
For SimCore-level tests, the global `cancel!(id::UInt64)` in SimCore remains available.
"""
cancel!(fel::FutureEventList, id::UInt64) = (push!(fel.cancelled, id); nothing)

"""
    schedule!(fel, event, t) -> UInt64

Wrap `event` in a `CancellableEvent` with auto-assigned ID, insert into the FEL
at priority `t` (simulated time), and return the event ID (for future cancellation).
"""
function schedule!(fel::FutureEventList, event::SimEvent, t::Float64)
    cev = CancellableEvent(event, t)
    enqueue!(fel.queue, cev => t)
    return cev.id
end

"""
    safe_dequeue!(fel) -> Union{Tuple{CancellableEvent, Float64}, Nothing}

Dequeue the next event, skipping any cancelled ones.
Returns `(event, time)` or `nothing` if the FEL is empty.
"""
function safe_dequeue!(fel::FutureEventList)
    while !Base.isempty(fel.queue)
        cev, t = dequeue_pair!(fel.queue)
        if cev.id in fel.cancelled
            delete!(fel.cancelled, cev.id)   # consume: free memory, O(1) no lock
            continue          # skip cancelled events
        end
        return cev, t
    end
    return nothing
end

"""
    peek_time(fel) -> Float64

Return the time of the next event without removing it.
Returns `Inf` if the FEL is empty.
"""
function peek_time(fel::FutureEventList)
    Base.isempty(fel.queue) && return Inf
    return peek(fel.queue)[2]
end

Base.isempty(fel::FutureEventList) = Base.isempty(fel.queue)
Base.length(fel::FutureEventList)  = length(fel.queue)
