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

# Usage
```julia
fel = FutureEventList()
id  = schedule!(fel, EntityArrival(1, 1, 0.5), 0.5)
ev, t = safe_dequeue!(fel)   # returns the event + its time
cancel!(id)                  # future dequeue will skip this event
```
"""
struct FutureEventList
    queue :: PriorityQueue{CancellableEvent, Float64}
end

FutureEventList() = FutureEventList(PriorityQueue{CancellableEvent, Float64}())

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
        if is_cancelled(cev.id)
            SimCore._consume_cancelled!(cev.id)
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
