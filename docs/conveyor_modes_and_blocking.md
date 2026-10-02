# Conveyor Physics Design: Free-Flow, Accumulating and Indexing Modes

Status legend: **[Implemented]** is in SimDES and covered by tests; **[Planned]** is the intended physics but not yet modelled.

## 1. Purpose

A conveyor is a finite-length transport element that carries discrete products from an inlet to an outlet. This document defines, from the physical point of view, what each conveyor mode must do under every condition it can meet: empty, flowing, blocked at the outlet, fed too fast, full, stopped, and released. Each mode is specified by a small set of invariants and a response table, followed by worked numeric examples.

## 2. Common Model

### 2.1 State and parameters

| Symbol | Meaning | Authoring property |
|---|---|---|
| $L$ | Belt length (m) | `length` |
| $v$ | Belt speed (m/s) | `speed` |
| $x_i$ | Position of product $i$ along the belt, $0 \le x_i \le L$; inlet at 0, outlet at $L$ | derived |
| $p$ | Product footprint along the belt (m) | `accumulation_pitch` |
| $g$ | Clear gap kept between products, $g \ge 0$ (m) | `accumulation_gap` |
| $s$ | Slot spacing, centre to centre | $s = p + g$ (accumulating, indexing); $s = p$ (free-flow) |
| $T$ | Index interval (s), indexing only | `index_interval` |
| $N$ | Maximum products on the belt | `capacity` |

Products are ordered by position; product 1 is the lead (closest to the outlet). Products never pass each other.

### 2.2 Invariants (all modes)

1. **Conservation.** A product that has been accepted is on the belt, or in the next element, or has left the system through an explicit exit. A full or busy destination never removes a product from the belt.
2. **No overlap.** For consecutive products, $x_{i-1} - x_i \ge s$ at all times. Products are never placed on top of one another, at the outlet or at the inlet.
3. **Order.** Products leave in the order they entered.
4. **Capacity.** At most $N$ products are on the belt. Spacing further limits the real count to about $\lfloor L/s \rfloor + 1$.
5. **Transit time.** A product that is never held takes $L/v$ from inlet to outlet.
6. **Atomic hand-off.** A product leaves the belt only at the instant the next element accepts it.

### 2.3 Outlet state

The next element is reached through the conveyor's routing. For each possible destination the state is:

| Destination state | Condition | Outlet |
|---|---|---|
| Open | Server slot or buffer slot free | Product transfers immediately |
| Full | `busy servers + waiting = capacity` | Blocked until the destination releases a slot |
| Down | Zero available servers (failure) | Blocked until repair |
| Inlet occupied | Destination is a conveyor and its rearmost product is closer than $s$ to its inlet | Blocked for a short, computable time, or until that belt moves |

**Several destinations.** The product may go only to a destination that is open at that instant. If every possible destination is blocked, the product waits; it is never sent to a full destination.

| Routing | Choice among open destinations |
|---|---|
| Fixed | The single destination. |
| Probabilistic | Probabilities of the open destinations are renormalised, so a blocked branch gives its share to the others. A branch that exits the system is always open. |
| Round-robin | The next open destination after the last one used. |
| Shortest-queue | The open destination with the lowest load. |
| Dynamic policy | The policy function is given only the open destinations. |

**Server and buffer meaning.** A standalone Server has only its service slots; it holds no hidden waiting space. A Queue fused with a Server has capacity *queue capacity + server count*. The outlet is blocked only when that total is reached. A busy Server in front of a Queue with free space is not a blocked outlet.

### 2.4 Inlet admission and feeders

A product is placed at $x = 0$ only if the belt has room (fewer than $N$ products) and the rearmost product is at least $s$ ahead of the inlet. A conveyor never drops a product. Otherwise:

| Feeder | Behaviour |
|---|---|
| Source | The product waits at the source and the source stays stalled; it generates its next product only after this one has entered. Interarrival time restarts from the entry instant. |
| Server, Queue or other station | The finished product stays in the station's service slot (blocking after service). The station cannot start new work until the product has moved on. |
| Another conveyor | The product stays at the outlet of the feeding belt, subject to that belt's own mode rules. |
| Gap will open soon (belt moving) | The arrival is delayed by exactly the time needed for the gap to reach $s$. |
| Belt stopped, packed or full | The arrival waits until the belt moves or a product leaves; waiting arrivals enter in first-come order. |

System entry is recorded when a held product first presents itself, so waiting at the feeder counts toward its time in the system. **[Implemented]**

## 3. Free-Flow Mode (non-accumulating belt)

### 3.1 Physics

All products ride on one rigid belt surface. They share one velocity. If the belt moves, every product moves; if the belt stops, every product stops. The distance between two products is fixed at the moment they enter and never changes while on the belt. The slot spacing is only the minimum physical footprint $p$ and cannot be enforced by the belt itself.

### 3.2 Response table

| Condition | Behaviour |
|---|---|
| Empty belt, product enters | Moves at $v$; reaches the outlet after $L/v$. |
| Steady flow, destination open | Every product moves at $v$ and transfers on arrival at the outlet. |
| Lead reaches the outlet and the destination is Full or Down | The belt stops. The lead stays at $x = L$. **Every other product stops where it is**; no product moves while the belt is stopped. Separation between products is unchanged. |
| Destination is blocked before the lead reaches the outlet | The belt continues to run until the lead reaches the outlet, and then stops as above. |
| New product arrives while the belt is stopped | It is placed at $x = 0$ at rest if the rearmost product is at least $p$ ahead; otherwise it waits at its feeder (Section 2.4). |
| Two products arrive closer than $p$ apart | The second waits at the feeder until the gap reaches $p$. |
| Destination releases a slot | The belt restarts. All products move at $v$ together. The lead transfers at once if the destination can accept it. |
| Destination fills again after the lead transfers | The next product to reach the outlet stops the belt again. |
| Belt full ($N$ products) | A new product waits at its feeder. |
| Destination failure, then repair | Same as Full while down; repair releases the belt. |

### 3.3 Example

$L = 4$ m, $v = 1$ m/s, $p = 0.5$ m. Products enter at $t = 0, 1, 2$ s. The server in front of the outlet is busy for 20 s.

At $t = 4$ the lead reaches $x = 4$ and the belt stops. The others are at $x = 3$ and $x = 2$. They stay there. The separation of 1 m is preserved and no product overlaps another. When the server frees at $t = 20$, all three start moving; the lead enters the server, and the others reach the outlet 1 s and 2 s later and wait again for the server.

## 4. Accumulating Mode (zero-pressure)

### 4.1 Physics

The belt is divided into sensed zones. A product that reaches a stopped product stops $s$ behind it, while products further back keep moving until they, too, reach their resting slot. Products never touch; the gap is exactly $g$ when packed.

Resting slot of the $k$-th product from the outlet when the outlet is blocked:

$$x_k^{\text{rest}} = L - (k-1)\,s, \qquad s = p + g.$$

### 4.2 Response table

| Condition | Behaviour |
|---|---|
| Empty belt, product enters | Moves at $v$ to the outlet. |
| Steady flow, destination open | Products advance at $v$ with spacing at least $s$ and transfer on arrival. |
| Outlet blocked (Full or Down) | The lead runs to $x = L$ and stops. Product $k$ runs to $x_k^{\text{rest}}$ and stops. Each product moves independently until it reaches its resting slot; no product overlaps another. |
| Gap parameter | $g = 0$: products touch, pitch is the footprint. $g > 0$: products stop $g$ metres apart. Any $g \ge 0$ is allowed. |
| Product arrives closer than $s$ to the one ahead | It waits at the feeder until the gap reaches $s$. |
| Arrival when the rearmost product is stationary and within $s$ of the inlet | The product waits at its feeder until the belt moves (belt is packed to the inlet). |
| Destination releases a slot | The lead transfers. The remaining products advance together. Spacing is preserved. |
| Packed belt reaches capacity $N$ or length limit | New products wait at their feeder. |
| Destination failure, then repair | Packs while down; releases on repair. |

### 4.3 Example

$L = 4$ m, $v = 1$ m/s, $p = 0.5$ m, $g = 0.25$ m, so $s = 0.75$ m. Products enter at $t = 0, 1, 2$ s; the outlet is blocked for 20 s.

Resting positions: $4.0$, $3.25$, $2.5$ m. With $g = 0$ they are $4.0$, $3.5$, $3.0$ m. The lead stops at $t = 4$, the second at $t = 4.25$, the third at $t = 4.5$.

## 5. Indexing Mode (stepped belt)

### 5.1 Physics

The bed moves in discrete steps. At each pulse, every product advances by one slot $s = p + g$, then stops. Pulses repeat every $T$ seconds. Between pulses nothing moves. The bed is rigid: the pulses that would move a product past the outlet are not performed.

### 5.2 Response table

| Condition | Behaviour |
|---|---|
| Product enters | Waits at $x = 0$ for the next pulse. |
| Pulse, destination open | All products advance by $s$. A product reaching $x = L$ transfers immediately. |
| Lead at the outlet and destination Full or Down | The lead stays at $x = L$. **No pulse is executed**: all products stay in their slots until the destination can accept the lead. |
| Product arrives while rearmost is within $s$ of the inlet | Waits at the feeder for one pulse ($T$) and tries again. |
| Arrival while the bed is held | The product waits at its feeder if the inlet slot is occupied. |
| Destination releases a slot | The lead transfers, pulses resume after one interval $T$. |
| Idle bed (no products) | No pulses are generated. |
| Destination failure, then repair | Bed is held while down; resumes on repair. |

### 5.3 Example

$L = 4$ m, $p = 0.5$ m, $g = 0.5$ m (step 1 m), $T = 1$ s. Products enter at $t = 0, 1.1, 2.2$ s. At the pulse at $t = 4$ the lead reaches $x = 4$; if the server is busy, the others rest at 3 m and 2 m and no further pulses occur until the lead transfers.

## 6. Chains and Interaction with Other Elements

| Case | Behaviour |
|---|---|
| Conveyor feeding a Server | Section 2.3. Standalone Server blocks while its service slots are occupied. |
| Conveyor feeding Queue + Server | Blocks only when Queue and Server together are at capacity. |
| Conveyor feeding a conveyor | The upstream lead waits at its outlet until the next belt's inlet is clear (Section 2.4), then transfers. Blocking propagates upstream belt by belt. |
| Server or Source feeding a conveyor | Section 2.4: the source stalls, the server holds the finished product in its slot. Nothing is lost. |
| Conveyor with several downstream destinations | Section 2.3: only open destinations are used; the product waits when none is open. |
| Server with several destinations where one is a conveyor | Same rule: blocking after service, open destinations only. |
| Server feeding any other station (Server, Queue, Queue + Server) | Blocking after service: the finished product stays in its slot until the destination has room. Internal transfers are never dropped. Only a Source whose destination is full and is not a conveyor still sees a rejected arrival, since that is the system boundary of a finite-capacity model. |

## 7. Parameter Rules

- $p > 0$, $g \ge 0$. In free-flow only $p$ is used.
- Larger $g$ lowers throughput: maximum departure rate is $v / s$ (products per second).
- $T$ and $s$ set the indexing throughput: $s / T$ metres per second of bed speed.
- Capacity $N$ is a count limit; the length limit $\lfloor L/s \rfloor + 1$ applies through the inlet check.

## 8. Not Yet Modelled

- Acceleration and deceleration of the belt, soft start, or hook-driven speed change while products are held.
- Switching mode while products are on the belt.
- Different product lengths on the same belt (each product uses the same footprint $p$).
- Belt breakdown or e-stop of the conveyor itself.
- Accumulating zones with sensors and sequential release (slug release).
- Blocked-time and starved-time statistics for conveyors.

## 9. Verification

Tests in `SimDES/test/runtests.jl`:

| Test | Checks |
|---|---|
| Conveyor outlet backpressure waits for downstream server in every mode | A product at the outlet waits for a busy server and is neither lost nor duplicated. |
| Blocked conveyor keeps products apart and stationary | Free-flow at 4, 3, 2 m, accumulating at 4, 3.25, 2.5 m (g = 0.25) and 4, 3.5, 3 m (g = 0), indexing at 4, 3, 2 m. All products have zero speed; all products later pass through the server. |
| Conveyor inlet defers a product that would overlap the one ahead | Close arrivals are separated by the slot spacing; none are rejected. |
| Conveyor outlet with several destinations only uses one that is open | Probabilistic, round-robin, shortest-queue and dynamic routing skip a full destination; with all full the product waits on the belt. |
| Sources and servers feeding a conveyor are held, never dropped | A stalled source and a blocked server hold their products; belt capacity is respected and every product eventually arrives. |
| Internal transfers between stations are lossless | A fast station in front of a slow, small one holds products instead of dropping them. |

### Conveyor Physics Lab

Ten scenarios with live charts and quantitative checks (`GodotBridge/src/lab/`), available in the Godot **Examples > Conveyor Physics Lab** menu and as `GodotBridge/test/test_conveyor_lab.jl`. Every scenario is also checked against these invariants: no overlap at the required spacing, never above capacity, no product moves backwards, nothing moves while a rigid belt is blocked, and no product is lost or created.

| # | Scenario | What it demonstrates |
|---|---|---|
| 1 | Free-flow stop-and-go | The whole belt halts and restarts; machine rate sets throughput; the source stalls. |
| 2 | Gap sets throughput | Accumulating belts deliver $v/(p+g)$; free-flow ignores the gap. |
| 3 | Accumulation packing | Resting spacing is exactly $p+g$; the belt holds $\lfloor L/s\rfloor+1$ products. |
| 4 | Mode comparison | Same bottleneck behind all three modes. |
| 5 | Diverter to open paths | Probabilistic, round-robin and shortest-queue routing never push into a busy machine. |
| 6 | Chain backpressure | The jam spreads upstream belt by belt, then the source stalls. |
| 7 | Machine breakdown | The belt stops while the machine is down and restarts on repair. |
| 8 | Blocking after service | A fast press holds its product when the next belt is full. |
| 9 | Merge at the inlet | Two sources share one inlet; throughput is $v/p$. |
| 10 | Indexing steps and hold | Positions are whole slots; a held bed does not move. |

Run `julia --project=packages/SimOptim scripts/run_conveyor_lab.jl` to regenerate the scenes in `godot/examples/conveyor_lab/` and write CSV data, SVG charts (including a space-time diagram of every product on every belt) and an HTML report per scenario to `reports/conveyor_lab/index.html`.

`GodotBridge/test/test_scenespec_compiler.jl` verifies that `accumulation_gap` is compiled into the runtime configuration.
