# API Reference

## Simulation World & Future Event List

```@docs
SimWorld
FutureEventList
schedule!
safe_dequeue!
peek_time
cancel!
```

---

## Zone Configuration & Routing

```@docs
ZoneConfig
RoutingPolicy
FixedRoute
ProbRoute
ShortestQueueRoute
RoundRobinRoute
ExitSystem
ArrivalProcess
PoissonArrival
NHPPArrival
NoArrival
```

---

## Simulation Runners & Loop

```@docs
sim_loop!
run_mm1!
run_mmc!
run_mm1k!
run_md1!
run_mg1!
run_tandem!
run_jackson!
run_priority!
run_with_failures!
run_nhpp!
run_forkjoin!
```

---

## Visualization Recipes (Extension)

```@docs
simplot
plot_queue_history
plot_gantt
animate_sim
```
