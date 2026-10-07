# Analytical Queueing Theory Reference

## Verification Matrix

Every queueing model in `SimDES.jl` corresponds to an explicit automated test in `test/runtests.jl` verified against analytical theory:

| Model | Formula / Invariant | Analytical Reference | Test Suite |
|---|---|---|---|
| **$M/M/1$** | $L = \frac{\rho}{1-\rho}, \quad W = \frac{1}{\mu(1-\rho)}$ | Kleinrock (1975) Vol. 1 | `DES-S-01`, `02`, `03` |
| **$M/M/c$** | $W_q = \frac{C(c, a)}{c\mu - \lambda}, \quad a = \lambda/\mu$ | Erlang-C Formula | `DES-S-04` |
| **$M/M/1/K$** | $\pi_K = \frac{(1-\rho)\rho^K}{1-\rho^{K+1}}$ | Erlang-B Blocking | `DES-S-05` |
| **$M/D/1$** | $W_q = \frac{\rho \cdot d}{2(1-\rho)}$ | Pollaczek-Khinchine ($c_s^2 = 0$) | `DES-S-06` |
| **$M/G/1$** | $W_q = \frac{\lambda E[S^2]}{2(1-\rho)}$ | Pollaczek-Khinchine Formula | `DES-S-07` |
| **Jackson Network** | $\lambda_i = \gamma_i + \sum_j \lambda_j P_{ji}$ | Jackson's Theorem (1957) | `DES-M-01`, `02` |
| **Priority Queue** | $W_{q,1} = \frac{R}{1-\rho_1}, \quad W_{q,2} = \frac{R}{(1-\rho_1)(1-\rho_1-\rho_2)}$ | Cobham (1954) | `DES-M-03` |
| **Machine Failures** | $A = \frac{\beta}{\alpha + \beta}$ | Markov Availability Model | `DES-M-04` |
| **NHPP** | $E[N(T)] = \int_0^T \lambda(t) dt$ | Lewis-Shedler Thinning (1979) | `DES-M-06` |
| **Fork-Join** | $E[W_{\text{join}}] \ge \max(E[S_1], \dots, E[S_k])$ | Baccelli-Makowski Bound (1989) | `DES-M-07` |

---

## Single-Server Queue ($M/M/1$)

With Poisson arrival rate $\lambda$ and exponential service rate $\mu$, traffic intensity is $\rho = \lambda / \mu < 1$.

In steady state:
- Probability of $n$ entities in system: $P_n = (1 - \rho)\rho^n$
- Average number in system: $L = \frac{\rho}{1 - \rho}$
- Average time in system (sojourn): $W = \frac{1}{\mu(1 - \rho)}$
- Average wait in queue: $W_q = \frac{\rho}{\mu(1 - \rho)}$

```julia
using SimDES
stats = run_mm1!(1.0, 2.0; n_arrivals = 100_000, seed = 42)
```

---

## General Service ($M/G/1$) & The Pollaczek-Khinchine Formula

For general independent service times with mean $E[S] = 1/\mu$ and second moment $E[S^2]$:

$$W_q = \frac{\lambda E[S^2]}{2(1 - \rho)}$$

Using squared coefficient of variation $c_s^2 = \frac{\operatorname{Var}(S)}{(E[S])^2}$:

$$W_q = \frac{\rho}{1 - \rho} \frac{1 + c_s^2}{2} E[S]$$

- For **$M/M/1$** (exponential service): $c_s^2 = 1 \implies W_q = \frac{\rho}{\mu(1 - \rho)}$.
- For **$M/D/1$** (deterministic service): $c_s^2 = 0 \implies W_q = \frac{\rho}{2\mu(1 - \rho)}$ (exact 50% wait reduction!).
- For **Erlang-$k$ service**: $c_s^2 = 1/k \implies E[S^2] = \frac{k+1}{k\mu^2}$.

---

## Open Jackson Networks

An open network of $M$ stations with external Poisson arrival rates $\gamma_i$ and routing probabilities $P_{ij}$.

The total arrival rate $\lambda_i$ at each station satisfies the linear traffic equations:

$$\lambda_i = \gamma_i + \sum_{j=1}^M \lambda_j P_{ji}$$

**Jackson's Theorem:** If $\rho_i = \lambda_i / \mu_i < 1$ for all stations $i$, the joint stationary distribution has a product form:

$$\pi(n_1, n_2, \dots, n_M) = \prod_{i=1}^M (1 - \rho_i) \rho_i^{n_i}$$

Each station behaves in steady state as if it were an independent $M/M/1$ queue!
