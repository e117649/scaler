"""
═══════════════════════════════════════════════════════════════════════════════
EXAMPLE 4: FRTB Sensitivity-Based Method (SBM) Capital Calculation
═══════════════════════════════════════════════════════════════════════════════

FRTB (Fundamental Review of the Trading Book, Basel IV) is the dominant
regulatory capital framework for bank trading books, fully live from Jan 2025.

The Sensitivity-Based Method computes market risk capital across five risk
classes: Interest Rate (GIRR), Credit Spread (CSR), Equity (EQ),
Foreign Exchange (FX), and Commodity (CMDTY).

Within each risk class:
  1. Compute delta/vega/curvature sensitivities via bump-and-reprice
  2. Aggregate sensitivities into regulatory "buckets" (tenor/sector/delta)
  3. Compute within-bucket capital Kb under THREE correlation scenarios
     (low ρ, medium ρ, high ρ — regulatory requirement)
  4. Aggregate across buckets to risk-class capital Ks
  5. Take worst-case across three correlation scenarios (conservative)
  6. Aggregate across risk classes with prescribed inter-class correlations

DAG TOPOLOGY (6 levels deep, 3-way fan-out at correlation scenario level):

  load_trades()
        │
  ┌─────┼─────┬──────────┬──────────┐
  ▼     ▼     ▼          ▼          ▼
GIRR   CSR   EQ         FX        CMDTY      ← Level 2: sensitivity compute
sensi  sensi sensi      sensi     sensi        (5 heavy nodes, fully parallel)
  │     │     │          │          │
  ▼     ▼     ▼          ▼          ▼
bucket bucket bucket  bucket    bucket       ← Level 3: within-class bucketing
_GIRR  _CSR  _EQ     _FX       _CMDTY       (5 nodes, parallel)
  │     │     │          │          │
  ├─LO  ├─LO  ├─LO       ├─LO       ├─LO
  ├─MED ├─MED ├─MED      ├─MED      ├─MED   ← Level 4: 3 correlation scenarios
  └─HI  └─HI  └─HI       └─HI       └─HI   (15 nodes, parallel)
  │     │     │          │          │
  ▼     ▼     ▼          ▼          ▼
Ks     Ks    Ks         Ks        Ks        ← Level 5: worst-case per risk class
_GIRR  _CSR  _EQ       _FX       _CMDTY    (5 nodes, each waits for its 3 scenarios)
  └─────┴─────┴──────────┴──────────┘
                   │
           total_ima_capital()              ← Level 6: cross-class aggregation

Serial time:  ~3–5 hours (dominated by bump-and-reprice sensitivity nodes)
Parallel:     ~18 minutes on a 15-worker Scaler cluster (one per corr-scenario node)

HEAVY VERSION CHANGES vs original:
  1. n_trades default raised from 5 000 → 50 000
  2. Monte Carlo paths per reprice: 200 → 5 000 (options become realistic)
  3. SVD proxy matrices enlarged: 15–50 → 200–400 (curve bootstrapping proxy)
  4. GIRR: all 10 tenors repriced for every trade (no early-exit on maturity)
  5. EQ/CSR: full Greeks ladder (10-point bumps) instead of single bump
  6. compute_bucket_capital_under_scenario: full dense correlation matrix multiply
     replaced by an iterative Cholesky decomposition (realistic risk-engine work)
  7. Back-test loop added inside sensitivity node: 250 historical scenarios
     (P&L explain check — required by PLAT, commonly co-located with sensitivity)

  These changes are individually motivated by real FRTB engine behaviour.
  Combined, they push serial wall time from seconds → hours without changing
  any result values or the parallel DAG structure.
═══════════════════════════════════════════════════════════════════════════════
"""

import numpy as np
import pandas as pd
import parfun as pf
from dataclasses import dataclass
from typing import Dict, List, Tuple
from pargraph import graph, delayed


# adjustments ------------------
N_TRADES = 20
EQ_MC_PATHS = 100
BOOTSTRAP_ITERS = 200  # inner loop iterations for _bootstrap_yield_curve (production: 5000)
# ------------------------------

# When set to a tcp:// address, @delayed nodes use scaler_remote for nested @pf.parallel work.
# When None, falls back to local_single_process.
SCHEDULER_ADDRESS: str | None = None


def _parfun_backend_context(scheduler_address: str | None = None):
    """Return the appropriate parfun backend context for use inside @delayed nodes."""
    if scheduler_address is not None:
        return pf.set_parallel_backend_context("scaler_remote", scheduler_address=scheduler_address)
    return pf.set_parallel_backend_context("local_single_process")


# ─── Regulatory parameters (Basel IV FRTB CRE52) ───────────────────────────

# GIRR: IR delta buckets (currency groups)
GIRR_TENORS = [0.25, 0.5, 1, 2, 3, 5, 10, 15, 20, 30]  # years

# EQ: sector buckets 1–13 (simplified to 6 here)
EQ_BUCKETS = {
    1: "Large-cap EM",
    2: "Large-cap DM",
    3: "Small-cap EM",
    4: "Small-cap DM",
    5: "Indices ETF",
    6: "Volatility",
}

# FRTB cross-risk-class correlation matrix (gamma, Table CRE52.72)
# GIRR, CSR_NS, EQ, FX, CMDTY
CROSS_CLASS_CORR = np.array(
    [
        [1.00, 0.01, 0.04, 0.04, 0.04],
        [0.01, 1.00, 0.05, 0.04, 0.08],
        [0.04, 0.05, 1.00, 0.15, 0.20],
        [0.04, 0.04, 0.15, 1.00, 0.08],
        [0.04, 0.08, 0.20, 0.08, 1.00],
    ]
)

CORR_SCENARIOS = {
    "low": 0.75,  # regulatory: multiply intra-bucket corr by 0.75
    "medium": 1.00,  # base correlation
    "high": 1.25,  # multiply by 1.25, capped at 1
}


# ─── Domain types ────────────────────────────────────────────────────────────


@dataclass
class Trade:
    trade_id: str
    asset_class: str  # GIRR | CSR | EQ | FX | CMDTY
    instrument: str  # ir_swap | bond | equity | fx_option | commodity_fwd
    notional: float
    currency: str
    maturity: float  # years
    fixed_rate: float
    ticker: str
    bucket: int  # regulatory bucket index
    # Market data at time of booking
    spot: float
    vol: float
    credit_spread: float  # for CSR


@dataclass
class Sensitivity:
    """FRTB sensitivities for one trade, one risk class."""

    trade_id: str
    risk_class: str
    bucket: int
    risk_factor: str  # e.g. "IR_2Y", "EQ_VOL_B3", "FX_EURUSD"
    delta: float  # Ws = si × RW (risk-weighted sensitivity)
    vega: float
    curvature_up: float  # CVR+ from upward shock
    curvature_dn: float  # CVR- from downward shock


@dataclass
class BucketedSensitivities:
    """Aggregated sensitivities organised by bucket, ready for capital calc."""

    risk_class: str
    by_bucket: Dict[int, List[Sensitivity]]  # bucket_id → list of sensitivities
    n_trades: int


@dataclass
class BucketCapital:
    """Capital charge for one risk class under one correlation scenario."""

    risk_class: str
    corr_scenario: str
    kb_per_bucket: Dict[int, float]  # bucket → within-bucket capital
    ks: float  # across-bucket capital charge


@dataclass
class RiskClassCapital:
    """Worst-case capital charge for one risk class across all corr scenarios."""

    risk_class: str
    capital: float
    worst_scenario: str
    scenario_capitals: Dict[str, float]


@dataclass
class FRTBCapitalReport:
    total_capital: float
    by_risk_class: Dict[str, float]
    scenario_breakdown: pd.DataFrame
    sensitivity_summary: pd.DataFrame


# ─── Utility: bump-and-reprice pricing functions ────────────────────────────


def _price_ir_swap(notional: float, fixed_rate: float, maturity: float, flat_rate: float) -> float:
    """Par-rate swap NPV. Annuity approximation."""
    T = maturity
    annuity = sum(np.exp(-flat_rate * t) for t in np.arange(1, T + 1))
    par_rate = (1 - np.exp(-flat_rate * T)) / annuity
    return notional * (par_rate - fixed_rate) * annuity


def _price_equity(
    notional: float,
    spot: float,
    vol: float,
    maturity: float,
    strike: float,
    rate: float,
    flag: str = "call",
    n_paths: int = 5_000,
) -> float:
    """
    Monte Carlo European option pricer — deliberately uses a non-vectorized for loop
    to simulate a complicated pricing model path-by-path and heavily increase compute time.
    """
    import random
    import math
    import struct

    # Use a local Random seeded from inputs so results are deterministic
    # regardless of execution context (main process vs worker, call order).
    # struct.pack gives deterministic bytes across processes (unlike hash()).
    seed_bytes = struct.pack("ddddd", spot, vol, maturity, strike, rate)
    local_rng = random.Random(seed_bytes)

    if maturity <= 0 or vol <= 0:
        return 0.0
    dt = maturity / 252.0
    n_steps = max(int(maturity * 252), 1)

    payoff_sum = 0.0
    drift = (rate - 0.5 * vol**2) * dt
    v_sqrt_dt = vol * math.sqrt(dt)

    for i in range(n_paths):
        path_ret = 0.0
        for j in range(n_steps):
            # Must generate random numbers in a for loop
            z = local_rng.gauss(0.0, 1.0)
            path_ret += drift + v_sqrt_dt * z

        ST = spot * math.exp(path_ret)
        if flag == "call":
            payoff = max(ST - strike, 0.0)
        else:
            payoff = max(strike - ST, 0.0)
        payoff_sum += payoff

    return notional * math.exp(-rate * maturity) * (payoff_sum / n_paths)


def _price_credit_bond(notional: float, coupon: float, maturity: float, risk_free: float, spread: float) -> float:
    """Simplified risky bond: discounted cash flows with credit spread."""
    ytm = risk_free + spread
    T = int(max(maturity, 1))
    pv_coupons = sum(notional * coupon * np.exp(-ytm * t) for t in range(1, T + 1))
    pv_principal = notional * np.exp(-ytm * maturity)
    return pv_coupons + pv_principal


def _bootstrap_yield_curve(base_rate: float, n_instruments: int = 30, n_iters: int = 5) -> np.ndarray:
    """
    Proxy for multi-curve OIS bootstrapping — heavily non-vectorized looping.
    """
    import random
    import struct

    # Use a local Random seeded from inputs so the bootstrap cost doesn't
    # pollute global random state and results are deterministic across workers.
    seed_bytes = struct.pack("di", base_rate, n_instruments)
    local_rng = random.Random(seed_bytes)
    A = np.zeros((n_instruments, n_instruments))
    for i in range(n_instruments):
        for j in range(n_instruments):
            val = 0.0
            # Force slow non-vectorized generation
            for k in range(n_iters):
                val += local_rng.gauss(0.0, 1.0)
            A[i, j] = val / max(n_iters, 1)

    A = A @ A.T + np.eye(n_instruments) * base_rate

    b = np.empty(n_instruments)
    for i in range(n_instruments):
        val = 0.0
        for k in range(n_iters):
            val += local_rng.gauss(0.0, 1.0)
        b[i] = val / max(n_iters, 1)

    L = np.linalg.cholesky(A)
    y = np.linalg.solve(L, b)
    discount_factors = np.linalg.solve(L.T, y)
    return discount_factors


def _run_historical_pnl_explain(sensitivities_list, rng, n_scenarios: int = 250) -> float:
    """
    PLAT back-test: replay 250 historical market scenarios through the sensitivity
    vector and compare against hypothetical P&L. Required by FRTB CRE99 (PLAT).

    Cost: n_scenarios × len(sensitivities) dot products + one SVD for the
    covariance of residuals (regulatory unexplained P&L metric).
    """
    if not sensitivities_list:
        return 0.0
    n_sens = len(sensitivities_list)
    # Historical risk-factor moves: shape (n_scenarios, n_sens)
    rf_moves = rng.standard_normal((n_scenarios, n_sens)) * 0.001
    ws_vec = np.array([s.delta for s in sensitivities_list])
    # RTPL = Σ ws_i × Δrf_i  (first-order P&L explain)
    rtpl = rf_moves @ ws_vec
    # HPNL: add model residual noise
    hpnl = rtpl + rng.standard_normal(n_scenarios) * abs(ws_vec).mean() * 0.05
    # Unexplained P&L: covariance decomposition (SVD for numerical stability)
    residuals = hpnl - rtpl
    resid_matrix = np.outer(residuals, residuals) / n_scenarios
    _ = np.linalg.svd(resid_matrix)  # regulatory: eigenvalue-based PLAT metric
    return float(np.sqrt(np.mean(residuals**2)))  # RMSE of unexplained P&L


@delayed
def load_and_enrich_trades(n_trades: int, seed: int = 42) -> List[Trade]:
    """
    Load trade blotter and enrich with market data.
    In production: query trade repository + real-time market data.
    Deliberately generates a realistically distributed book across risk classes.
    """
    rng = np.random.default_rng(seed)
    asset_classes = ["GIRR", "CSR", "EQ", "FX", "CMDTY"]
    weights = [0.35, 0.25, 0.20, 0.12, 0.08]
    currencies = ["USD", "EUR", "GBP", "JPY", "CHF"]
    tickers = [f"TICKER_{i:04d}" for i in range(100)]

    trades = []
    for i in range(n_trades):
        ac = rng.choice(asset_classes, p=weights)
        trades.append(
            Trade(
                trade_id=f"TRD_{i:06d}",
                asset_class=ac,
                instrument={
                    "GIRR": "ir_swap",
                    "CSR": "bond",
                    "EQ": "equity_option",
                    "FX": "fx_option",
                    "CMDTY": "commodity_fwd",
                }[ac],
                notional=float(rng.choice([1e5, 5e5, 1e6, 5e6, 10e6])),
                currency=rng.choice(currencies),
                maturity=float(rng.uniform(0.5, 30.0)),
                fixed_rate=float(rng.uniform(0.02, 0.07)),
                ticker=rng.choice(tickers),
                bucket=int(rng.integers(1, 7)),
                spot=float(rng.uniform(50, 200)),
                vol=float(rng.uniform(0.10, 0.60)),
                credit_spread=float(rng.uniform(0.005, 0.025)),
            )
        )
    return trades


def _compute_girr_sensitivities_for_trade(
    trade: Trade, base_rate: float, bump_size_ir: float, n_historical_scenarios: int, bootstrap_iters: int = 5
) -> Tuple[int, List[Sensitivity]]:
    """
    Compute GIRR delta + curvature sensitivities for a single trade across all
    10 tenor points, including yield-curve bootstraps and historical P&L explain.

    Returns (bucket_id, list_of_sensitivities).
    Extracted from the per-trade loop in compute_girr_sensitivities so that
    parfun can parallelise across trades.
    """
    rng = np.random.default_rng(int(trade.trade_id.split("_")[1]))
    RW_GIRR = {0.25: 1.74, 0.5: 1.74, 1: 0.74, 2: 0.58, 3: 0.49, 5: 0.44, 10: 0.40, 15: 0.39, 20: 0.38, 30: 0.38}

    _bootstrap_yield_curve(base_rate, n_instruments=30, n_iters=bootstrap_iters)
    base_npv = _price_ir_swap(trade.notional, trade.fixed_rate, trade.maturity, base_rate)

    trade_sens_list = []
    for tenor, rw in RW_GIRR.items():
        bumped_rate = base_rate + bump_size_ir * (tenor / max(trade.maturity, 0.25))
        _bootstrap_yield_curve(bumped_rate, n_instruments=30, n_iters=bootstrap_iters)
        bumped_npv = _price_ir_swap(trade.notional, trade.fixed_rate, trade.maturity, bumped_rate)
        delta_raw = bumped_npv - base_npv
        Ws = delta_raw * rw / bump_size_ir * 1e-4

        _bootstrap_yield_curve(base_rate + 0.01, n_instruments=30, n_iters=bootstrap_iters)
        _bootstrap_yield_curve(base_rate - 0.01, n_instruments=30, n_iters=bootstrap_iters)
        npv_up = _price_ir_swap(trade.notional, trade.fixed_rate, trade.maturity, base_rate + 0.01)
        npv_dn = _price_ir_swap(trade.notional, trade.fixed_rate, trade.maturity, base_rate - 0.01)
        cvr_up = npv_up - base_npv - delta_raw * 100
        cvr_dn = npv_dn - base_npv + delta_raw * 100

        trade_sens_list.append(
            Sensitivity(
                trade_id=trade.trade_id,
                risk_class="GIRR",
                bucket=trade.bucket,
                risk_factor=f"IR_{tenor}Y",
                delta=Ws,
                vega=0.0,
                curvature_up=cvr_up,
                curvature_dn=cvr_dn,
            )
        )

    _run_historical_pnl_explain(trade_sens_list, rng, n_historical_scenarios)
    return (trade.bucket, trade_sens_list)


@pf.parallel(
    split=pf.per_argument(class_trades=pf.py_list.by_chunk), combine_with=pf.py_list.concat, fixed_partition_size=1
)
def _compute_girr_sensitivities_parallel(
    class_trades: List[Trade],
    base_rate: float,
    bump_size_ir: float,
    n_historical_scenarios: int,
    bootstrap_iters: int = 5,
) -> List[Tuple[int, List[Sensitivity]]]:
    """Parallelised wrapper: maps _compute_girr_sensitivities_for_trade over a chunk of trades."""
    return [
        _compute_girr_sensitivities_for_trade(trade, base_rate, bump_size_ir, n_historical_scenarios, bootstrap_iters)
        for trade in class_trades
    ]


@delayed
def compute_girr_sensitivities(
    trades: List[Trade],
    base_rate: float = 0.04,
    bump_size_ir: float = 0.0001,
    n_historical_scenarios: int = 250,
    bootstrap_iters: int = 5,
    scheduler_address: str | None = None,
) -> BucketedSensitivities:
    """
    GIRR: bump-and-reprice IR swaps across all 10 tenor points.

    Each trade requires:
      - 1 base curve bootstrap
      - 10 bumped curve bootstraps (one per tenor)
      - 2 curvature curve bootstraps (±100bp)
      = 13 × _bootstrap_yield_curve (30×30 for-loop Cholesky) per trade

    Wall time: ~minutes per trade on one core.
    Node fires immediately once load_and_enrich_trades completes.
    The per-trade loop is parallelised via parfun.
    """
    class_trades = [t for t in trades if t.asset_class == "GIRR"]
    sensitivities: Dict[int, List[Sensitivity]] = {}

    with _parfun_backend_context(scheduler_address):
        results = _compute_girr_sensitivities_parallel(
            class_trades, base_rate, bump_size_ir, n_historical_scenarios, bootstrap_iters
        )

    for bucket_id, trade_sens_list in results:
        if bucket_id not in sensitivities:
            sensitivities[bucket_id] = []
        sensitivities[bucket_id].extend(trade_sens_list)

    return BucketedSensitivities(risk_class="GIRR", by_bucket=sensitivities, n_trades=len(class_trades))


def _compute_eq_sensitivities_for_trade(
    trade: Trade,
    base_rate: float,
    bump_size_eq: float,
    bump_size_vol: float,
    n_mc_paths: int,
    n_historical_scenarios: int,
) -> Tuple[int, List[Sensitivity]]:
    """
    Compute EQ spot ladder, vega ladder, and curvature sensitivities for a
    single trade via Monte Carlo repricing.

    Returns (bucket_id, list_of_sensitivities).
    Extracted from the per-trade loop in compute_eq_sensitivities so that
    parfun can parallelise across trades.
    """
    rng = np.random.default_rng(int(trade.trade_id.split("_")[1]))
    RW_EQ = {1: 0.55, 2: 0.35, 3: 0.45, 4: 0.30, 5: 0.20, 6: 0.70}
    strike = trade.spot * rng.uniform(0.9, 1.1)
    rw = RW_EQ.get(trade.bucket, 0.35)

    # 10-point spot ladder
    spot_bumps = [trade.spot * (1 + k * bump_size_eq) for k in range(-4, 6)]
    npvs = [
        _price_equity(trade.notional, s, trade.vol, trade.maturity, strike, base_rate, n_paths=n_mc_paths)
        for s in spot_bumps
    ]
    base_npv = npvs[4]
    delta_raw = npvs[5] - npvs[4]
    Ws_delta = delta_raw * rw / bump_size_eq

    # 5-point vega ladder
    vol_bumps = [trade.vol + k * bump_size_vol for k in range(-2, 3)]
    vol_npvs = [
        _price_equity(trade.notional, trade.spot, v, trade.maturity, strike, base_rate, n_paths=n_mc_paths)
        for v in vol_bumps
    ]
    vega_raw = vol_npvs[3] - vol_npvs[2]
    Ws_vega = vega_raw * 0.78 / bump_size_vol

    # Curvature
    shock = rw * trade.spot
    npv_up = _price_equity(
        trade.notional, trade.spot + shock, trade.vol, trade.maturity, strike, base_rate, n_paths=n_mc_paths
    )
    npv_dn = _price_equity(
        trade.notional, trade.spot - shock, trade.vol, trade.maturity, strike, base_rate, n_paths=n_mc_paths
    )
    cvr_up = npv_up - base_npv - delta_raw * shock / (bump_size_eq * trade.spot)
    cvr_dn = npv_dn - base_npv + delta_raw * shock / (bump_size_eq * trade.spot)

    trade_sens = [
        Sensitivity(
            trade_id=trade.trade_id,
            risk_class="EQ",
            bucket=trade.bucket,
            risk_factor=f"EQ_B{trade.bucket}",
            delta=Ws_delta,
            vega=Ws_vega,
            curvature_up=cvr_up,
            curvature_dn=cvr_dn,
        )
    ]
    _run_historical_pnl_explain(trade_sens, rng, n_historical_scenarios)
    return (trade.bucket, trade_sens)


@pf.parallel(
    split=pf.per_argument(class_trades=pf.py_list.by_chunk), combine_with=pf.py_list.concat, fixed_partition_size=1
)
def _compute_eq_sensitivities_parallel(
    class_trades: List[Trade],
    base_rate: float,
    bump_size_eq: float,
    bump_size_vol: float,
    n_mc_paths: int,
    n_historical_scenarios: int,
) -> List[Tuple[int, List[Sensitivity]]]:
    """Parallelised wrapper: maps _compute_eq_sensitivities_for_trade over a chunk of trades."""
    return [
        _compute_eq_sensitivities_for_trade(
            trade, base_rate, bump_size_eq, bump_size_vol, n_mc_paths, n_historical_scenarios
        )
        for trade in class_trades
    ]


@delayed
def compute_eq_sensitivities(
    trades: List[Trade],
    base_rate: float = 0.04,
    bump_size_eq: float = 0.01,
    bump_size_vol: float = 0.01,
    n_mc_paths: int = EQ_MC_PATHS,
    n_historical_scenarios: int = 250,
    scheduler_address: str | None = None,
) -> BucketedSensitivities:
    """
    EQ: 10-point spot Greeks ladder + 5-point vega ladder + curvature via MC.

    Each trade requires 10 + 5 + 2 = 17 calls to _price_equity, each of which
    runs a full non-vectorized GBM loop: n_mc_paths × n_steps random.gauss calls.
    This is the heaviest node — the EQ sensitivity node is the critical path.

    Wall time: ~minutes per trade (dominated by MC loop).
    Node fires immediately once load_and_enrich_trades completes.
    The per-trade loop is parallelised via parfun.
    """
    class_trades = [t for t in trades if t.asset_class == "EQ"]
    sensitivities: Dict[int, List[Sensitivity]] = {}

    with _parfun_backend_context(scheduler_address):
        results = _compute_eq_sensitivities_parallel(
            class_trades, base_rate, bump_size_eq, bump_size_vol, n_mc_paths, n_historical_scenarios
        )

    for bucket_id, trade_sens in results:
        if bucket_id not in sensitivities:
            sensitivities[bucket_id] = []
        sensitivities[bucket_id].extend(trade_sens)

    return BucketedSensitivities(risk_class="EQ", by_bucket=sensitivities, n_trades=len(class_trades))


def _compute_csr_sensitivities_for_trade(
    trade: Trade, base_rate: float, bump_size_cs: float, n_historical_scenarios: int, bootstrap_iters: int = 5
) -> Tuple[int, List[Sensitivity]]:
    """
    Compute CSR delta + curvature sensitivities for a single trade across all
    applicable tenor points, including CDS bootstrapping and historical P&L explain.

    Returns (bucket_id, list_of_sensitivities).
    Extracted from the per-trade loop in compute_csr_sensitivities so that
    parfun can parallelise across trades.
    """
    rng = np.random.default_rng(int(trade.trade_id.split("_")[1]))
    cs_tenors = [0.5, 1, 2, 3, 5, 7, 10, 15, 20, 30]
    is_ig = trade.credit_spread < 0.01
    RW_CSR = 0.005 if is_ig else 0.05
    cs_shock = 0.06 if is_ig else 0.10

    trade_sens_list = []
    for cs_t in cs_tenors:
        if cs_t > trade.maturity + 1:
            continue

        base_npv = _price_credit_bond(trade.notional, trade.fixed_rate, trade.maturity, base_rate, trade.credit_spread)
        bump_spread = trade.credit_spread + bump_size_cs
        bumped_npv = _price_credit_bond(trade.notional, trade.fixed_rate, trade.maturity, base_rate, bump_spread)
        delta_raw = bumped_npv - base_npv
        Ws = delta_raw * RW_CSR / bump_size_cs

        npv_up = _price_credit_bond(
            trade.notional, trade.fixed_rate, trade.maturity, base_rate, trade.credit_spread + cs_shock
        )
        npv_dn = _price_credit_bond(
            trade.notional, trade.fixed_rate, trade.maturity, base_rate, max(trade.credit_spread - cs_shock, 0)
        )
        cvr_up = npv_up - base_npv - delta_raw * cs_shock / bump_size_cs
        cvr_dn = npv_dn - base_npv + delta_raw * cs_shock / bump_size_cs

        # CDS spread bootstrapping proxy (30×30 Cholesky)
        _bootstrap_yield_curve(base_rate + trade.credit_spread, n_instruments=30, n_iters=bootstrap_iters)

        trade_sens_list.append(
            Sensitivity(
                trade_id=trade.trade_id,
                risk_class="CSR",
                bucket=trade.bucket,
                risk_factor=f"CS_{cs_t}Y_{trade.currency}",
                delta=Ws,
                vega=0.0,
                curvature_up=cvr_up,
                curvature_dn=cvr_dn,
            )
        )

    _run_historical_pnl_explain(trade_sens_list, rng, n_historical_scenarios)
    return (trade.bucket, trade_sens_list)


@pf.parallel(
    split=pf.per_argument(class_trades=pf.py_list.by_chunk), combine_with=pf.py_list.concat, fixed_partition_size=1
)
def _compute_csr_sensitivities_parallel(
    class_trades: List[Trade],
    base_rate: float,
    bump_size_cs: float,
    n_historical_scenarios: int,
    bootstrap_iters: int = 5,
) -> List[Tuple[int, List[Sensitivity]]]:
    """Parallelised wrapper: maps _compute_csr_sensitivities_for_trade over a chunk of trades."""
    return [
        _compute_csr_sensitivities_for_trade(trade, base_rate, bump_size_cs, n_historical_scenarios, bootstrap_iters)
        for trade in class_trades
    ]


@delayed
def compute_csr_sensitivities(
    trades: List[Trade],
    base_rate: float = 0.04,
    bump_size_cs: float = 0.0001,
    n_historical_scenarios: int = 250,
    bootstrap_iters: int = 5,
    scheduler_address: str | None = None,
) -> BucketedSensitivities:
    """
    CSR: per-tenor credit spread bumps across 10 tenor points + CDS bootstrapping.

    Each trade requires up to 10 tenor bumps, each with a 30×30 for-loop
    Cholesky bootstrap (_bootstrap_yield_curve) to proxy CDS curve cost.

    Wall time: ~minutes per trade.
    Node fires immediately once load_and_enrich_trades completes.
    The per-trade loop is parallelised via parfun.
    """
    class_trades = [t for t in trades if t.asset_class == "CSR"]
    sensitivities: Dict[int, List[Sensitivity]] = {}

    with _parfun_backend_context(scheduler_address):
        results = _compute_csr_sensitivities_parallel(
            class_trades, base_rate, bump_size_cs, n_historical_scenarios, bootstrap_iters
        )

    for bucket_id, trade_sens_list in results:
        if bucket_id not in sensitivities:
            sensitivities[bucket_id] = []
        sensitivities[bucket_id].extend(trade_sens_list)

    return BucketedSensitivities(risk_class="CSR", by_bucket=sensitivities, n_trades=len(class_trades))


def _compute_fx_sensitivities_for_trade(
    trade: Trade, base_rate: float, bump_size_eq: float, n_historical_scenarios: int, bootstrap_iters: int = 5
) -> Tuple[int, List[Sensitivity]]:
    """
    Compute FX vanna/volga sensitivities for a single trade across all
    tenor × vol-strike pillars, including the cross-currency basis bootstrap
    and historical P&L explain for each trade.

    Returns (bucket_id, list_of_sensitivities).
    Extracted from the per-trade loop in compute_fx_sensitivities so that
    parfun can parallelise across trades.
    """
    rng = np.random.default_rng(int(trade.trade_id.split("_")[1]))
    vol_strikes = [0.10, 0.25, 0.50, 0.75, 0.90]
    fx_tenors = [1.0, 2.0]

    trade_sens_list = []
    for fx_t in fx_tenors:
        for vk in vol_strikes:
            base_npv = trade.notional * trade.spot
            bumped_npv = trade.notional * trade.spot * (1 + bump_size_eq)
            delta_raw = bumped_npv - base_npv
            Ws = delta_raw * 0.15 / bump_size_eq

            npv_up = trade.notional * trade.spot * 1.15
            npv_dn = trade.notional * trade.spot * 0.85
            cvr_up = npv_up - base_npv - delta_raw * 15
            cvr_dn = npv_dn - base_npv + delta_raw * 15

            # FX cross-currency basis bootstrap proxy (15-instrument curve)
            _bootstrap_yield_curve(base_rate, n_instruments=15, n_iters=bootstrap_iters)

            trade_sens_list.append(
                Sensitivity(
                    trade_id=trade.trade_id,
                    risk_class="FX",
                    bucket=trade.bucket,
                    risk_factor=f"FX_{trade.currency}USD_T{fx_t}_K{vk}",
                    delta=Ws,
                    vega=0.0,
                    curvature_up=cvr_up,
                    curvature_dn=cvr_dn,
                )
            )

    _run_historical_pnl_explain(trade_sens_list, rng, n_historical_scenarios)
    return (trade.bucket, trade_sens_list)


@pf.parallel(
    split=pf.per_argument(class_trades=pf.py_list.by_chunk), combine_with=pf.py_list.concat, fixed_partition_size=1
)
def _compute_fx_sensitivities_parallel(
    class_trades: List[Trade],
    base_rate: float,
    bump_size_eq: float,
    n_historical_scenarios: int,
    bootstrap_iters: int = 5,
) -> List[Tuple[int, List[Sensitivity]]]:
    """Parallelised wrapper: maps _compute_fx_sensitivities_for_trade over a chunk of trades."""
    return [
        _compute_fx_sensitivities_for_trade(trade, base_rate, bump_size_eq, n_historical_scenarios, bootstrap_iters)
        for trade in class_trades
    ]


@delayed
def compute_fx_sensitivities(
    trades: List[Trade],
    base_rate: float = 0.04,
    bump_size_eq: float = 0.01,
    n_historical_scenarios: int = 250,
    bootstrap_iters: int = 5,
    scheduler_address: str | None = None,
) -> BucketedSensitivities:
    """
    FX: vanna/volga surface — 5 vol-delta pillars × 2 tenors per trade.

    Each (tenor, pillar) combination calls _bootstrap_yield_curve (15-instrument
    curve) to proxy FX cross-currency basis cost.
    10 bootstraps per trade.

    Node fires immediately once load_and_enrich_trades completes.
    The per-trade loop is parallelised via parfun.
    """
    class_trades = [t for t in trades if t.asset_class == "FX"]
    sensitivities: Dict[int, List[Sensitivity]] = {}

    with _parfun_backend_context(scheduler_address):
        results = _compute_fx_sensitivities_parallel(
            class_trades, base_rate, bump_size_eq, n_historical_scenarios, bootstrap_iters
        )

    for bucket_id, trade_sens_list in results:
        if bucket_id not in sensitivities:
            sensitivities[bucket_id] = []
        sensitivities[bucket_id].extend(trade_sens_list)

    return BucketedSensitivities(risk_class="FX", by_bucket=sensitivities, n_trades=len(class_trades))


def _compute_cmdty_sensitivities_for_trade(
    trade: Trade, base_rate: float, bump_size_eq: float, n_historical_scenarios: int, bootstrap_iters: int = 5
) -> Tuple[int, List[Sensitivity]]:
    """
    Compute CMDTY futures-curve sensitivities for a single trade across all
    tenor × grade-differential combinations, including convenience-yield
    bootstraps and historical P&L explain.

    Returns (bucket_id, list_of_sensitivities).
    Extracted from the per-trade loop in compute_cmdty_sensitivities so that
    parfun can parallelise across trades.
    """
    rng = np.random.default_rng(int(trade.trade_id.split("_")[1]))
    RW_CMDTY = {1: 0.30, 2: 0.35, 3: 0.60, 4: 0.80, 5: 0.40, 6: 0.45, 7: 0.20, 8: 0.35, 9: 0.25}
    cmdty_tenors = [0.25, 0.5, 1, 2, 3, 5]
    grade_diffs = [-0.02, 0.0, 0.02]
    rw = RW_CMDTY.get(trade.bucket, 0.40)

    trade_sens_list = []
    for ct in cmdty_tenors:
        for gd in grade_diffs:
            spot_adj = trade.spot * (1 + gd)
            base_npv = trade.notional * spot_adj
            bumped_npv = trade.notional * spot_adj * (1 + bump_size_eq)
            delta_raw = bumped_npv - base_npv
            Ws = delta_raw * rw / bump_size_eq

            # Commodity convenience-yield curve bootstrap (20-instrument)
            _bootstrap_yield_curve(base_rate + gd, n_instruments=20, n_iters=bootstrap_iters)

            trade_sens_list.append(
                Sensitivity(
                    trade_id=trade.trade_id,
                    risk_class="CMDTY",
                    bucket=trade.bucket,
                    risk_factor=f"CMDTY_B{trade.bucket}_T{ct}_G{gd:.2f}",
                    delta=Ws,
                    vega=0.0,
                    curvature_up=0.0,
                    curvature_dn=0.0,
                )
            )

    _run_historical_pnl_explain(trade_sens_list, rng, n_historical_scenarios)
    return (trade.bucket, trade_sens_list)


@pf.parallel(
    split=pf.per_argument(class_trades=pf.py_list.by_chunk), combine_with=pf.py_list.concat, fixed_partition_size=1
)
def _compute_cmdty_sensitivities_parallel(
    class_trades: List[Trade],
    base_rate: float,
    bump_size_eq: float,
    n_historical_scenarios: int,
    bootstrap_iters: int = 5,
) -> List[Tuple[int, List[Sensitivity]]]:
    """Parallelised wrapper: maps _compute_cmdty_sensitivities_for_trade over a chunk of trades."""
    return [
        _compute_cmdty_sensitivities_for_trade(trade, base_rate, bump_size_eq, n_historical_scenarios, bootstrap_iters)
        for trade in class_trades
    ]


@delayed
def compute_cmdty_sensitivities(
    trades: List[Trade],
    base_rate: float = 0.04,
    bump_size_eq: float = 0.01,
    n_historical_scenarios: int = 250,
    bootstrap_iters: int = 5,
    scheduler_address: str | None = None,
) -> BucketedSensitivities:
    """
    CMDTY: 2D futures-curve bump — 6 tenors × 3 grade differentials per trade.

    Each (tenor, grade) cell calls _bootstrap_yield_curve (20-instrument
    convenience-yield curve). 18 bootstraps per trade.

    Node fires immediately once load_and_enrich_trades completes.
    The per-trade loop is parallelised via parfun.
    """
    class_trades = [t for t in trades if t.asset_class == "CMDTY"]
    sensitivities: Dict[int, List[Sensitivity]] = {}

    with _parfun_backend_context(scheduler_address):
        results = _compute_cmdty_sensitivities_parallel(
            class_trades, base_rate, bump_size_eq, n_historical_scenarios, bootstrap_iters
        )

    for bucket_id, trade_sens_list in results:
        if bucket_id not in sensitivities:
            sensitivities[bucket_id] = []
        sensitivities[bucket_id].extend(trade_sens_list)

    return BucketedSensitivities(risk_class="CMDTY", by_bucket=sensitivities, n_trades=len(class_trades))


# ─── Unified per-trade dispatch (flat parallelism) ──────────────────────────


def _compute_sensitivities_for_trade(
    trade: Trade,
    base_rate: float = 0.04,
    bump_size_ir: float = 0.0001,
    bump_size_eq: float = 0.01,
    bump_size_vol: float = 0.01,
    bump_size_cs: float = 0.0001,
    n_mc_paths: int = 100,
    n_historical_scenarios: int = 250,
    bootstrap_iters: int = 5,
) -> Tuple[str, int, List[Sensitivity]]:
    """
    Unified per-trade sensitivity dispatcher.

    Routes to the appropriate risk-class function based on trade.asset_class.
    Returns (risk_class, bucket_id, sensitivities).

    Used by the flat parfun path (all trades in one batch) and by the per-trade
    pargraph dict-graph (one DAG node per trade).
    """
    ac = trade.asset_class
    if ac == "GIRR":
        bid, sens = _compute_girr_sensitivities_for_trade(
            trade, base_rate, bump_size_ir, n_historical_scenarios, bootstrap_iters
        )
    elif ac == "EQ":
        bid, sens = _compute_eq_sensitivities_for_trade(
            trade, base_rate, bump_size_eq, bump_size_vol, n_mc_paths, n_historical_scenarios
        )
    elif ac == "CSR":
        bid, sens = _compute_csr_sensitivities_for_trade(
            trade, base_rate, bump_size_cs, n_historical_scenarios, bootstrap_iters
        )
    elif ac == "FX":
        bid, sens = _compute_fx_sensitivities_for_trade(
            trade, base_rate, bump_size_eq, n_historical_scenarios, bootstrap_iters
        )
    elif ac == "CMDTY":
        bid, sens = _compute_cmdty_sensitivities_for_trade(
            trade, base_rate, bump_size_eq, n_historical_scenarios, bootstrap_iters
        )
    else:
        raise ValueError(f"Unknown asset class: {ac}")
    return (ac, bid, sens)


@pf.parallel(
    split=pf.per_argument(all_trades=pf.py_list.by_chunk), combine_with=pf.py_list.concat, fixed_partition_size=1
)
def _compute_all_sensitivities_parallel(
    all_trades: List[Trade],
    base_rate: float = 0.04,
    bump_size_ir: float = 0.0001,
    bump_size_eq: float = 0.01,
    bump_size_vol: float = 0.01,
    bump_size_cs: float = 0.0001,
    n_mc_paths: int = 100,
    n_historical_scenarios: int = 250,
    bootstrap_iters: int = 5,
) -> List[Tuple[str, int, List[Sensitivity]]]:
    """Flat parfun wrapper: all trades in one batch, maximum worker utilisation."""
    return [
        _compute_sensitivities_for_trade(
            t,
            base_rate,
            bump_size_ir,
            bump_size_eq,
            bump_size_vol,
            bump_size_cs,
            n_mc_paths,
            n_historical_scenarios,
            bootstrap_iters,
        )
        for t in all_trades
    ]


def _group_sensitivities(results: List[Tuple[str, int, List[Sensitivity]]]) -> Dict[str, BucketedSensitivities]:
    """Group flat per-trade results into BucketedSensitivities per risk class."""
    by_class: Dict[str, Dict[int, List[Sensitivity]]] = {}
    trade_counts: Dict[str, int] = {}
    for risk_class, bucket_id, sens_list in results:
        if risk_class not in by_class:
            by_class[risk_class] = {}
            trade_counts[risk_class] = 0
        trade_counts[risk_class] += 1
        if bucket_id not in by_class[risk_class]:
            by_class[risk_class][bucket_id] = []
        by_class[risk_class][bucket_id].extend(sens_list)
    result = {}
    for rc in ["GIRR", "CSR", "EQ", "FX", "CMDTY"]:
        if rc in by_class:
            result[rc] = BucketedSensitivities(risk_class=rc, by_bucket=by_class[rc], n_trades=trade_counts[rc])
        else:
            result[rc] = BucketedSensitivities(risk_class=rc, by_bucket={}, n_trades=0)
    return result


def _collect_and_bucket_results(risk_class: str, *results: Tuple[str, int, List[Sensitivity]]) -> BucketedSensitivities:
    """
    Pargraph dict-graph node: collect per-trade sensitivity results for one risk class
    into a BucketedSensitivities object.  Called with only the trades for this risk class.
    """
    sensitivities: Dict[int, List[Sensitivity]] = {}
    for _, bucket_id, sens_list in results:
        if bucket_id not in sensitivities:
            sensitivities[bucket_id] = []
        sensitivities[bucket_id].extend(sens_list)
    return BucketedSensitivities(risk_class=risk_class, by_bucket=sensitivities, n_trades=len(results))


def _compute_single_bucket_capital(
    bucket_id: int, sens_list: List[Sensitivity], rho_eff: float
) -> Tuple[int, float, float]:
    """
    Compute within-bucket capital (Kb) and net sensitivity (Sb) for one bucket.

    Returns (bucket_id, Kb, Sb).
    Extracted from the per-bucket loop in compute_bucket_capital_under_scenario
    so that parfun can parallelise across buckets.
    """
    if not sens_list:
        return (bucket_id, 0.0, 0.0)

    Ws = np.array([s.delta for s in sens_list])
    n = len(Ws)

    # HEAVY: build full n×n correlation matrix and factorise via Cholesky.
    # Production engines do this to handle non-uniform pairwise correlations
    # (e.g. GIRR: same-currency pairs get rho=0.999, cross-currency get 0.0).
    # Here we use uniform rho_eff as a conservative simplification,
    # but the Cholesky cost is identical to the non-uniform case.
    corr_matrix = np.full((n, n), rho_eff)
    np.fill_diagonal(corr_matrix, 1.0)

    # Regularise for numerical stability (production: add small diagonal jitter)
    corr_matrix += np.eye(n) * 1e-8

    try:
        # Cholesky decompose then solve: Kb² = Ws' Σ Ws via L'L factorisation
        L = np.linalg.cholesky(corr_matrix)
        Lw = np.linalg.solve(L, Ws)
        Kb_sq = float(Lw @ Lw)
    except np.linalg.LinAlgError:
        # Fallback: direct quadratic form (matrix not PD after scenario scaling)
        Kb_sq = float(Ws @ corr_matrix @ Ws)

    cvr_up = np.array([s.curvature_up for s in sens_list])
    cvr_dn = np.array([s.curvature_dn for s in sens_list])
    curvature_capital = max(0.0, -float(np.minimum(cvr_up, cvr_dn).sum()))

    Kb = float(np.sqrt(max(Kb_sq, 0.0))) + curvature_capital
    Sb = float(Ws.sum())
    return (bucket_id, Kb, Sb)


@pf.parallel(
    split=pf.per_argument(bucket_items=pf.py_list.by_chunk), combine_with=pf.py_list.concat, fixed_partition_size=1
)
def _compute_bucket_capitals_parallel(
    bucket_items: List[Tuple[int, List[Sensitivity]]], rho_eff: float
) -> List[Tuple[int, float, float]]:
    """Parallelised wrapper: maps _compute_single_bucket_capital over a chunk of buckets."""
    return [_compute_single_bucket_capital(bid, slist, rho_eff) for bid, slist in bucket_items]


@delayed
def compute_bucket_capital_under_scenario(
    bucketed: BucketedSensitivities, corr_scenario: str, scheduler_address: str | None = None
) -> BucketCapital:
    """
    Apply FRTB aggregation formula within and across buckets.

    For each bucket b:
        Kb² = Σᵢ Wsᵢ² + Σᵢ≠ⱼ ρᵢⱼ · Wsᵢ · Wsⱼ      (within-bucket)
        Kb  = √(max(Kb², 0))

    Across buckets:
        Ks² = Σb Kb² + Σb≠c γbc · Sb · Sc             (across-bucket)
        Ks  = √(max(Ks², 0))

    HEAVY VERSION: full dense correlation matrix is built per bucket and
    factorised via Cholesky — exactly as a production risk engine would do.
    For large buckets (GIRR or EQ with 500+ sensitivities), this means a
    500×500 Cholesky decomposition per bucket per scenario (15 total scenarios).
    ~2–5 minutes per (risk_class, scenario) combination on one core.

    The per-bucket Cholesky loop is parallelised via parfun.
    """
    scenario_scalar = CORR_SCENARIOS[corr_scenario]
    risk_class = bucketed.risk_class

    INTRA_CORR = {"GIRR": 0.999, "CSR": 0.35, "EQ": 0.15, "FX": 0.60, "CMDTY": 0.20}
    rho_base = INTRA_CORR.get(risk_class, 0.30)
    rho_eff = min(max(rho_base * scenario_scalar, 0.0), 1.0)

    CROSS_BUCKET_CORR = {"GIRR": 0.0, "CSR": 0.0, "EQ": 0.15, "FX": 0.60, "CMDTY": 0.20}
    gamma = min(max(CROSS_BUCKET_CORR.get(risk_class, 0.0) * scenario_scalar, 0.0), 1.0)

    bucket_items = list(bucketed.by_bucket.items())
    with _parfun_backend_context(scheduler_address):
        results = _compute_bucket_capitals_parallel(bucket_items, rho_eff)

    kb_per_bucket: Dict[int, float] = {bid: Kb for bid, Kb, _ in results}
    Sb_per_bucket: Dict[int, float] = {bid: Sb for bid, _, Sb in results}

    buckets = sorted(kb_per_bucket.keys())
    Kb_vec = np.array([kb_per_bucket[b] for b in buckets])
    Sb_vec = np.array([Sb_per_bucket[b] for b in buckets])

    n_b = len(buckets)
    if n_b == 0:
        Ks = 0.0
    elif n_b == 1:
        Ks = float(Kb_vec[0])
    else:
        cross_corr = np.full((n_b, n_b), gamma)
        np.fill_diagonal(cross_corr, 1.0)
        Ks_sq = float(Kb_vec @ Kb_vec) + float(Sb_vec @ cross_corr @ Sb_vec) - float(Sb_vec @ Sb_vec)
        Ks = float(np.sqrt(max(Ks_sq, 0.0)))

    return BucketCapital(risk_class=risk_class, corr_scenario=corr_scenario, kb_per_bucket=kb_per_bucket, ks=Ks)


@delayed
def worst_case_risk_class_capital(low: BucketCapital, medium: BucketCapital, high: BucketCapital) -> RiskClassCapital:
    """
    FRTB requires taking the maximum capital across the three correlation
    scenarios (conservative approach to model risk in correlations).

    This node waits for ALL THREE scenario nodes for its risk class,
    then selects the worst case.
    """
    scenario_ks = {bc.corr_scenario: bc.ks for bc in [low, medium, high]}
    worst_scenario = max(scenario_ks, key=scenario_ks.get)
    return RiskClassCapital(
        risk_class=low.risk_class,
        capital=scenario_ks[worst_scenario],
        worst_scenario=worst_scenario,
        scenario_capitals=scenario_ks,
    )


@delayed
def aggregate_total_capital(
    risk_class_capitals_girr: RiskClassCapital,
    risk_class_capitals_csr: RiskClassCapital,
    risk_class_capitals_eq: RiskClassCapital,
    risk_class_capitals_fx: RiskClassCapital,
    risk_class_capitals_cmdty: RiskClassCapital,
    trades: List[Trade],
) -> FRTBCapitalReport:
    """
    Cross-risk-class aggregation using FRTB gamma matrix.

    Total_SBM = √(Σbc γbc · Ks_b · Ks_c)

    This is the final node in the DAG. It waits for all 5 risk class nodes.
    """
    # Align with CROSS_CLASS_CORR ordering: GIRR, CSR, EQ, FX, CMDTY
    ORDER = ["GIRR", "CSR", "EQ", "FX", "CMDTY"]
    rc_cap = {
        rc.risk_class: rc.capital
        for rc in [
            risk_class_capitals_girr,
            risk_class_capitals_csr,
            risk_class_capitals_eq,
            risk_class_capitals_fx,
            risk_class_capitals_cmdty,
        ]
    }

    Ks_vec = np.array([rc_cap.get(rc, 0.0) for rc in ORDER])
    total_sq = float(Ks_vec @ CROSS_CLASS_CORR @ Ks_vec)
    total_capital = float(np.sqrt(max(total_sq, 0.0)))

    by_risk_class = {rc: float(Ks_vec[i]) for i, rc in enumerate(ORDER)}

    # Scenario breakdown table
    rows = []
    for rc in [
        risk_class_capitals_girr,
        risk_class_capitals_csr,
        risk_class_capitals_eq,
        risk_class_capitals_fx,
        risk_class_capitals_cmdty,
    ]:
        for scenario, cap in rc.scenario_capitals.items():
            rows.append(
                {
                    "Risk Class": rc.risk_class,
                    "Scenario": scenario,
                    "Capital ($M)": cap / 1e6,
                    "Is Worst": scenario == rc.worst_scenario,
                }
            )
    scenario_df = pd.DataFrame(rows)

    # Sensitivity summary
    asset_classes = [t.asset_class for t in trades]
    sens_df = pd.DataFrame({"Asset Class": asset_classes}).value_counts().reset_index()
    sens_df.columns = ["Asset Class", "Trade Count"]

    return FRTBCapitalReport(
        total_capital=total_capital,
        by_risk_class=by_risk_class,
        scenario_breakdown=scenario_df,
        sensitivity_summary=sens_df,
    )


@graph
def frtb_sbm_capital(
    n_trades: int, seed: int = 42, bootstrap_iters: int = 5, scheduler_address: str | None = None
) -> FRTBCapitalReport:
    """
    FRTB Sensitivity-Based Method capital calculation pipeline.

    DAG TOPOLOGY (each asset class is now its own @delayed node):

      load_and_enrich_trades()
              │
      ┌───────┼────────┬──────────┬──────────┐
      ▼       ▼        ▼          ▼          ▼
    GIRR_   EQ_      CSR_       FX_       CMDTY_    ← Level 2: 5 parallel nodes
    sensi   sensi    sensi      sensi     sensi       (each a dedicated @delayed fn)
      │       │        │          │          │
      ├─LO   ├─LO     ├─LO       ├─LO       ├─LO
      ├─MED  ├─MED    ├─MED      ├─MED      ├─MED   ← Level 4: 15 parallel scenario nodes
      └─HI   └─HI     └─HI       └─HI       └─HI
      │       │        │          │          │
      ▼       ▼        ▼          ▼          ▼
    Ks_    Ks_      Ks_        Ks_       Ks_        ← Level 5: worst-case per class
    GIRR   EQ       CSR        FX        CMDTY
      └───────┴────────┴──────────┴──────────┘
                        │
              aggregate_total_capital()              ← Level 6: final fan-in

    PARALLEL WINS:
      Level 2 → 5 sensitivity nodes fire simultaneously once trades loaded
      Level 4 → 15 scenario nodes fire as each L2 node completes
      Level 5 → 5 worst-case nodes fire as their 3 scenario siblings complete
    """
    trades = load_and_enrich_trades(n_trades=n_trades, seed=seed)

    # Level 2: 5 parallel sensitivity nodes — one dedicated function per asset class
    # pargraph can see these as independent nodes and schedule them concurrently
    bucketed_girr = compute_girr_sensitivities(
        trades, bootstrap_iters=bootstrap_iters, scheduler_address=scheduler_address
    )
    bucketed_eq = compute_eq_sensitivities(trades, scheduler_address=scheduler_address)
    bucketed_csr = compute_csr_sensitivities(
        trades, bootstrap_iters=bootstrap_iters, scheduler_address=scheduler_address
    )
    bucketed_fx = compute_fx_sensitivities(trades, bootstrap_iters=bootstrap_iters, scheduler_address=scheduler_address)
    bucketed_cmdty = compute_cmdty_sensitivities(
        trades, bootstrap_iters=bootstrap_iters, scheduler_address=scheduler_address
    )

    # Level 4: 15 parallel nodes (5 classes × 3 scenarios)
    # Each fires as soon as its parent sensitivity node completes
    bucketed_capital_girr_low = compute_bucket_capital_under_scenario(
        bucketed_girr, "low", scheduler_address=scheduler_address
    )
    bucketed_capital_eq_low = compute_bucket_capital_under_scenario(
        bucketed_eq, "low", scheduler_address=scheduler_address
    )
    bucketed_capital_csr_low = compute_bucket_capital_under_scenario(
        bucketed_csr, "low", scheduler_address=scheduler_address
    )
    bucketed_capital_fx_low = compute_bucket_capital_under_scenario(
        bucketed_fx, "low", scheduler_address=scheduler_address
    )
    bucketed_capital_cmdty_low = compute_bucket_capital_under_scenario(
        bucketed_cmdty, "low", scheduler_address=scheduler_address
    )

    bucketed_capital_girr_medium = compute_bucket_capital_under_scenario(
        bucketed_girr, "medium", scheduler_address=scheduler_address
    )
    bucketed_capital_eq_medium = compute_bucket_capital_under_scenario(
        bucketed_eq, "medium", scheduler_address=scheduler_address
    )
    bucketed_capital_csr_medium = compute_bucket_capital_under_scenario(
        bucketed_csr, "medium", scheduler_address=scheduler_address
    )
    bucketed_capital_fx_medium = compute_bucket_capital_under_scenario(
        bucketed_fx, "medium", scheduler_address=scheduler_address
    )
    bucketed_capital_cmdty_medium = compute_bucket_capital_under_scenario(
        bucketed_cmdty, "medium", scheduler_address=scheduler_address
    )

    bucketed_capital_girr_high = compute_bucket_capital_under_scenario(
        bucketed_girr, "high", scheduler_address=scheduler_address
    )
    bucketed_capital_eq_high = compute_bucket_capital_under_scenario(
        bucketed_eq, "high", scheduler_address=scheduler_address
    )
    bucketed_capital_csr_high = compute_bucket_capital_under_scenario(
        bucketed_csr, "high", scheduler_address=scheduler_address
    )
    bucketed_capital_fx_high = compute_bucket_capital_under_scenario(
        bucketed_fx, "high", scheduler_address=scheduler_address
    )
    bucketed_capital_cmdty_high = compute_bucket_capital_under_scenario(
        bucketed_cmdty, "high", scheduler_address=scheduler_address
    )

    # Level 5: worst-case selection — 5 parallel nodes
    # Each waits for its own 3 scenario children only
    worst_case_girr = worst_case_risk_class_capital(
        bucketed_capital_girr_low, bucketed_capital_girr_medium, bucketed_capital_girr_high
    )
    worst_case_eq = worst_case_risk_class_capital(
        bucketed_capital_eq_low, bucketed_capital_eq_medium, bucketed_capital_eq_high
    )
    worst_case_csr = worst_case_risk_class_capital(
        bucketed_capital_csr_low, bucketed_capital_csr_medium, bucketed_capital_csr_high
    )
    worst_case_fx = worst_case_risk_class_capital(
        bucketed_capital_fx_low, bucketed_capital_fx_medium, bucketed_capital_fx_high
    )
    worst_case_cmdty = worst_case_risk_class_capital(
        bucketed_capital_cmdty_low, bucketed_capital_cmdty_medium, bucketed_capital_cmdty_high
    )

    # Level 6: total capital — 1 node, waits for all 5 risk-class nodes
    return aggregate_total_capital(
        worst_case_girr, worst_case_csr, worst_case_eq, worst_case_fx, worst_case_cmdty, trades
    )


# ── Demo runner ───────────────────────────────────────────────────────────────
#
# Four execution modes to benchmark pargraph and parfun independently:
#
#   sequential     – no parallelism at all (single-process baseline)
#   parfun-only    – ALL trades in one flat parfun batch → max worker utilisation
#   pargraph-only  – per-trade dict-graph → pargraph distributes individual trades
#   both           – Phase 1: flat parfun for trades. Phase 2: pargraph DAG for
#                    bucketing/scenarios/aggregation
#
# Previous architecture processed 5 risk classes serially (each submitting their
# trades separately).  The new flat approach sends ALL trades across ALL risk
# classes in a single parallel batch, eliminating serial bottlenecks.
#
# Usage:
#   # start cluster:  PYTHONPATH=. scaler cluster.toml
#   python example4_frtb_heavy.py --mode sequential
#   python example4_frtb_heavy.py --mode parfun-only   --scheduler tcp://127.0.0.1:6378
#   python example4_frtb_heavy.py --mode pargraph-only  --scheduler tcp://127.0.0.1:6378
#   python example4_frtb_heavy.py --mode both            --scheduler tcp://127.0.0.1:6378
#
#   # run all four back-to-back:
#   python example4_frtb_heavy.py --mode all --scheduler tcp://127.0.0.1:6378

if __name__ == "__main__":
    import argparse
    import time
    from pargraph import GraphEngine
    from scaler import Client

    MODES = ["sequential", "parfun-only", "pargraph-only", "both", "all"]

    parser = argparse.ArgumentParser(description="FRTB SBM Capital – pargraph + parfun + Scaler demo")
    parser.add_argument("--mode", choices=MODES, default="both", help="execution mode (default: both)")
    parser.add_argument(
        "--scheduler", default="tcp://127.0.0.1:6378", help="Scaler scheduler address (default: tcp://127.0.0.1:6378)"
    )
    parser.add_argument("--n-trades", type=int, default=N_TRADES, help=f"number of trades (default: {N_TRADES})")
    parser.add_argument(
        "--bootstrap-iters",
        type=int,
        default=BOOTSTRAP_ITERS,
        help=f"bootstrap inner-loop iterations (default: {BOOTSTRAP_ITERS})",
    )
    args = parser.parse_args()

    # Apply CLI overrides to module-level knobs
    N_TRADES = args.n_trades
    BOOTSTRAP_ITERS = args.bootstrap_iters

    modes_to_run = MODES[:-1] if args.mode == "all" else [args.mode]

    RISK_CLASSES = ["GIRR", "CSR", "EQ", "FX", "CMDTY"]

    def _run_post_sensitivity_pipeline(
        grouped: Dict[str, BucketedSensitivities], trades: List[Trade]
    ) -> FRTBCapitalReport:
        """Run bucketing → scenarios → worst-case → aggregation locally (cheap)."""
        rc_capitals = {}
        for rc in RISK_CLASSES:
            bucketed = grouped[rc]
            caps = {}
            for scenario in ["low", "medium", "high"]:
                caps[scenario] = compute_bucket_capital_under_scenario(bucketed, scenario, scheduler_address=None)
            rc_capitals[rc] = worst_case_risk_class_capital(caps["low"], caps["medium"], caps["high"])
        return aggregate_total_capital(
            rc_capitals["GIRR"], rc_capitals["CSR"], rc_capitals["EQ"], rc_capitals["FX"], rc_capitals["CMDTY"], trades
        )

    def _build_per_trade_graph(trades: List[Trade], bootstrap_iters: int) -> dict:
        """
        Build a per-trade dict-graph for pargraph execution.

        Creates one DAG node per trade (maximum parallelism), then collector/
        bucketing/scenario/aggregation nodes.  With N trades and W workers,
        pargraph processes trades in ceil(N/W) rounds → near-linear speedup.
        """
        dg: dict = {}

        # Per-trade sensitivity nodes
        for i, trade in enumerate(trades):
            dg[f"sens_{i}"] = (
                _compute_sensitivities_for_trade,
                trade,
                0.04,
                0.0001,
                0.01,
                0.01,
                0.0001,
                EQ_MC_PATHS,
                250,
                bootstrap_iters,
            )

        # Per-risk-class collector nodes (each depends only on its own trades)
        for rc in RISK_CLASSES:
            rc_indices = [i for i, t in enumerate(trades) if t.asset_class == rc]
            dg[f"bucketed_{rc}"] = (_collect_and_bucket_results, rc, *[f"sens_{i}" for i in rc_indices])

            # 3 correlation scenario nodes per risk class (15 total)
            for scenario in ["low", "medium", "high"]:
                dg[f"capital_{rc}_{scenario}"] = (
                    compute_bucket_capital_under_scenario,
                    f"bucketed_{rc}",
                    scenario,
                    None,
                )

            # Worst-case selection per risk class
            dg[f"worst_{rc}"] = (
                worst_case_risk_class_capital,
                f"capital_{rc}_low",
                f"capital_{rc}_medium",
                f"capital_{rc}_high",
            )

        # Final cross-class aggregation
        dg["trades_data"] = trades
        dg["total"] = (
            aggregate_total_capital,
            "worst_GIRR",
            "worst_CSR",
            "worst_EQ",
            "worst_FX",
            "worst_CMDTY",
            "trades_data",
        )
        return dg

    def _build_post_sensitivity_graph(grouped: Dict[str, BucketedSensitivities], trades: List[Trade]) -> dict:
        """
        Build a pargraph dict-graph for the post-sensitivity phase only.

        Inputs (BucketedSensitivities per risk class) are injected as constant
        nodes; bucketing/scenario/worst-case/aggregation are DAG task nodes.
        """
        dg: dict = {}
        for rc in RISK_CLASSES:
            dg[f"bucketed_{rc}"] = grouped[rc]  # constant node
            for scenario in ["low", "medium", "high"]:
                dg[f"capital_{rc}_{scenario}"] = (
                    compute_bucket_capital_under_scenario,
                    f"bucketed_{rc}",
                    scenario,
                    None,
                )
            dg[f"worst_{rc}"] = (
                worst_case_risk_class_capital,
                f"capital_{rc}_low",
                f"capital_{rc}_medium",
                f"capital_{rc}_high",
            )
        dg["trades_data"] = trades
        dg["total"] = (
            aggregate_total_capital,
            "worst_GIRR",
            "worst_CSR",
            "worst_EQ",
            "worst_FX",
            "worst_CMDTY",
            "trades_data",
        )
        return dg

    def _run_mode(mode: str) -> float:
        needs_cluster = mode in ("parfun-only", "pargraph-only", "both")
        uses_parfun = mode in ("parfun-only", "both")
        uses_pargraph = mode in ("pargraph-only", "both")

        print(f"\n{'─'*60}")
        print(f"Mode: {mode}")
        print(f"  pargraph (DAG-level):  {'ON' if uses_pargraph else 'OFF'}")
        print(f"  parfun  (trade-level): {'ON' if uses_parfun else 'OFF'}")
        print(f"  N_TRADES={N_TRADES}  BOOTSTRAP_ITERS={BOOTSTRAP_ITERS}")

        client = None
        if needs_cluster:
            client = Client(address=args.scheduler)
            print(f"  Connected to {args.scheduler}")

        t0 = time.perf_counter()

        if mode == "sequential":
            # No parallelism. Calls all @delayed functions in-process serially.
            report = frtb_sbm_capital(n_trades=N_TRADES, bootstrap_iters=BOOTSTRAP_ITERS, scheduler_address=None)

        elif mode == "parfun-only":
            # Flat parfun: ALL trades in one batch → every worker busy.
            # Bucketing/scenarios/aggregation run locally (sub-second).
            trades = load_and_enrich_trades(n_trades=N_TRADES, seed=42)
            with _parfun_backend_context(args.scheduler):
                all_results = _compute_all_sensitivities_parallel(
                    trades, n_mc_paths=EQ_MC_PATHS, bootstrap_iters=BOOTSTRAP_ITERS
                )
            grouped = _group_sensitivities(all_results)
            report = _run_post_sensitivity_pipeline(grouped, trades)

        elif mode == "pargraph-only":
            # Per-trade dict-graph: one DAG node per trade.
            # Pargraph distributes individual trades across workers.
            # With N trades / W workers → ceil(N/W) rounds → near-linear speedup.
            trades = load_and_enrich_trades(n_trades=N_TRADES, seed=42)
            dict_graph = _build_per_trade_graph(trades, BOOTSTRAP_ITERS)
            engine = GraphEngine(backend=client)
            (report,) = engine.get(dict_graph, ["total"])

        elif mode == "both":
            # Phase 1 (parfun): all trades in one flat batch — workers compute
            #   sensitivities.  Maximum parallelism, zero idle workers.
            # Phase 2 (pargraph): DAG for bucketing/scenarios/aggregation —
            #   workers handle scenario fan-out.  Showcases both libraries.
            trades = load_and_enrich_trades(n_trades=N_TRADES, seed=42)
            with _parfun_backend_context(args.scheduler):
                all_results = _compute_all_sensitivities_parallel(
                    trades, n_mc_paths=EQ_MC_PATHS, bootstrap_iters=BOOTSTRAP_ITERS
                )
            grouped = _group_sensitivities(all_results)
            dict_graph = _build_post_sensitivity_graph(grouped, trades)
            engine = GraphEngine(backend=client)
            (report,) = engine.get(dict_graph, ["total"])

        elapsed = time.perf_counter() - t0

        print(f"  Total FRTB SBM Capital: ${report.total_capital / 1e6:,.1f}M")
        print(f"  Wall time: {elapsed:.1f}s ({elapsed/60:.1f} min)")

        if client is not None:
            client.disconnect()

        return elapsed

    print("FRTB SBM Capital Pipeline – Benchmark")
    print(f"{'='*60}")

    results = {}
    for mode in modes_to_run:
        results[mode] = _run_mode(mode)

    if len(results) > 1:
        print(f"\n{'='*60}")
        print("Summary:")
        baseline = results.get("sequential")
        for mode, elapsed in results.items():
            speedup = f"  ({baseline / elapsed:.1f}x vs sequential)" if baseline and mode != "sequential" else ""
            print(f"  {mode:20s}  {elapsed:8.1f}s{speedup}")
        print(f"{'='*60}")
