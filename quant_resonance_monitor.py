#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股三维量化自动监控与资金强弱评分系统 (AKShare 1.20.44+ / 旧版双兼容)
v11: 在 v10 基础上为期权模块增加「东财限流自动托底策略」(主路正常时不触发)：
1. 主路不变：优先走东财 (option_realtime_em + option_value_analysis_em / option_risk_analysis_em)
   并沿用 v10 的跨进程 push2 熔断器 (60->120分钟指数退避) 与 25 分钟磁盘缓存；
2. 托底一层（新浪 50ETF 精准批量托底）：仅当东财 PCR 或 IV 失败/处于熔断期且无当日缓存时自动启动，
   通过 option_sse_list_sina + option_sse_codes_sina 拿到 50ETF 合约号，用 2~4 次新浪批量行情请求
   (CON_OP_ / CON_SO_) 直接补齐 Vol_PCR、OI_PCR 与近/次月平值 IV，完全绕过东财 262 页翻页；
3. 托底二层（上交所官网统计）：若 PCR 仍缺失，尝试调用 option_daily_stats_sse 补齐官方认购/认沽统计；
4. 托底三层（QVIX 60日分位降级打分）：当 PCR 与 IV 全部缺失但 QVIX 正常时，自动用 QVIX 近 60 日分位
   给出降级期权情绪分（区间收敛在 20%~80% 并标注「期权仅QVIX降级」），避免全天 N/A。
====================================================================
用法示例：
    python quant_resonance_monitor_v11.py --once
    python quant_resonance_monitor_v11.py --once --reset-push2
    python quant_resonance_monitor_v11.py --watch --interval 1200
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import akshare as ak
except ImportError as exc:
    ak = None
    _AK_IMPORT_ERR = exc
else:
    _AK_IMPORT_ERR = None

DATA_DIR = Path(__file__).resolve().parent / "monitor_data"

CN_TZ = timezone(timedelta(hours=8))
# 沪深主板 ST/*ST 涨跌幅限制自 2026-07-06 起由 5% 调整为 10%
ST_MAIN_10PCT_FROM = date(2026, 7, 6)


def now_cn() -> datetime:
    return datetime.now(CN_TZ).replace(tzinfo=None)


def today_cn() -> date:
    return now_cn().date()


DEFAULT_INDEX_FUTURES_CONFIG = {
    "IF": {"name": "沪深300", "spot_sina": "sh000300", "spot_em": "000300", "annual_div_yield": 0.026},
    "IH": {"name": "上证50",  "spot_sina": "sh000016", "spot_em": "000016", "annual_div_yield": 0.032},
    "IC": {"name": "中证500", "spot_sina": "sh000905", "spot_em": "000905", "annual_div_yield": 0.016},
    "IM": {"name": "中证1000","spot_sina": "sh000852", "spot_em": "000852", "annual_div_yield": 0.012},
}

MONTHLY_DIV_WEIGHT = {
    1: 0.02, 2: 0.02, 3: 0.03, 4: 0.05,
    5: 0.15, 6: 0.30, 7: 0.22, 8: 0.08,
    9: 0.05, 10: 0.03, 11: 0.03, 12: 0.02,
}


def load_index_futures_config() -> Dict[str, Dict[str, object]]:
    cfg = {k: dict(v) for k, v in DEFAULT_INDEX_FUTURES_CONFIG.items()}
    override_file = DATA_DIR / "div_yield_override.json"
    if override_file.exists():
        try:
            overrides = json.loads(override_file.read_text(encoding="utf-8"))
            if isinstance(overrides, dict):
                for prod, val in overrides.items():
                    if prod in cfg and isinstance(val, (int, float)) and 0.0 <= float(val) <= 0.20:
                        cfg[prod]["annual_div_yield"] = float(val)
        except Exception as exc:
            print(f"[提示] 读取分红率覆盖文件失败，沿用默认值: {exc}")
    return cfg


# ==============================================================================
# 跨进程东财 push2 熔断器
# ==============================================================================
_PUSH2_LOCK = threading.Lock()
_PUSH2_CB_FILE = DATA_DIR / "push2_circuit_breaker.json"


def _read_push2_cb() -> Dict[str, object]:
    try:
        if not _PUSH2_CB_FILE.exists():
            return {}
        d = json.loads(_PUSH2_CB_FILE.read_text(encoding="utf-8"))
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _write_push2_cb(payload: Dict[str, object]) -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = _PUSH2_CB_FILE.with_suffix(
            _PUSH2_CB_FILE.suffix + f".{os.getpid()}.{threading.get_ident()}.tmp"
        )
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, _PUSH2_CB_FILE)
    except Exception as exc:
        print(f"[提示] push2 熔断状态写入失败: {exc}")


def reset_push2_circuit_breaker() -> None:
    try:
        if _PUSH2_CB_FILE.exists():
            _PUSH2_CB_FILE.unlink()
    except Exception as exc:
        print(f"[提示] 清除 push2 熔断状态失败: {exc}")


def push2_cooldown_info() -> Tuple[bool, int, int]:
    d = _read_push2_cb()
    blocked_until = float(d.get("blocked_until", 0.0) or 0.0)
    streak = int(d.get("streak", 0) or 0)
    rem = int(max(0.0, blocked_until - time.time()))
    return rem > 0, rem, streak


def call_push2_guarded(func: Callable, base_cooldown_sec: float = 3600.0, **kwargs):
    """
    东财 push2 翻页接口专用调用：
    - 熔断期内直接抛错跳过，不发任何网络请求；
    - 禁用快速重试 (retries=1)，失败后指数退避 (60min -> 120min 上限) 写入磁盘熔断文件；
    - 成功则自动清除熔断状态。
    - 全程持有 _PUSH2_LOCK：检查、拉取、写入都在同一把锁内，TRIN 与期权并行时，
      第二个调用方会等第一个完成后再检查熔断状态，不会在第一个刚失败后又去撞一次限流。
    """
    label = getattr(func, "__name__", str(func))
    with _PUSH2_LOCK:
        blocked, rem, streak = push2_cooldown_info()
        if blocked:
            until_str = datetime.fromtimestamp(time.time() + rem, tz=CN_TZ).strftime("%H:%M")
            raise RuntimeError(f"push2 熔断冷却中(连续失败{streak}次，预计{until_str}恢复，约剩{rem // 60}分钟)，本期已跳过")

        try:
            res = retry_call(func, retries=1, delay=0.0, **kwargs)
        except Exception as exc:
            new_streak = streak + 1
            # 指数退避：首次 1x (默认60分钟)，第2次起 2x (120分钟上限)
            wait_sec = base_cooldown_sec * min(2, 2 ** (new_streak - 1))
            _write_push2_cb({
                "blocked_until": time.time() + wait_sec,
                "streak": new_streak,
                "last_error": str(exc),
                "updated_at": now_cn().strftime("%Y-%m-%d %H:%M:%S"),
            })
            raise RuntimeError(
                f"{label} 触发 push2 限流/失败(第{new_streak}次)，已暂停拉取 {int(wait_sec // 60)} 分钟: {exc}"
            ) from exc
        reset_push2_circuit_breaker()
        return res


# ==============================================================================
# 运行环境初始化与通用工具
# ==============================================================================
def install_default_http_timeout(connect: float = 8.0, read: float = 25.0) -> None:
    try:
        from akshare.utils.context import config

        config.set_timeout((connect, read))
        return
    except Exception:
        pass
    import requests

    if getattr(requests.Session.request, "_ak_timeout_patched", False):
        return
    orig = requests.Session.request

    def _request(self, method, url, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = (connect, read)
        return orig(self, method, url, **kwargs)

    _request._ak_timeout_patched = True  # type: ignore[attr-defined]
    requests.Session.request = _request  # type: ignore[assignment]


def init_runtime_env(bypass_proxy: bool = True, http_timeout: float = 25.0) -> None:
    if ak is None:
        print(f"未检测到 akshare 库 ({_AK_IMPORT_ERR})，请先执行: pip install akshare pandas numpy", file=sys.stderr)
        sys.exit(1)

    if bypass_proxy:
        for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
            os.environ.pop(var, None)
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"

    if http_timeout and http_timeout > 0:
        install_default_http_timeout(connect=min(8.0, http_timeout), read=http_timeout)

    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _resolve_ak_func(*names: str) -> Callable:
    if ak is None:
        raise RuntimeError("akshare 未安装")
    for name in names:
        fn = getattr(ak, name, None)
        if fn is not None:
            return fn
    raise AttributeError(f"当前 akshare 版本未找到以下任一接口: {', '.join(names)}")


_TIMINGS: Dict[str, float] = {}
_TIMINGS_LOCK = threading.Lock()


def _record_timing(label: str, seconds: float) -> None:
    with _TIMINGS_LOCK:
        _TIMINGS[label] = _TIMINGS.get(label, 0.0) + seconds


def retry_call(
    func: Callable,
    retries: int = 3,
    delay: float = 0.8,
    allow_empty: bool = False,
    **kwargs,
):
    last_err: Optional[Exception] = None
    label = getattr(func, "__name__", str(func))
    if "symbol" in kwargs:
        label = f"{label}({kwargs['symbol']})"
    t0 = time.monotonic()
    try:
        for i in range(retries):
            try:
                res = func(**kwargs)
                if not allow_empty and isinstance(res, pd.DataFrame) and res.empty:
                    raise ValueError(f"{label} 返回空 DataFrame")
                return res
            except Exception as exc:
                last_err = exc
                if i < retries - 1:
                    time.sleep(delay * (i + 1))
        raise RuntimeError(f"接口 {label} 连续 {retries} 次调用失败: {last_err}") from last_err
    finally:
        _record_timing(label, time.monotonic() - t0)


def run_parallel(
    tasks: Dict[str, Callable[[], object]], workers: int = 4
) -> Dict[str, Tuple[object, Optional[Exception]]]:
    def _wrap(fn: Callable[[], object]):
        try:
            return fn(), None
        except Exception as exc:
            return None, exc

    out: Dict[str, Tuple[object, Optional[Exception]]] = {}
    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(tasks)))) as pool:
        futs = {k: pool.submit(_wrap, fn) for k, fn in tasks.items()}
        for k, f in futs.items():
            out[k] = f.result()
    return out


def atomic_to_csv(df: pd.DataFrame, path: Path, **kwargs) -> None:
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    df.to_csv(tmp, index=False, **kwargs)
    os.replace(tmp, path)


def clip_pct(val: float) -> float:
    return round(float(np.clip(val, 0.0, 100.0)), 1)


_CAL_CACHE: Dict[str, object] = {"at": 0.0, "cal": None}


def _load_trade_calendar() -> Optional[frozenset]:
    now_ts = time.time()
    if _CAL_CACHE["cal"] is not None and now_ts - float(_CAL_CACHE["at"]) < 86400:
        return _CAL_CACHE["cal"]  # type: ignore[return-value]
    cache = DATA_DIR / "trade_calendar.csv"
    try:
        if cache.exists() and now_ts - cache.stat().st_mtime < 7 * 86400:
            ser = pd.read_csv(cache)["trade_date"]
        else:
            fn = _resolve_ak_func("calendar_trade_date_hist_sina", "tool_trade_date_hist_sina")
            ser = retry_call(fn, retries=2)["trade_date"]
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            atomic_to_csv(ser.to_frame(), cache)
        cal = frozenset(pd.to_datetime(ser).dt.date)
        _CAL_CACHE.update(at=now_ts, cal=cal)
        return cal
    except Exception:
        stale = None
        try:
            if cache.exists():
                stale = frozenset(pd.to_datetime(pd.read_csv(cache)["trade_date"]).dt.date)
        except Exception:
            stale = None
        _CAL_CACHE.update(at=now_ts - 86400 + 600, cal=stale if stale else frozenset())
        return stale


def is_trading_day(d: Optional[date] = None) -> bool:
    d = d or today_cn()
    if d.weekday() >= 5:
        return False
    cal = _load_trade_calendar()
    if cal and d <= max(cal):
        return d in cal
    return True


def is_trading_time(now: Optional[datetime] = None) -> bool:
    dt = now or now_cn()
    if not is_trading_day(dt.date()):
        return False
    t = dt.time()
    return (dt_time(9, 25) <= t <= dt_time(11, 32)) or (dt_time(13, 0) <= t <= dt_time(15, 2))


def is_lunch_break(now: Optional[datetime] = None) -> bool:
    dt = now or now_cn()
    return is_trading_day(dt.date()) and (dt_time(11, 32) < dt.time() < dt_time(13, 0))


def third_friday(year: int, month: int) -> date:
    first_day = date(year, month, 1)
    offset = (4 - first_day.weekday()) % 7
    return first_day + timedelta(days=offset + 14)


def get_active_cffex_contracts(ref_date: Optional[date] = None) -> List[Tuple[str, date]]:
    ref = ref_date or today_cn()
    y, m = ref.year, ref.month
    if ref > third_friday(y, m):
        m += 1
        if m > 12:
            m = 1
            y += 1

    months: List[Tuple[int, int]] = []
    for i in range(2):
        nm = m + i
        ny = y + (nm - 1) // 12
        nm = (nm - 1) % 12 + 1
        months.append((ny, nm))

    cy, cm = months[-1]
    while len(months) < 4:
        cm += 1
        if cm > 12:
            cm = 1
            cy += 1
        if cm in (3, 6, 9, 12) and (cy, cm) not in months:
            months.append((cy, cm))

    return [(f"{yy % 100:02d}{mm:02d}", third_friday(yy, mm)) for yy, mm in months]


def estimate_expected_dividend_points(
    spot_price: float, annual_div_yield: float, start_dt: date, expiry_dt: date
) -> float:
    if expiry_dt <= start_dt:
        return 0.0
    weight_sum = 0.0
    cur = start_dt + timedelta(days=1)
    while cur <= expiry_dt:
        weight_sum += MONTHLY_DIV_WEIGHT[cur.month] / 30.4375
        cur += timedelta(days=1)
    return spot_price * annual_div_yield * weight_sum


# ==============================================================================
# 模块一：股指期货 (IF/IH/IC/IM) 分红修正 + 30日插值基差
# ==============================================================================
def fetch_spot_index_history(symbol_sina: str, symbol_em: str, lookback_days: int = 120) -> pd.DataFrame:
    sina_err: Optional[Exception] = None
    try:
        fn_sina = _resolve_ak_func("index_zh_daily_sina", "stock_zh_index_daily")
        df = retry_call(fn_sina, symbol=symbol_sina)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        return df[["date", "close"]].dropna().tail(lookback_days).reset_index(drop=True)
    except Exception as exc:
        sina_err = exc

    try:
        fn_em = _resolve_ak_func("index_zh_hist_em", "index_zh_a_hist")
        df = retry_call(fn_em, symbol=symbol_em, period="daily")
        df = df.rename(columns={"日期": "date", "收盘": "close"})
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        return df[["date", "close"]].dropna().tail(lookback_days).reset_index(drop=True)
    except Exception as em_err:
        raise RuntimeError(f"新浪({sina_err})与东财({em_err})指数接口均失败") from em_err


def fetch_futures_contract_history(contract_code: str) -> pd.DataFrame:
    fn = _resolve_ak_func("futures_zh_daily_sina")
    df = retry_call(fn, symbol=contract_code)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for col in ("close", "volume", "hold"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[["date", "close", "volume", "hold"]].dropna(subset=["close"]).sort_values("date").reset_index(drop=True)


def _update_basis_history(dt: date, prod: str, rate: float) -> pd.DataFrame:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    hist_file = DATA_DIR / "basis_30d_history.csv"
    if hist_file.exists():
        df = pd.read_csv(hist_file)
    else:
        df = pd.DataFrame(columns=["date", "product", "interp_30d_rate"])

    dt_str = str(dt)
    mask = (df["date"] == dt_str) & (df["product"] == prod)
    if mask.any():
        df.loc[mask, "interp_30d_rate"] = rate
    else:
        new_row = pd.DataFrame([{"date": dt_str, "product": prod, "interp_30d_rate": rate}])
        df = new_row if df.empty else pd.concat([df, new_row], ignore_index=True)
    df = df.sort_values(["product", "date"]).reset_index(drop=True)
    atomic_to_csv(df, hist_file)
    return df


def prefetch_basis_data(
    cfg_map: Dict[str, Dict[str, object]],
    contracts_meta: List[Tuple[str, date]],
    workers: int = 8,
) -> Tuple[Dict[str, object], Dict[str, object]]:
    spot_map: Dict[str, object] = {}
    fut_map: Dict[str, object] = {}

    def _spot_all() -> None:
        for prod, cfg in cfg_map.items():
            try:
                spot_map[prod] = fetch_spot_index_history(
                    str(cfg["spot_sina"]), str(cfg["spot_em"]), lookback_days=120
                )
            except Exception as exc:
                spot_map[prod] = exc

    def _fut_one(code: str) -> None:
        try:
            fut_map[code] = fetch_futures_contract_history(code)
        except Exception as exc:
            fut_map[code] = exc

    codes = [f"{prod}{suffix}" for prod in cfg_map for suffix, _ in contracts_meta]
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futs = [pool.submit(_spot_all)] + [pool.submit(_fut_one, c) for c in codes]
        for f in futs:
            f.result()
    return spot_map, fut_map


def compute_basis_module(workers: int = 8) -> Tuple[pd.DataFrame, Dict[str, object]]:
    today = today_cn()
    cfg_map = load_index_futures_config()
    contracts_meta = get_active_cffex_contracts(today)
    spot_map, fut_map = prefetch_basis_data(cfg_map, contracts_meta, workers=workers)
    records = []
    errors: List[str] = []
    metrics: Dict[str, object] = {}
    pct_list: List[float] = []
    slope_list: List[float] = []
    vol_ratio_list: List[float] = []

    for prod, cfg in cfg_map.items():
        spot_df = spot_map.get(prod)
        if isinstance(spot_df, Exception):
            errors.append(f"{prod} 现货获取失败: {spot_df}")
            continue
        if spot_df is None or spot_df.empty:
            errors.append(f"{prod} 现货返回空表")
            continue

        latest_spot_date = spot_df["date"].iloc[-1]

        raw_futs: List[Tuple[str, date, pd.DataFrame]] = []
        for suffix, exp_dt in contracts_meta:
            code = f"{prod}{suffix}"
            fut_df = fut_map.get(code)
            if fut_df is None or isinstance(fut_df, Exception):
                errors.append(f"{code} 行情获取失败: {fut_df}")
                continue
            if not fut_df.empty:
                raw_futs.append((code, exp_dt, fut_df))

        if not raw_futs:
            errors.append(f"{prod} 无可用期货合约")
            continue

        spot_dates = set(spot_df["date"])
        aligned_date = latest_spot_date
        is_lagged = False

        has_latest = any(
            (exp_dt - latest_spot_date).days >= 5 and (latest_spot_date in set(fdf["date"]))
            for _, exp_dt, fdf in raw_futs
        )
        if not has_latest:
            common_dates = set()
            for _, _, fdf in raw_futs:
                common_dates |= (spot_dates & set(fdf["date"]))
            if common_dates:
                aligned_date = max(common_dates)
                is_lagged = aligned_date != latest_spot_date

        spot_sub = spot_df[spot_df["date"] <= aligned_date].copy()
        if spot_sub.empty:
            errors.append(f"{prod} 找不到对齐日期({aligned_date})的现货记录")
            continue

        aligned_spot_close = float(spot_sub["close"].iloc[-1])
        spot_sub["ma60"] = spot_sub["close"].rolling(60).mean()
        is_20d_low = aligned_spot_close <= float(spot_sub["close"].tail(20).min())
        is_20d_high = aligned_spot_close >= float(spot_sub["close"].tail(20).max())
        trend_up = pd.notna(spot_sub["ma60"].iloc[-1]) and aligned_spot_close > float(spot_sub["ma60"].iloc[-1])

        valid_legs = []
        for code, exp_dt, fut_df in raw_futs:
            days_left = (exp_dt - aligned_date).days
            if days_left < 5:
                continue
            matched_idx = fut_df.index[fut_df["date"] == aligned_date]
            if len(matched_idx) == 0:
                continue
            pos = int(fut_df.index.get_loc(matched_idx[-1]))

            fut_close = float(fut_df["close"].iloc[pos])
            vol = float(fut_df["volume"].iloc[pos]) if pd.notna(fut_df["volume"].iloc[pos]) else 0.0
            oi = float(fut_df["hold"].iloc[pos]) if pd.notna(fut_df["hold"].iloc[pos]) else 0.0
            oi_prev = float(fut_df["hold"].iloc[pos - 1]) if pos >= 1 and pd.notna(fut_df["hold"].iloc[pos - 1]) else oi
            vol_ma5 = float(fut_df["volume"].iloc[max(0, pos - 5):pos].mean()) if pos >= 1 else vol

            raw_basis = fut_close - aligned_spot_close
            div_pts = estimate_expected_dividend_points(
                aligned_spot_close, float(cfg["annual_div_yield"]), aligned_date, exp_dt
            )
            adj_basis = raw_basis + div_pts
            ann_basis_rate = (adj_basis / aligned_spot_close) * (365.0 / max(days_left, 10))

            valid_legs.append({
                "code": code,
                "days_left": days_left,
                "raw_basis": raw_basis,
                "ann_rate": ann_basis_rate,
                "vol_ratio": vol / vol_ma5 if vol_ma5 > 0 else 1.0,
                "delta_oi": oi - oi_prev,
            })

        if not valid_legs:
            errors.append(f"{prod} 未找到与交易日({aligned_date})对齐的有效期货合约")
            continue

        valid_legs.sort(key=lambda x: x["days_left"])
        front = valid_legs[0]

        if len(valid_legs) >= 2 and valid_legs[0]["days_left"] <= 30 <= valid_legs[1]["days_left"]:
            t1, r1 = valid_legs[0]["days_left"], valid_legs[0]["ann_rate"]
            t2, r2 = valid_legs[1]["days_left"], valid_legs[1]["ann_rate"]
            w = (t2 - 30.0) / (t2 - t1) if t2 != t1 else 1.0
            interp_30d_rate = w * r1 + (1.0 - w) * r2
            interp_note = "30日插值"
        elif len(valid_legs) >= 2 and valid_legs[0]["days_left"] > 30:
            t1, r1 = valid_legs[0]["days_left"], valid_legs[0]["ann_rate"]
            t2, r2 = valid_legs[1]["days_left"], valid_legs[1]["ann_rate"]
            slope = (r2 - r1) / (t2 - t1) if t2 != t1 else 0.0
            interp_30d_rate = r1 + float(np.clip(slope * (30.0 - t1), -0.05, 0.05))
            interp_note = "线性外推近似"
        else:
            interp_30d_rate = front["ann_rate"]
            interp_note = f"近月({front['days_left']}天)近似"

        if is_lagged:
            interp_note += f"(滞后:{aligned_date})"

        metrics[prod] = interp_30d_rate

        hist_df = _update_basis_history(aligned_date, prod, interp_30d_rate)
        prod_hist = hist_df[hist_df["product"] == prod].sort_values("date")

        if len(prod_hist) >= 3:
            ema3 = prod_hist["interp_30d_rate"].ewm(span=3, adjust=False).mean()
            ema3_slope = float(ema3.iloc[-1] - ema3.iloc[-2])
        else:
            ema3_slope = 0.0

        past_60 = prod_hist[prod_hist["date"] < str(aligned_date)]["interp_30d_rate"].tail(60)
        if len(past_60) >= 3:
            pct_60 = float((past_60 <= interp_30d_rate).mean())
            pct_str = f"{pct_60 * 100:.1f}%"
            pct_list.append(pct_60)
        else:
            pct_60 = None
            pct_str = f"冷启动({len(past_60)}天)"

        slope_list.append(ema3_slope)
        vol_ratio_list.append(front["vol_ratio"])

        if pct_60 is not None and is_20d_low and ema3_slope > 0 and pct_60 <= 0.20:
            signal = "偏多(底部背离)"
        elif pct_60 is not None and is_20d_high and ema3_slope < 0 and pct_60 >= 0.80:
            signal = "偏空(顶部背离)"
        else:
            signal = "中性"

        records.append({
            "品种": prod,
            "现货收盘": round(aligned_spot_close, 2),
            "近月合约": front["code"],
            "余日": front["days_left"],
            "基差口径": interp_note,
            "原始基差": round(front["raw_basis"], 2),
            "30日年化基差": f"{interp_30d_rate * 100:.2f}%",
            "3日EMA斜率": f"{ema3_slope * 100:+.2f}%",
            "60日分位": pct_str,
            "量能比(vs前5日)": round(front["vol_ratio"], 2),
            "持仓变动": int(front["delta_oi"]),
            "MA60": "多" if trend_up else "空",
            "信号": signal,
        })

    if "IM" in metrics and "IH" in metrics:
        metrics["IM_minus_IH"] = float(metrics["IM"]) - float(metrics["IH"])
    metrics["avg_pct_60"] = float(np.mean(pct_list)) if pct_list else None
    metrics["avg_ema3_slope"] = float(np.mean(slope_list)) if slope_list else 0.0
    metrics["avg_vol_ratio"] = float(np.mean(vol_ratio_list)) if vol_ratio_list else 1.0
    metrics["valid_count"] = len(records)
    metrics["errors"] = errors

    return pd.DataFrame(records), metrics


# ==============================================================================
# 模块二：日内微观广度与 Relative TRIN (em 走熔断保护 + 自动切 sina 兜底)
# ==============================================================================
def _classify_limit_threshold(code: str, name: str, trade_date: Optional[date] = None) -> Optional[float]:
    name_s = str(name).strip().upper()
    code_s = re.sub(r"^(sh|sz|bj)", "", str(code).strip().lower())
    if re.match(r"^N[^\x00-\x7f]", name_s):
        return None
    if re.match(r"^C[^\x00-\x7f]", name_s) and not code_s.startswith(("43", "83", "87", "92")):
        return None
    if code_s.startswith(("300", "301", "688", "689")):
        return 20.0
    if code_s.startswith(("43", "83", "87", "92")):
        return 30.0
    if "ST" in name_s and (trade_date or today_cn()) < ST_MAIN_10PCT_FROM:
        return 5.0
    return 10.0


def _count_limit_up_down(
    pct_vals: np.ndarray,
    thresholds: np.ndarray,
    last_vals: Optional[np.ndarray] = None,
    prev_vals: Optional[np.ndarray] = None,
) -> Tuple[int, int]:
    up = dn = 0
    for i, t in enumerate(thresholds):
        if not np.isfinite(t):
            continue
        priced = False
        if last_vals is not None and prev_vals is not None:
            l, p = float(last_vals[i]), float(prev_vals[i])
            tol_pp = max(0.3, 1.2 / p) if p > 0 else 0.3
            if np.isfinite(l) and np.isfinite(p) and p > 0 and abs((l / p - 1.0) * 100.0 - float(pct_vals[i])) <= tol_pp:
                priced = True
                t_dec = Decimal(str(t)) / Decimal(100)
                up_px = float((Decimal(str(p)) * (Decimal(1) + t_dec)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
                dn_px = float((Decimal(str(p)) * (Decimal(1) - t_dec)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
                if l >= up_px - 1e-9:
                    up += 1
                elif l <= dn_px + 1e-9:
                    dn += 1
        if not priced:
            tol = t - 0.2
            if pct_vals[i] >= tol:
                up += 1
            elif pct_vals[i] <= -tol:
                dn += 1
    return up, dn


def _resolve_spot_columns(df: pd.DataFrame) -> Tuple[str, str, str, str]:
    code_col = next((c for c in ("代码", "code", "symbol") if c in df.columns), None)
    name_col = next((c for c in ("名称", "name") if c in df.columns), None)
    pct_col = next((c for c in ("涨跌幅", "changepercent") if c in df.columns), None)
    amt_col = next((c for c in ("成交额", "amount") if c in df.columns), None)
    if not all([code_col, name_col, pct_col, amt_col]):
        raise KeyError(f"全市场行情缺少必要列，实际列: {list(df.columns)}")
    return code_col, name_col, pct_col, amt_col  # type: ignore[return-value]


def _fetch_all_a_spot(spot_source: str = "em", push2_cooldown: float = 3600.0) -> pd.DataFrame:
    order = ("sina", "em") if spot_source == "sina" else ("em", "sina")
    errs: List[str] = []
    for src in order:
        try:
            if src == "sina":
                fn = _resolve_ak_func("stock_zh_a_spot_sina", "stock_zh_a_spot")
                return retry_call(fn, retries=2)
            fn = _resolve_ak_func("stock_zh_a_spot_em")
            return call_push2_guarded(fn, base_cooldown_sec=push2_cooldown)
        except Exception as exc:
            errs.append(f"{src}: {exc}")
    raise RuntimeError(f"全市场行情获取失败 ({' | '.join(errs)})")


def compute_relative_trin_module(
    spot_source: str = "em",
    push2_cooldown: float = 3600.0,
) -> Dict[str, object]:
    fetch_start = now_cn()
    df = _fetch_all_a_spot(spot_source=spot_source, push2_cooldown=push2_cooldown)
    code_col, name_col, pct_col, amt_col = _resolve_spot_columns(df)

    df[pct_col] = pd.to_numeric(df[pct_col], errors="coerce")
    df[amt_col] = pd.to_numeric(df[amt_col], errors="coerce")
    valid = df.dropna(subset=[pct_col, amt_col])
    valid = valid[valid[amt_col] > 0].copy()

    if valid.empty:
        raise ValueError("全市场有效成交个股数为 0（可能为休市或接口异常）")

    up_df = valid[valid[pct_col] > 0]
    down_df = valid[valid[pct_col] < 0]
    flat_cnt = int((valid[pct_col] == 0).sum())

    total_cnt = len(valid)
    up_cnt, down_cnt = len(up_df), len(down_df)
    total_amt = float(valid[amt_col].sum())
    up_amt = float(up_df[amt_col].sum())
    down_amt = float(down_df[amt_col].sum())

    up_ratio_cnt = up_cnt / total_cnt if total_cnt > 0 else 0.0
    up_ratio_amt = up_amt / total_amt if total_amt > 0 else 0.0
    amt_breadth_spread = up_ratio_amt - up_ratio_cnt

    trade_dt = fetch_start.date()
    thresholds = np.array([
        np.nan if t is None else t
        for t in (_classify_limit_threshold(c, n, trade_dt) for c, n in zip(valid[code_col], valid[name_col]))
    ], dtype=float)
    pct_vals = valid[pct_col].to_numpy()
    last_col = next((c for c in ("最新价", "trade") if c in valid.columns), None)
    prev_col = next((c for c in ("昨收", "settlement") if c in valid.columns), None)
    last_vals = pd.to_numeric(valid[last_col], errors="coerce").to_numpy(dtype=float) if last_col else None
    prev_vals = pd.to_numeric(valid[prev_col], errors="coerce").to_numpy(dtype=float) if prev_col else None
    limit_up, limit_down = _count_limit_up_down(pct_vals, thresholds, last_vals, prev_vals)

    if up_cnt <= 0 or down_cnt <= 0 or up_amt <= 0 or down_amt <= 0:
        return {
            "有效家数": total_cnt,
            "up_cnt": up_cnt,
            "down_cnt": down_cnt,
            "up_amt": up_amt,
            "down_amt": down_amt,
            "涨/跌/平": f"{up_cnt}/{down_cnt}/{flat_cnt}",
            "涨停/跌停": f"{limit_up}/{limit_down}",
            "limit_up": limit_up,
            "limit_down": limit_down,
            "涨家占比": round(up_ratio_cnt * 100, 2),
            "涨额占比": round(up_ratio_amt * 100, 2),
            "额价差": round(amt_breadth_spread * 100, 2),
            "TRIN_额": None,
            "Relative_TRIN": None,
            "trin_note": "极端单边市(涨或跌成交额为0)，TRIN未定义",
        }

    trin_amt = (up_cnt / down_cnt) / (up_amt / down_amt)
    ln_trin = float(np.log(trin_amt))

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    today = fetch_start.date()
    today_str = str(today)
    hist_file = DATA_DIR / "trin_history.csv"
    if hist_file.exists():
        hist = pd.read_csv(hist_file)
    else:
        hist = pd.DataFrame(columns=["date", "trin_amt", "ln_trin", "amt_breadth_spread"])

    hist = hist.sort_values("date").reset_index(drop=True)
    past_hist = hist[hist["date"] < today_str]["ln_trin"].dropna().tail(20)
    if len(past_hist) >= 3:
        rel_trin: Optional[float] = round(ln_trin - float(past_hist.median()), 3)
        trin_note = f"20日去偏(样本{len(past_hist)}天)"
    else:
        rel_trin = None
        trin_note = f"冷启动(历史样本{len(past_hist)}天<3)"

    if is_trading_day(today) and fetch_start.time() >= dt_time(15, 0):
        if (hist["date"] == today_str).any():
            hist.loc[hist["date"] == today_str, ["trin_amt", "ln_trin", "amt_breadth_spread"]] = [
                trin_amt, ln_trin, amt_breadth_spread
            ]
        else:
            row_df = pd.DataFrame([{
                "date": today_str,
                "trin_amt": trin_amt,
                "ln_trin": ln_trin,
                "amt_breadth_spread": amt_breadth_spread,
            }])
            hist = row_df if hist.empty else pd.concat([hist, row_df], ignore_index=True)
        hist = hist.sort_values("date").reset_index(drop=True)
        atomic_to_csv(hist, hist_file)
    else:
        trin_note += "；非交易日，未写入历史" if not is_trading_day(today) else "；盘中快照，未写入历史（15:00 后开始抓取的才写入）"

    return {
        "有效家数": total_cnt,
        "up_cnt": up_cnt,
        "down_cnt": down_cnt,
        "up_amt": up_amt,
        "down_amt": down_amt,
        "涨/跌/平": f"{up_cnt}/{down_cnt}/{flat_cnt}",
        "涨停/跌停": f"{limit_up}/{limit_down}",
        "limit_up": limit_up,
        "limit_down": limit_down,
        "涨家占比": round(up_ratio_cnt * 100, 2),
        "涨额占比": round(up_ratio_amt * 100, 2),
        "额价差": round(amt_breadth_spread * 100, 2),
        "TRIN_额": round(trin_amt, 3),
        "Relative_TRIN": rel_trin,
        "trin_note": trin_note,
    }


# ==============================================================================
# 模块三：50ETF 期权 PCR 与 IV 期限结构 (东财主路 + 限流时自动启动三层托底)
# ==============================================================================
def _extract_atm_iv(sub_df: pd.DataFrame, iv_col: str = "隐含波动率") -> Optional[float]:
    df = sub_df.dropna(subset=[iv_col]).copy()
    if df.empty:
        return None
    if "Delta" in df.columns and df["Delta"].notna().any():
        df["Delta"] = pd.to_numeric(df["Delta"], errors="coerce")
        df["dist"] = (df["Delta"].abs() - 0.5).abs()
        return float(df.sort_values("dist").head(4)[iv_col].mean())
    return float(df[iv_col].median())


_RT_CACHE_FILE = DATA_DIR / "option_rt_cache.json"
_RT_CACHE_TTL = 1500.0


def _load_rt_cache() -> Optional[Dict[str, object]]:
    try:
        if not _RT_CACHE_FILE.exists():
            return None
        d = json.loads(_RT_CACHE_FILE.read_text(encoding="utf-8"))
        if not isinstance(d, dict):
            return None
        vol = d.get("vol_pcr")
        oi = d.get("oi_pcr")
        ts = d.get("fetched_at")
        if ts is None or (vol is None and oi is None):
            return None
        return {
            "vol_pcr": None if vol is None else float(vol),
            "oi_pcr": None if oi is None else float(oi),
            "fetched_at": float(ts),
            "rt_name": d.get("rt_name"),
        }
    except Exception:
        return None


def _save_rt_cache(vol_pcr, oi_pcr, rt_name) -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "vol_pcr": vol_pcr,
            "oi_pcr": oi_pcr,
            "rt_name": rt_name,
            "fetched_at": time.time(),
        }
        tmp = _RT_CACHE_FILE.with_suffix(_RT_CACHE_FILE.suffix + f".{os.getpid()}.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, _RT_CACHE_FILE)
    except Exception as exc:
        print(f"[提示] 期权 PCR 缓存写入失败: {exc}")


def _cache_is_today(cached: Dict[str, object]) -> bool:
    try:
        return datetime.fromtimestamp(float(cached["fetched_at"]), tz=CN_TZ).date() == today_cn()
    except Exception:
        return False


def _sina_batch_fetch_lines(symbols: List[str], chunk_size: int = 60) -> Dict[str, List[str]]:
    """向新浪 hq.sinajs.cn 批量拉取合约行情（每批 60 个，全量 50ETF 仅需 2~4 次请求）。"""
    import requests

    out: Dict[str, List[str]] = {}
    headers = {"Referer": "https://finance.sina.com.cn/"}
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        url = f"https://hq.sinajs.cn/list={','.join(chunk)}"
        resp = requests.get(url, headers=headers, timeout=(8.0, 15.0))
        resp.raise_for_status()
        resp.encoding = "gbk"
        for line in resp.text.splitlines():
            m = re.match(r'^var hq_str_([^=]+)="([^"]*)";', line.strip())
            if m and m.group(2):
                out[m.group(1)] = m.group(2).split(",")
    return out


def _fallback_50etf_from_sina(need_pcr: bool, need_iv: bool) -> Dict[str, object]:
    """
    托底一层（仅在东财失败/熔断时调用）：
    通过新浪 50ETF 合约代码列表 + hq.sinajs.cn 批量接口获取 Vol_PCR、OI_PCR 与近/次月平值 IV。
    """
    fb: Dict[str, object] = {
        "Vol_PCR": None,
        "OI_PCR": None,
        "近月平值IV": None,
        "次月平值IV": None,
        "IV_diff": None,
    }
    fn_list = _resolve_ak_func("option_sse_list_sina")
    fn_codes = _resolve_ak_func("option_sse_codes_sina")

    months_raw = retry_call(fn_list, retries=2, symbol="50ETF", exchange="null")
    if isinstance(months_raw, pd.DataFrame) and "到期月份" in months_raw.columns:
        months = [str(m).replace("-", "") for m in months_raw["到期月份"].dropna().tolist()]
    elif isinstance(months_raw, list):
        months = [str(m).replace("-", "") for m in months_raw]
    else:
        return fb

    months = sorted({m[:6] for m in months if len(m) >= 6})
    if not months:
        return fb

    # 收集 50ETF (510050) 在市全部月份的认购/认沽合约代码
    month_codes: Dict[str, Tuple[List[str], List[str]]] = {}
    all_calls: List[str] = []
    all_puts: List[str] = []
    for ym in months:
        c_list: List[str] = []
        p_list: List[str] = []
        for side, target in (("看涨期权", c_list), ("看跌期权", p_list)):
            try:
                cdf = retry_call(fn_codes, retries=2, symbol=side, trade_date=ym, underlying="510050")
                if isinstance(cdf, pd.DataFrame) and not cdf.empty:
                    col = "期权代码" if "期权代码" in cdf.columns else cdf.columns[-1]
                    codes = [re.sub(r"\D", "", str(x)) for x in cdf[col].dropna().tolist()]
                    target.extend([c for c in codes if c])
            except Exception:
                continue
        month_codes[ym] = (c_list, p_list)
        all_calls.extend(c_list)
        all_puts.extend(p_list)

    if need_pcr and all_calls and all_puts:
        op_syms = [f"CON_OP_{c}" for c in (all_calls + all_puts)]
        op_data = _sina_batch_fetch_lines(op_syms)
        c_vol = p_vol = c_oi = p_oi = 0.0
        for code in all_calls:
            parts = op_data.get(f"CON_OP_{code}")
            if parts and len(parts) > 41:
                try:
                    c_oi += float(parts[5])
                    c_vol += float(parts[41])
                except ValueError:
                    pass
        for code in all_puts:
            parts = op_data.get(f"CON_OP_{code}")
            if parts and len(parts) > 41:
                try:
                    p_oi += float(parts[5])
                    p_vol += float(parts[41])
                except ValueError:
                    pass
        if c_vol > 0:
            fb["Vol_PCR"] = round(p_vol / c_vol, 3)
        if c_oi > 0:
            fb["OI_PCR"] = round(p_oi / c_oi, 3)

    if need_iv and len(months) >= 2:
        near_m, next_m = months[0], months[1]
        near_codes = month_codes[near_m][0] + month_codes[near_m][1]
        next_codes = month_codes[next_m][0] + month_codes[next_m][1]
        so_syms = [f"CON_SO_{c}" for c in (near_codes + next_codes)]
        so_data = _sina_batch_fetch_lines(so_syms)

        def _atm_iv_for(codes: List[str]) -> Optional[float]:
            rows = []
            for code in codes:
                parts = so_data.get(f"CON_SO_{code}")
                if parts and len(parts) > 9:
                    try:
                        delta = float(parts[5])
                        iv = float(parts[9])
                        if iv > 0:
                            iv_pct = iv * 100.0 if iv < 2.0 else iv
                            rows.append((abs(abs(delta) - 0.5), iv_pct))
                    except ValueError:
                        pass
            if not rows:
                return None
            rows.sort(key=lambda x: x[0])
            return float(np.mean([r[1] for r in rows[:4]]))

        near_iv = _atm_iv_for(near_codes)
        next_iv = _atm_iv_for(next_codes)
        if near_iv is not None and next_iv is not None:
            fb["近月平值IV"] = round(near_iv, 2)
            fb["次月平值IV"] = round(next_iv, 2)
            fb["IV_diff"] = round(near_iv - next_iv, 2)

    return fb


def _fallback_50etf_pcr_from_sse() -> Tuple[Optional[float], Optional[float]]:
    """托底二层（上交所官网每日统计）：尝试补齐官方 Vol_PCR / OI_PCR。"""
    fn = _resolve_ak_func("option_daily_stats_sse")
    for delta_days in range(0, 5):
        dt_str = (today_cn() - timedelta(days=delta_days)).strftime("%Y%m%d")
        try:
            df = retry_call(fn, retries=1, date=dt_str)
        except Exception:
            continue
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue
        mask = df.astype(str).apply(lambda col: col.str.contains("510050|50ETF", na=False)).any(axis=1)
        sub = df[mask]
        if sub.empty:
            continue
        row = sub.iloc[0]
        vol_pcr = oi_pcr = None
        if "认沽/认购持仓比" in row.index and pd.notna(row["认沽/认购持仓比"]):
            raw_oi = float(pd.to_numeric(row["认沽/认购持仓比"], errors="coerce"))
            if np.isfinite(raw_oi) and raw_oi > 0:
                oi_pcr = round(raw_oi / 100.0 if raw_oi > 5.0 else raw_oi, 3)
        c_col = next((c for c in row.index if "认购成交" in str(c)), None)
        p_col = next((c for c in row.index if "认沽成交" in str(c)), None)
        if c_col and p_col:
            c_v = float(pd.to_numeric(row[c_col], errors="coerce"))
            p_v = float(pd.to_numeric(row[p_col], errors="coerce"))
            if np.isfinite(c_v) and c_v > 0 and np.isfinite(p_v):
                vol_pcr = round(p_v / c_v, 3)
        if vol_pcr is not None or oi_pcr is not None:
            return vol_pcr, oi_pcr
    return None, None


def _fetch_rt_and_fill(
    result: Dict[str, object],
    warn_list: List[str],
    fetched: Dict[str, Tuple[object, Optional[Exception]]],
    rt_cached: Optional[Dict[str, object]],
) -> None:
    try:
        rt_res, rt_exc = fetched["rt"]
        if rt_exc is not None:
            raise rt_exc
        rt_name, rt_df = rt_res
        name_col = next((c for c in ("名称", "期权名称", "期权合约简称") if c in rt_df.columns), None)
        vol_col = next((c for c in ("成交量", "volume") if c in rt_df.columns), None)
        oi_col = next((c for c in ("持仓量", "open_interest") if c in rt_df.columns), None)

        if name_col and vol_col:
            sub = rt_df[rt_df[name_col].astype(str).str.startswith("50ETF")].copy()
            if sub.empty:
                warn_list.append(f"{rt_name} 返回 {len(rt_df)} 行，未匹配到 50ETF 合约")
            else:
                is_put = sub[name_col].astype(str).str.contains("沽", na=False)
                is_call = sub[name_col].astype(str).str.contains("购", na=False)
                sub[vol_col] = pd.to_numeric(sub[vol_col], errors="coerce").fillna(0)
                c_vol = float(sub.loc[is_call, vol_col].sum())
                p_vol = float(sub.loc[is_put, vol_col].sum())
                if c_vol > 0:
                    result["Vol_PCR"] = round(p_vol / c_vol, 3)

                if oi_col:
                    sub[oi_col] = pd.to_numeric(sub[oi_col], errors="coerce").fillna(0)
                    c_oi = float(sub.loc[is_call, oi_col].sum())
                    p_oi = float(sub.loc[is_put, oi_col].sum())
                    if c_oi > 0:
                        result["OI_PCR"] = round(p_oi / c_oi, 3)
                else:
                    warn_list.append(f"{rt_name} 缺少持仓量列")
        else:
            warn_list.append(f"{rt_name} 缺少名称或成交量列")

        if result["Vol_PCR"] is not None or result["OI_PCR"] is not None:
            _save_rt_cache(result["Vol_PCR"], result["OI_PCR"], rt_name)
            result["PCR_来源"] = "东财实时"
    except Exception as exc:
        warn_list.append(f"东财期权PCR失败: {exc}")
        if rt_cached is not None and _cache_is_today(rt_cached):
            result["Vol_PCR"] = rt_cached["vol_pcr"]
            result["OI_PCR"] = rt_cached["oi_pcr"]
            cached_at = datetime.fromtimestamp(float(rt_cached["fetched_at"]), tz=CN_TZ).strftime("%H:%M")
            result["PCR_来源"] = f"缓存 {cached_at}（东财失败兜底）"
            warn_list.append(f"Vol/OI PCR 改用当日缓存（{cached_at}）")


def _format_iv_structure(diff: float) -> str:
    if diff > 2.0:
        return f"近月倒挂({diff:+.2f}%, 短期恐慌)"
    if diff < -2.0:
        return f"正斜率({diff:+.2f}%, 情绪平稳)"
    return f"平坦({diff:+.2f}%)"


def compute_50etf_option_module(
    workers: int = 4,
    rt_ttl: float = _RT_CACHE_TTL,
    push2_cooldown: float = 3600.0,
) -> Dict[str, object]:
    result: Dict[str, object] = {
        "QVIX": None,
        "QVIX_60日分位": None,
        "Vol_PCR": None,
        "OI_PCR": None,
        "近月平值IV": None,
        "次月平值IV": None,
        "IV_diff": None,
        "IV期限结构": "数据不足",
        "PCR_来源": "东财实时",
        "warnings": [],
    }
    warn_list: List[str] = []

    def _fetch_rt():
        fn = _resolve_ak_func("option_realtime_em", "option_current_em")
        return fn.__name__, call_push2_guarded(fn, base_cooldown_sec=push2_cooldown)

    rt_cached = _load_rt_cache()
    rt_age = (time.time() - float(rt_cached["fetched_at"])) if rt_cached else None
    force_rt = now_cn().time() >= dt_time(15, 0)
    use_cache = (
        rt_cached is not None
        and rt_age is not None
        and rt_age < rt_ttl
        and not force_rt
        and (rt_cached["vol_pcr"] is not None or rt_cached["oi_pcr"] is not None)
    )
    tasks: Dict[str, Callable[[], object]] = {
        "qvix": lambda: retry_call(_resolve_ak_func("index_option_50etf_qvix"), retries=2),
        "val": lambda: retry_call(_resolve_ak_func("option_value_analysis_em"), retries=2),
        "risk": lambda: retry_call(_resolve_ak_func("option_risk_analysis_em"), retries=2, allow_empty=True),
    }
    if not use_cache:
        tasks["rt"] = _fetch_rt
    fetched = run_parallel(tasks, workers=workers)

    # 1. QVIX 及近 60 日历史分位 (独立源 1.optbbs.com，可作最终降级因子)
    try:
        qvix_df, qvix_exc = fetched["qvix"]
        if qvix_exc is not None:
            raise qvix_exc
        qvix_ser = pd.to_numeric(qvix_df["close"], errors="coerce").dropna()
        if not qvix_ser.empty:
            cur_qvix = float(qvix_ser.iloc[-1])
            result["QVIX"] = round(cur_qvix, 2)
            past_q = qvix_ser.iloc[:-1].tail(60)
            if len(past_q) >= 10:
                result["QVIX_60日分位"] = round(float((past_q <= cur_qvix).mean()), 3)
    except Exception as exc:
        warn_list.append(f"QVIX获取失败: {exc}")

    # 2. 东财主路：PCR
    if use_cache:
        result["Vol_PCR"] = rt_cached["vol_pcr"]
        result["OI_PCR"] = rt_cached["oi_pcr"]
        cached_at = datetime.fromtimestamp(float(rt_cached["fetched_at"]), tz=CN_TZ).strftime("%H:%M")
        result["PCR_来源"] = f"缓存 {cached_at}（{rt_age / 60:.0f} 分钟前）"
        warn_list.append(f"Vol/OI PCR 使用缓存（{cached_at} 拉取，{rt_age / 60:.0f} 分钟前），本期未请求 realtime_em")
    else:
        _fetch_rt_and_fill(result, warn_list, fetched, rt_cached)

    # 3. 东财主路：IV 期限结构
    try:
        val_df, val_exc = fetched["val"]
        if val_exc is not None:
            raise val_exc
        risk_df, risk_exc = fetched["risk"]
        if risk_exc is not None:
            warn_list.append(f"option_risk_analysis_em 获取失败，平值IV改用中位数近似: {risk_exc}")
            risk_df = None
        total_rows = len(val_df)
        etf_val = val_df[val_df["期权名称"].astype(str).str.startswith("50ETF")].copy()

        if total_rows <= 100 or len(etf_val) == 0:
            warn_list.append(
                f"option_value_analysis_em 总行数={total_rows}, 50ETF行数={len(etf_val)} (疑似接口100条分页截断)"
            )

        if not etf_val.empty and "隐含波动率" in etf_val.columns and "到期日" in etf_val.columns:
            etf_val["隐含波动率"] = pd.to_numeric(etf_val["隐含波动率"], errors="coerce")
            etf_val["到期日"] = pd.to_datetime(etf_val["到期日"], errors="coerce")
            if (
                isinstance(risk_df, pd.DataFrame)
                and not risk_df.empty
                and {"期权代码", "Delta"} <= set(risk_df.columns)
            ):
                etf_val = etf_val.merge(
                    risk_df[["期权代码", "Delta"]].drop_duplicates("期权代码"),
                    on="期权代码",
                    how="left",
                )
            expiries = sorted(etf_val["到期日"].dropna().unique())
            if len(expiries) >= 2:
                near_iv = _extract_atm_iv(etf_val[etf_val["到期日"] == expiries[0]])
                next_iv = _extract_atm_iv(etf_val[etf_val["到期日"] == expiries[1]])
                result["近月平值IV"] = round(near_iv, 2) if near_iv is not None else None
                result["次月平值IV"] = round(next_iv, 2) if next_iv is not None else None
                if near_iv is not None and next_iv is not None:
                    diff = near_iv - next_iv
                    result["IV_diff"] = round(diff, 2)
                    result["IV期限结构"] = _format_iv_structure(diff)
            else:
                warn_list.append(f"50ETF 仅获取 {len(expiries)} 个到期月，无法计算期限结构")
    except Exception as exc:
        warn_list.append(f"东财期权IV失败: {exc}")

    # 4. 自动托底一层：仅当东财未拿到 PCR 或 IV 时，启动新浪 50ETF 精准批量托底
    need_pcr = result["Vol_PCR"] is None
    need_iv = result["IV_diff"] is None
    if need_pcr or need_iv:
        try:
            fb = _fallback_50etf_from_sina(need_pcr=need_pcr, need_iv=need_iv)
            if need_pcr and fb["Vol_PCR"] is not None:
                result["Vol_PCR"] = fb["Vol_PCR"]
                if result["OI_PCR"] is None:
                    result["OI_PCR"] = fb["OI_PCR"]
                result["PCR_来源"] = "新浪批量托底"
                _save_rt_cache(result["Vol_PCR"], result["OI_PCR"], "sina_batch_fallback")
                warn_list.append("东财限流/异常，Vol/OI PCR 已自动切换至新浪 50ETF 批量托底")
            if need_iv and fb["IV_diff"] is not None:
                result["近月平值IV"] = fb["近月平值IV"]
                result["次月平值IV"] = fb["次月平值IV"]
                result["IV_diff"] = fb["IV_diff"]
                result["IV期限结构"] = _format_iv_structure(float(fb["IV_diff"])) + "[新浪托底]"
                warn_list.append("东财IV异常，平值IV期限结构已自动切换至新浪托底")
        except Exception as exc:
            warn_list.append(f"新浪期权托底失败: {exc}")

    # 5. 自动托底二层：若 PCR 仍缺失，尝试上交所官网每日统计接口
    if result["Vol_PCR"] is None:
        try:
            sse_vol_pcr, sse_oi_pcr = _fallback_50etf_pcr_from_sse()
            if sse_vol_pcr is not None:
                result["Vol_PCR"] = sse_vol_pcr
                if result["OI_PCR"] is None:
                    result["OI_PCR"] = sse_oi_pcr
                result["PCR_来源"] = "上交所官网统计托底"
                warn_list.append("Vol/OI PCR 已切换至上交所官网每日统计托底")
        except Exception as exc:
            warn_list.append(f"上交所期权统计托底失败: {exc}")

    result["warnings"] = warn_list
    return result


# ==============================================================================
# 模块四：基于视频标准的「资金强弱百分比」量化打分引擎 (含 QVIX 最终降级托底)
# ==============================================================================
def _score_pcr(vol_pcr: float) -> float:
    if 0.70 <= vol_pcr <= 0.95:
        return 85.0
    if vol_pcr < 0.70:
        return float(np.clip(85.0 - (0.70 - vol_pcr) * 50.0, 65.0, 85.0))
    return float(np.clip(85.0 - (vol_pcr - 0.95) * 90.0, 10.0, 85.0))


def compute_capital_strength_pct(
    basis_metrics: Optional[Dict[str, object]],
    trin_res: Optional[Dict[str, object]],
    opt_res: Optional[Dict[str, object]],
) -> Dict[str, object]:
    fut_ok = bool(basis_metrics and basis_metrics.get("valid_count", 0) > 0)
    trin_ok = bool(trin_res and trin_res.get("有效家数", 0) > 0)
    opt_primary_ok = bool(opt_res and (opt_res.get("Vol_PCR") is not None or opt_res.get("IV_diff") is not None))
    opt_qvix_fallback = bool(not opt_primary_ok and opt_res and opt_res.get("QVIX_60日分位") is not None)
    opt_ok = opt_primary_ok or opt_qvix_fallback

    missing_modules = []
    if not fut_ok:
        missing_modules.append("股指期货")
    if not trin_ok:
        missing_modules.append("微观TRIN")
    if not opt_ok:
        missing_modules.append("50ETF期权")

    if not fut_ok and not trin_ok:
        return {
            "数据可信度": "数据缺失，分数不可信",
            "缺失模块": missing_modules,
            "综合资金强弱(%)": None,
            "期货资金强弱(%)": None,
            "微观综合强弱(%)": None,
            "权重吸金强度(%)": None,
            "市场真实广度(%)": None,
            "期权情绪支撑(%)": None,
            "盘面定性": "核心数据缺失，无法判定",
        }

    futures_strength_pct: Optional[float] = None
    if fut_ok and basis_metrics is not None:
        avg_pct = basis_metrics.get("avg_pct_60")
        avg_slope = float(basis_metrics.get("avg_ema3_slope", 0.0))
        vol_ratio = float(basis_metrics.get("avg_vol_ratio", 1.0))
        im_ih_spread = basis_metrics.get("IM_minus_IH")

        fut_parts: List[Tuple[float, float]] = []
        if avg_pct is not None:
            fut_parts.append((0.55, float(avg_pct) * 100.0))
        fut_slope_score = 50.0 + float(np.clip(avg_slope * 100.0 * 15.0, -50.0, 50.0))
        fut_parts.append((0.30, fut_slope_score))
        if im_ih_spread is not None:
            fut_struct_score = 50.0 + float(np.clip(float(im_ih_spread) * 100.0 * 5.0, -50.0, 50.0))
            fut_parts.append((0.15, fut_struct_score))

        w_sum = sum(w for w, _ in fut_parts)
        futures_strength_pct = clip_pct(sum(w * v for w, v in fut_parts) / w_sum)
        if vol_ratio >= 1.2:
            futures_strength_pct = clip_pct(50.0 + (futures_strength_pct - 50.0) * 1.15)

    micro_strength_pct: Optional[float] = None
    weight_capital_pct: Optional[float] = None
    market_breadth_pct: Optional[float] = None
    if trin_ok and trin_res is not None:
        up_cnt_pct = float(trin_res.get("涨家占比", 50.0))
        up_amt_pct = float(trin_res.get("涨额占比", 50.0))
        amt_spread = float(trin_res.get("额价差", 0.0))
        trin_amt = trin_res.get("TRIN_额")
        lim_up = int(trin_res.get("limit_up", 0))
        lim_dn = int(trin_res.get("limit_down", 0))

        if trin_amt is not None:
            trin_pull_score = float(np.clip(50.0 + (1.0 - float(trin_amt)) * 75.0, 0.0, 100.0))
        else:
            up_c = int(trin_res.get("up_cnt", 0))
            dn_c = int(trin_res.get("down_cnt", 0))
            if dn_c == 0 and up_c > 0:
                trin_pull_score = 100.0
            elif up_c == 0 and dn_c > 0:
                trin_pull_score = 0.0
            else:
                trin_pull_score = 50.0

        spread_pull_score = float(np.clip(50.0 + amt_spread * 3.0, 0.0, 100.0))
        weight_capital_pct = clip_pct(0.45 * trin_pull_score + 0.30 * spread_pull_score + 0.25 * up_amt_pct)

        if (lim_up + lim_dn) == 0:
            market_breadth_pct = clip_pct(up_cnt_pct)
        else:
            lim_ratio = lim_up / (lim_up + lim_dn) * 100.0
            market_breadth_pct = clip_pct(0.65 * up_cnt_pct + 0.35 * lim_ratio)

        micro_strength_pct = clip_pct(0.45 * weight_capital_pct + 0.55 * market_breadth_pct)

    # 托底三层：PCR 与 IV 全缺时，使用 QVIX 近 60 日分位输出降级期权分 (限制在 20~80%)
    option_strength_pct: Optional[float] = None
    if opt_primary_ok and opt_res is not None:
        opt_parts: List[Tuple[float, float]] = []
        vol_pcr = opt_res.get("Vol_PCR")
        iv_diff = opt_res.get("IV_diff")
        if vol_pcr is not None:
            opt_parts.append((0.60, _score_pcr(float(vol_pcr))))
        if iv_diff is not None:
            opt_parts.append((0.40, float(np.clip(50.0 - float(iv_diff) * 8.0, 0.0, 100.0))))
        if opt_parts:
            w_sum = sum(w for w, _ in opt_parts)
            option_strength_pct = clip_pct(sum(w * v for w, v in opt_parts) / w_sum)
    elif opt_qvix_fallback and opt_res is not None:
        qvix_pct = float(opt_res["QVIX_60日分位"])
        # QVIX 越高恐慌越强 -> 情绪支撑分越低，映射至 [20, 80]
        option_strength_pct = clip_pct(80.0 - qvix_pct * 60.0)

    weighted_sum, weight_total = 0.0, 0.0
    if futures_strength_pct is not None:
        weighted_sum += 0.40 * futures_strength_pct
        weight_total += 0.40
    if micro_strength_pct is not None:
        weighted_sum += 0.40 * micro_strength_pct
        weight_total += 0.40
    if option_strength_pct is not None:
        # 仅 QVIX 降级时权重减半为 0.10，防止单指标主导
        opt_w = 0.10 if opt_qvix_fallback else 0.20
        weighted_sum += opt_w * option_strength_pct
        weight_total += opt_w

    total_strength_pct = clip_pct(weighted_sum / weight_total) if weight_total > 0 else None
    if missing_modules:
        credibility = f"部分数据缺失({', '.join(missing_modules)})，分数降级参考"
    elif opt_qvix_fallback:
        credibility = "全维度已出分(期权仅QVIX60日分位降级托底)"
    else:
        credibility = "全维度数据完整"

    if total_strength_pct is None:
        regime_label = "无法判定"
    elif total_strength_pct >= 65.0 and (market_breadth_pct or 0.0) >= 55.0:
        regime_label = "强势多头共振（资金与宽度同步走强）"
    elif (
        weight_capital_pct is not None and market_breadth_pct is not None
        and weight_capital_pct >= 60.0 and market_breadth_pct < 35.0
    ):
        regime_label = "权重吸金护盘（资金强于宽度，个股失血，次日谨防弱修复）"
    elif total_strength_pct < 35.0 or (
        (futures_strength_pct is not None and futures_strength_pct < 35.0) and total_strength_pct < 40.0
    ):
        regime_label = "弱势空头主导（资金偏弱，下方仍有空间）"
    else:
        regime_label = "震荡分化格局（多空力量胶着）"

    return {
        "数据可信度": credibility,
        "缺失模块": missing_modules,
        "综合资金强弱(%)": total_strength_pct,
        "期货资金强弱(%)": futures_strength_pct,
        "微观综合强弱(%)": micro_strength_pct,
        "权重吸金强度(%)": weight_capital_pct,
        "市场真实广度(%)": market_breadth_pct,
        "期权情绪支撑(%)": option_strength_pct,
        "盘面定性": regime_label,
    }


def append_intraday_log(
    strength_res: Dict[str, object],
    trin_res: Optional[Dict[str, object]],
    opt_res: Optional[Dict[str, object]],
    basis_metrics: Optional[Dict[str, object]],
) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log_file = DATA_DIR / f"intraday_strength_{today_cn().strftime('%Y%m%d')}.csv"
    row = {
        "time": now_cn().strftime("%Y-%m-%d %H:%M:%S"),
        "credibility": strength_res["数据可信度"],
        "total_strength_pct": strength_res["综合资金强弱(%)"],
        "futures_pct": strength_res["期货资金强弱(%)"],
        "weight_capital_pct": strength_res["权重吸金强度(%)"],
        "breadth_pct": strength_res["市场真实广度(%)"],
        "option_pct": strength_res["期权情绪支撑(%)"],
        "trin_amt": trin_res.get("TRIN_额") if trin_res else None,
        "rel_trin": trin_res.get("Relative_TRIN") if trin_res else None,
        "amt_spread": trin_res.get("额价差") if trin_res else None,
        "if_30d_rate": round(float(basis_metrics["IF"]) * 100, 3) if basis_metrics and "IF" in basis_metrics else None,
        "im_ih_spread": round(float(basis_metrics["IM_minus_IH"]) * 100, 3) if basis_metrics and "IM_minus_IH" in basis_metrics else None,
        "vol_pcr": opt_res.get("Vol_PCR") if opt_res else None,
        "oi_pcr": opt_res.get("OI_PCR") if opt_res else None,
        "qvix": opt_res.get("QVIX") if opt_res else None,
        "regime": strength_res["盘面定性"],
    }
    pd.DataFrame([row]).to_csv(log_file, mode="a", header=not log_file.exists(), index=False, encoding="utf-8-sig")


def _fmt_pct(val: Optional[float]) -> str:
    return f"{val:.1f}%" if val is not None else "N/A"


_BASIS_CACHE: Dict[str, object] = {"ts": 0.0, "date": None, "df": None, "metrics": None}


def run_once(
    spot_source: str = "em",
    workers: int = 8,
    basis_ttl: float = 0.0,
    push2_cooldown: float = 3600.0,
) -> None:
    t0 = time.monotonic()
    with _TIMINGS_LOCK:
        _TIMINGS.clear()
    now_dt = now_cn()
    now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    print("\n" + "=" * 86)
    print(f"A股三维量化自动监控日报 | 采集时间: {now_str} | 行情源: {spot_source}")
    print("=" * 86)

    module_secs: Dict[str, float] = {}

    def _timed(name: str, fn: Callable):
        s = time.monotonic()
        try:
            return fn(), None
        except Exception as exc:
            return None, str(exc)
        finally:
            module_secs[name] = time.monotonic() - s

    cached_df = _BASIS_CACHE.get("df")
    cache_fresh = (
        basis_ttl > 0
        and _BASIS_CACHE.get("date") == now_dt.date()
        and isinstance(cached_df, pd.DataFrame)
        and not cached_df.empty
        and (time.monotonic() - float(_BASIS_CACHE["ts"])) < basis_ttl  # type: ignore[arg-type]
    )

    with ThreadPoolExecutor(max_workers=3) as pool:
        f_trin = pool.submit(
            _timed,
            "TRIN",
            lambda: compute_relative_trin_module(spot_source=spot_source, push2_cooldown=push2_cooldown),
        )
        f_opt = pool.submit(
            _timed,
            "期权",
            lambda: compute_50etf_option_module(workers=min(4, workers), push2_cooldown=push2_cooldown),
        )
        f_basis = None if cache_fresh else pool.submit(_timed, "期货", lambda: compute_basis_module(workers=workers))
        if f_basis is None:
            basis_out = ((_BASIS_CACHE["df"], _BASIS_CACHE["metrics"]), None)
            module_secs["期货"] = 0.0
        else:
            basis_out = f_basis.result()
        trin_out = f_trin.result()
        opt_out = f_opt.result()

    basis_pair, basis_err = basis_out
    basis_df, basis_metrics = basis_pair if basis_pair is not None else (pd.DataFrame(), None)
    if f_basis is not None and basis_err is None and basis_ttl > 0 and not basis_df.empty:
        _BASIS_CACHE.update(ts=time.monotonic(), date=now_dt.date(), df=basis_df, metrics=basis_metrics)
    trin_res, trin_err = trin_out
    opt_res, opt_err = opt_out

    strength_res = compute_capital_strength_pct(basis_metrics, trin_res, opt_res)

    print("\n[1] 股指期货 30日修正基差看板 (IF / IH / IC / IM)")
    if cache_fresh:
        print("  [提示] 期货基差基于日线数据，本轮沿用缓存结果")
    if basis_err:
        print(f"  [异常] 期货模块失败: {basis_err}")
    elif not basis_df.empty:
        print(basis_df.to_string(index=False))
        if basis_metrics and "IM_minus_IH" in basis_metrics:
            print(f"  >> IM - IH 贴水剪刀差: {float(basis_metrics['IM_minus_IH']) * 100:+.2f}%")
        if basis_metrics and basis_metrics.get("errors"):
            print(f"  >> 部分品种提示: {'; '.join(basis_metrics['errors'])}")
    else:
        print("  [提示] 无有效期货基差记录")

    print("\n[2] 微观量能广度 (TRIN) & 50ETF 期权情绪")
    if trin_err:
        print(f"  [异常] 微观TRIN模块失败: {trin_err}")
    elif trin_res:
        print(
            f"  有效 {trin_res['有效家数']} | 涨/跌/平 {trin_res['涨/跌/平']} | "
            f"涨停/跌停 {trin_res['涨停/跌停']} | 涨家占比 {trin_res['涨家占比']}% | "
            f"涨额占比 {trin_res['涨额占比']}% | 额价差 {trin_res['额价差']:+.2f}%"
        )
        print(
            f"  TRIN_额 {trin_res['TRIN_额']} | Relative_TRIN {trin_res['Relative_TRIN']} "
            f"({trin_res['trin_note']})"
        )

    if opt_err:
        print(f"  [异常] 期权模块失败: {opt_err}")
    elif opt_res:
        qvix_p = opt_res.get("QVIX_60日分位")
        qvix_p_str = f"({qvix_p * 100:.0f}%分位)" if isinstance(qvix_p, (int, float)) else ""
        print(
            f"  50ETF期权[{opt_res.get('PCR_来源', '东财实时')}]: QVIX {opt_res['QVIX']}{qvix_p_str} | "
            f"Vol_PCR {opt_res['Vol_PCR']} | OI_PCR {opt_res['OI_PCR']} | IV结构 {opt_res['IV期限结构']}"
        )
        if opt_res.get("warnings"):
            print(f"  >> 期权告警: {'; '.join(opt_res['warnings'])}")

    print("\n" + "-" * 86)
    print(f"★ 资金强弱百分比评估 [{strength_res['数据可信度']}] (本轮耗时 {time.monotonic() - t0:.1f}s)：")
    print(f"  综合资金强弱度 : {_fmt_pct(strength_res['综合资金强弱(%)']):>6}  -->  {strength_res['盘面定性']}")
    print(
        f"  分项拆解       : 期货主力 {_fmt_pct(strength_res['期货资金强弱(%)'])} | "
        f"权重吸金 {_fmt_pct(strength_res['权重吸金强度(%)'])} | "
        f"市场真实广度 {_fmt_pct(strength_res['市场真实广度(%)'])} | "
        f"期权情绪 {_fmt_pct(strength_res['期权情绪支撑(%)'])}"
    )
    with _TIMINGS_LOCK:
        slow = sorted(_TIMINGS.items(), key=lambda kv: kv[1], reverse=True)[:5]
    print("  模块耗时       : " + " | ".join(f"{k} {v:.1f}s" for k, v in module_secs.items()))
    print("  最慢接口(含重试): " + " | ".join(f"{k} {v:.1f}s" for k, v in slow))
    print("=" * 86)

    try:
        append_intraday_log(strength_res, trin_res, opt_res, basis_metrics)
    except Exception as exc:
        print(f"  [提示] 日内日志写入失败: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="A股三维量化自动监控与资金强弱评分脚本")
    parser.add_argument("--once", action="store_true", help="仅执行一次采集与评估后退出")
    parser.add_argument("--watch", action="store_true", help="开启盘中自动定时巡检模式")
    parser.add_argument("--interval", type=int, default=1200, help="巡检间隔秒数 (必须大于0，默认 1200 秒/20分钟)")
    parser.add_argument("--spot-source", choices=["em", "sina"], default="em", help="全市场行情源 (默认 em；em 熔断时自动切 sina 兜底)")
    parser.add_argument("--no-proxy-bypass", action="store_true", help="保留系统代理环境变量")
    parser.add_argument("--workers", type=int, default=8, help="并发线程数 (必须大于0，默认 8；1=近似串行)")
    parser.add_argument("--http-timeout", type=float, default=25.0, help="单次 HTTP 读取超时秒数 (默认 25；<=0 表示不设置)")
    parser.add_argument("--basis-ttl", type=int, default=900, help="--watch 下期货基差(日线数据)的复用秒数 (默认 900；0=每轮重算)")
    parser.add_argument("--push2-cooldown", type=float, default=3600.0, help="东财 push2 限流后的跨进程熔断基础秒数 (默认 3600/60分钟)")
    parser.add_argument("--reset-push2", action="store_true", help="运行前手动清除 push2 跨进程熔断状态")
    args = parser.parse_args()

    if args.interval <= 0:
        print("参数错误: --interval 必须大于 0。", file=sys.stderr)
        sys.exit(2)
    if args.workers <= 0:
        print("参数错误: --workers 必须大于 0。", file=sys.stderr)
        sys.exit(2)

    init_runtime_env(bypass_proxy=not args.no_proxy_bypass, http_timeout=args.http_timeout)
    if args.reset_push2:
        reset_push2_circuit_breaker()
        print("[提示] 已手动清除东财 push2 熔断状态。")

    if args.once or not args.watch:
        run_once(
            spot_source=args.spot_source,
            workers=args.workers,
            push2_cooldown=args.push2_cooldown,
        )
        return

    now = now_cn()
    if not is_trading_day(now.date()) or now.time() > dt_time(15, 2):
        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 当前为非交易日或已过收盘时间(15:02)，--watch 模式自动退出。如需拉取收盘快照请使用 --once。")
        return

    if args.spot_source == "sina" and args.interval < 180:
        print("[提示] 新浪全市场接口高频轮询存在暂时封 IP 风险，建议 --watch 模式使用默认 em 源或将 --interval 设为 >=180 秒。")
    print(f"已启动盘中自动监控模式 (刷新间隔 {args.interval} 秒，行情源={args.spot_source}，push2熔断基准={int(args.push2_cooldown)}s)...")
    lunch_notified = False
    try:
        while True:
            now = now_cn()
            if now.time() > dt_time(15, 2):
                try:
                    run_once(spot_source=args.spot_source, workers=args.workers, push2_cooldown=args.push2_cooldown)
                except Exception as exc:
                    print(f"[{now.strftime('%H:%M:%S')}] 收盘终轮归档异常: {exc}")
                print(f"\n[{now.strftime('%H:%M:%S')}] 已收盘，今日数据已归档至 monitor_data/，停止自动刷新。")
                break

            if is_trading_time(now):
                lunch_notified = False
                t_start = time.monotonic()
                try:
                    run_once(
                        spot_source=args.spot_source,
                        workers=args.workers,
                        basis_ttl=float(args.basis_ttl),
                        push2_cooldown=args.push2_cooldown,
                    )
                except Exception as exc:
                    print(f"[{now.strftime('%H:%M:%S')}] 本轮采集异常，将在下个周期重试: {exc}")
                elapsed = time.monotonic() - t_start
                if elapsed > args.interval:
                    print(f"[提示] 本轮耗时 {elapsed:.0f}s 已超过巡检间隔 {args.interval}s，请检查网络或调大 --interval。")
                time.sleep(max(1.0, args.interval - elapsed))
            elif is_lunch_break(now):
                if not lunch_notified:
                    print(f"[{now.strftime('%H:%M:%S')}] 午间休市 (11:32-13:00)，暂停采集...")
                    lunch_notified = True
                time.sleep(min(args.interval, 30))
            else:
                print(f"[{now.strftime('%H:%M:%S')}] 等待开盘...")
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n已手动停止监控。")


if __name__ == "__main__":
    main()