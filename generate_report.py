#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""每 20 分钟跑一次 quant_resonance_monitor，生成静态 HTML 报告页。

用法: /data/workspace/quant-monitor/venv/bin/python generate_report.py
输出: report/index.html（原子替换），日志写到 run.log
"""
import html as htmlmod
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent
REPORT_DIR = BASE / "report"
LOG_FILE = BASE / "run.log"
sys.path.insert(0, str(BASE))

import pandas as pd  # noqa: E402

import quant_resonance_monitor as qrm  # noqa: E402

CST = timezone(timedelta(hours=8))


def log(msg: str) -> None:
    line = f"{datetime.now(CST).strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def fmt_pct(v):
    return f"{v:.1f}%" if v is not None else "N/A"


def fmt(v):
    return "N/A" if v is None else str(v)


def df_to_html(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "<p>无有效记录</p>"
    th = "".join(f"<th>{htmlmod.escape(str(c))}</th>" for c in df.columns)
    rows = ""
    for _, r in df.iterrows():
        rows += "<tr>" + "".join(
            f"<td>{htmlmod.escape(str(v))}</td>" for v in r
        ) + "</tr>"
    return f"<div class='scroll'><table><thead><tr>{th}</tr></thead><tbody>{rows}</tbody></table></div>"


def intraday_table() -> str:
    today = datetime.now(CST).strftime("%Y%m%d")
    f = qrm.DATA_DIR / f"intraday_strength_{today}.csv"
    if not f.exists():
        return "<p>今日暂无日内记录</p>"
    try:
        df = pd.read_csv(f)
        cols = [c for c in ("time", "total_strength_pct", "regime") if c in df.columns]
        if not cols:
            return "<p>日内记录格式异常</p>"
        df = df[cols].tail(20)
        df = df.rename(columns={"time": "时间", "total_strength_pct": "综合强弱", "regime": "盘面定性"})
        return df_to_html(df)
    except Exception as exc:
        return f"<p>日内记录读取失败: {htmlmod.escape(str(exc))}</p>"


def main() -> int:
    t0 = time.monotonic()
    log("开始采集")

    # 单实例锁:上一轮未跑完时,定时触发直接跳过,避免重叠采集
    import fcntl

    lock_f = open(BASE / "generate.lock", "a", encoding="utf-8")
    try:
        fcntl.flock(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log("上一轮仍在运行，本轮跳过")
        return 0
    errors = {}

    # v5: 初始化运行环境——清代理、设置 HTTP 超时(经 akshare 全局配置生效)、建数据目录。
    # 注意 generate_report.py 直接调 compute_*，不走 monitor 的 main()，不调这行超时配置不会生效。
    try:
        qrm.init_runtime_env(http_timeout=25.0)
    except SystemExit:
        raise
    except Exception as exc:
        errors["环境初始化"] = str(exc)

    basis_df, basis_metrics = pd.DataFrame(), None
    try:
        basis_df, basis_metrics = qrm.compute_basis_module()
    except Exception as exc:
        errors["期货"] = str(exc)

    trin_res, opt_res = None, None
    try:
        trin_res = qrm.compute_relative_trin_module()
    except Exception as exc:
        errors["微观TRIN"] = str(exc)
    try:
        opt_res = qrm.compute_50etf_option_module()
    except Exception as exc:
        errors["期权"] = str(exc)

    strength = qrm.compute_capital_strength_pct(basis_metrics, trin_res, opt_res)
    try:
        qrm.append_intraday_log(strength, trin_res, opt_res, basis_metrics)
    except Exception as exc:
        errors["日志落盘"] = str(exc)

    elapsed = time.monotonic() - t0
    now = datetime.now(CST)  # 用采集完成时间作为报告时间戳
    total = strength.get("综合资金强弱(%)")
    regime = strength.get("盘面定性", "无法判定")
    credibility = strength.get("数据可信度", "")
    missing = strength.get("缺失模块") or []
    next_run = now + timedelta(minutes=20)

    err_html = ""
    if errors:
        items = "".join(
            f"<li>{htmlmod.escape(k)}: {htmlmod.escape(str(v))}</li>"
            for k, v in errors.items()
        )
        err_html = f"<div class='warn'><b>本轮异常</b><ul>{items}</ul></div>"

    trin_html = "<p>微观模块失败</p>"
    if trin_res:
        trin_html = (
            f"<p>有效 {trin_res['有效家数']} 家 | 涨/跌/平 {trin_res['涨/跌/平']} | "
            f"涨停/跌停 {trin_res['涨停/跌停']} | 涨家占比 {trin_res['涨家占比']}% | "
            f"涨额占比 {trin_res['涨额占比']}% | 额价差 {trin_res['额价差']:+.2f}%</p>"
            f"<p>TRIN_额 {fmt(trin_res['TRIN_额'])} | Relative_TRIN {fmt(trin_res['Relative_TRIN'])} "
            f"({htmlmod.escape(str(trin_res['trin_note']))})</p>"
        )

    opt_html = "<p>期权模块失败</p>"
    if opt_res:
        warns = opt_res.get("warnings") or []
        w = ("<p class='muted'>" + "; ".join(htmlmod.escape(str(x)) for x in warns) + "</p>") if warns else ""
        opt_html = (
            f"<p>QVIX {fmt(opt_res['QVIX'])} | Vol_PCR {fmt(opt_res['Vol_PCR'])} | "
            f"OI_PCR {fmt(opt_res['OI_PCR'])} | IV结构 {htmlmod.escape(str(opt_res['IV期限结构']))}</p>{w}"
        )

    im_ih = ""
    if basis_metrics and "IM_minus_IH" in basis_metrics:
        im_ih = f"<p>IM - IH 贴水剪刀差: {float(basis_metrics['IM_minus_IH']) * 100:+.2f}%</p>"
    basis_err = ""
    if basis_metrics and basis_metrics.get("errors"):
        basis_err = "<p class='muted'>" + "; ".join(
            htmlmod.escape(str(x)) for x in basis_metrics["errors"]
        ) + "</p>"

    page = f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="1800">
<title>A股三维量化监控</title>
<style>
body{{font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;margin:0;padding:16px;background:#f5f6f8;color:#222}}
.card{{background:#fff;border-radius:12px;padding:16px;margin-bottom:12px;box-shadow:0 1px 3px rgba(0,0,0,.08)}}
h1{{font-size:19px;margin:0 0 4px}} h2{{font-size:16px;margin:0 0 8px}}
.score{{font-size:52px;font-weight:700;line-height:1.1}}
.regime{{font-size:17px;font-weight:600;margin-top:4px}}
.muted{{color:#888;font-size:13px}} .warn{{background:#fff4e5;border:1px solid #f5c98a;border-radius:8px;padding:10px}}
table{{border-collapse:collapse;width:100%;font-size:13px}}
th,td{{border-bottom:1px solid #eee;padding:6px 4px;text-align:left;white-space:nowrap}}
th{{color:#666}} .scroll{{overflow-x:auto}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:8px}}
.grid div{{background:#f5f6f8;border-radius:8px;padding:10px}}
.grid b{{display:block;font-size:12px;color:#666;font-weight:400}} .grid span{{font-size:17px;font-weight:600}}
</style></head><body>
<div class="card"><h1>A股三维量化监控</h1>
<p class="muted">生成时间 {now.strftime('%Y-%m-%d %H:%M:%S')}（北京时间）· 本轮耗时 {elapsed:.0f} 秒 · 下次更新约 {next_run.strftime('%H:%M')} · 每 20 分钟一期（交易时段）</p>
<div class="score">{fmt_pct(total)}</div>
<div class="regime">{htmlmod.escape(str(regime))}</div>
<p class="muted">{htmlmod.escape(str(credibility))}{" · 缺失: " + htmlmod.escape("、".join(str(x) for x in missing)) if missing else ""}</p>
<div class="grid">
<div><b>期货资金强弱</b><span>{fmt_pct(strength.get("期货资金强弱(%)"))}</span></div>
<div><b>权重吸金强度</b><span>{fmt_pct(strength.get("权重吸金强度(%)"))}</span></div>
<div><b>市场真实广度</b><span>{fmt_pct(strength.get("市场真实广度(%)"))}</span></div>
<div><b>期权情绪支撑</b><span>{fmt_pct(strength.get("期权情绪支撑(%)"))}</span></div>
</div></div>
{err_html}
<div class="card"><h2>股指期货 30日修正基差</h2>{df_to_html(basis_df)}{im_ih}{basis_err}</div>
<div class="card"><h2>微观量能广度 (TRIN)</h2>{trin_html}</div>
<div class="card"><h2>50ETF 期权情绪</h2>{opt_html}</div>
<div class="card"><h2>今日日内走势</h2>{intraday_table()}</div>
<p class="muted">数据来源: AKShare（东财/新浪/期权实时行情）。仅供参考，不构成投资建议。报告由 quant_resonance_monitor v10 生成。</p>
</body></html>"""

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    tmp = REPORT_DIR / "index.html.tmp"
    tmp.write_text(page, encoding="utf-8")
    tmp.replace(REPORT_DIR / "index.html")
    log(f"报告已生成，综合 {fmt_pct(total)}，{regime}，耗时 {elapsed:.0f}s")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        log("生成失败:\n" + traceback.format_exc())
        raise
