# quant-resonance-monitor

A股三维量化自动监控与资金强弱评分系统（AKShare）—— Instabox VPS 主站的 GitHub Actions 备份，交易日每 30 分钟生成报告页并提交到仓库。

## 这是什么

- 主站：Instabox VPS，每 20 分钟一期（09:30–15:05 CST），报告页见 VPS 公开链接。
- 备份：本仓库的 GitHub Actions，交易日每 30 分钟运行一次 `generate_report.py`，生成 `report/index.html` 并提交回仓库。
- 程序：`quant_resonance_monitor.py`（v11，含东财 push2 熔断器与新浪/上交所期权托底）+ `generate_report.py`（生成静态 HTML 报告页）。

## 定时

GitHub Actions（UTC，北京时间 = UTC+8）：

- 09:37、10:07、10:37、11:07、11:37、13:07、13:37、14:07、14:37、15:07 CST，每 30 分钟
- 15:12 CST 收盘快照
- 仅周一至周五；法定节假日由交易日判断步骤跳过，不生成新报告。

## 本地运行

```bash
pip install -r requirements.txt
python generate_report.py
```

报告生成在 `report/index.html`。单次运行（不生成 HTML）：

```bash
python quant_resonance_monitor.py --once
python quant_resonance_monitor.py --once --reset-push2  # 手动解除 push2 熔断
```

## 文件

- `quant_resonance_monitor.py`：监控主程序（期货基差、TRIN、50ETF 期权 PCR、打分）
- `generate_report.py`：每期生成静态 HTML 报告页
- `report/index.html`：最新一期报告（由 Actions 提交）
- `monitor_data/`：历史数据（基差/TRIN 历史、交易日历、日内日志、期权 PCR 缓存、push2 熔断状态），由 Actions 持久化提交，冷启动时需要

仅供参考，不构成投资建议。
