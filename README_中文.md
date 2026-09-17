# C/C++ 静态漏洞文献自动检索系统 v1.0

## 一、系统作用

自动完成：

OpenAlex + Semantic Scholar + arXiv + IEEE Xplore（四个自动来源）
DBLP 作为可选来源，只有 live probe 通过后才加入
→ Q1-Q7 检索
→ DOI / arXiv ID / 标题去重
→ 最近 14 天增量
→ seen_keys 历史去重
→ 生成 CSV
→ 生成 RIS
→ RIS 导入 Zotero
→ CSV 交给 Codex 做 S1 标题摘要筛选

IEEE Xplore 已在 `settings.json` 中启用，必须提供 `IEEE_API_KEY`。
DBLP 默认关闭；当前环境 probe 未通过，正式回溯暂不启用。
ACM Digital Library 只接受本地 RIS/CSV/BibTeX 导出，不使用网页爬虫：

```powershell
python scripts/import_acm.py "D:\\path\\acm-export.ris" --dry-run
```

---

## 二、第一次安装

建议 Python 3.11 或 3.12。

### Windows PowerShell

```powershell
cd C_CPP_Static_Vulnerability_Literature_Monitor_v1.0
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### macOS / Linux

```bash
cd C_CPP_Static_Vulnerability_Literature_Monitor_v1.0
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 三、配置 API Key

四个自动来源需要配置以下 Windows 用户环境变量；DBLP 不需要 Key，但当前环境的 DBLP 搜索接口会返回反爬页面，暂不列入正式自动回溯：

- `OPENALEX_API_KEY`
- `OPENALEX_POLITE_EMAIL`（用于 OpenAlex 礼貌池，也会写入 arXiv 的 User-Agent 联系信息）
- `S2_API_KEY`
- `IEEE_API_KEY`
- `ARXIV_API_URL`（可选；默认 `https://arxiv.org/api/query`，仅在需要切换官方端点时设置）

只需永久写入一次，不要把 Key 写进代码、`settings.json` 或 GitHub 仓库：

```powershell
[Environment]::SetEnvironmentVariable("OPENALEX_API_KEY", "你的OpenAlex Key", "User")
[Environment]::SetEnvironmentVariable("OPENALEX_POLITE_EMAIL", "你的联系邮箱", "User")
[Environment]::SetEnvironmentVariable("S2_API_KEY", "你的Semantic Scholar Key", "User")
[Environment]::SetEnvironmentVariable("IEEE_API_KEY", "你的IEEE Key", "User")
```

新开的 PowerShell 会自动继承这些变量。若要在当前窗口立即使用，执行以下刷新命令；它只读取变量，不打印 Key：

```powershell
$names = @("OPENALEX_API_KEY", "OPENALEX_POLITE_EMAIL", "S2_API_KEY", "IEEE_API_KEY")
foreach ($name in $names) {
    $value = [Environment]::GetEnvironmentVariable($name, "User")
    Set-Item -Path "Env:$name" -Value $value
}
```

---

## 四、第一次测试

先运行离线回归检查（不访问数据库，也不提交 `seen_keys`）：

```powershell
python scripts/validate_retrieval.py --self-test
```

再做四个正式自动来源的启动前检查。该命令不访问网络、不产生候选记录，也不提交 `seen_keys`：

```powershell
python scripts/run_backfill.py `
  --sources openalex,semantic_scholar,arxiv,ieee `
  --preflight-only
```

输出中的 `credentials_configured` 只有 `true/false`，不会显示 Key。只要 `errors` 非空，正式检索就会在发出网络请求前停止。

凭据预检不等于服务端授权。API Key 写入后、正式回溯前，对可用来源各做一次真实 probe：

```powershell
python scripts/run_backfill.py `
  --sources openalex,semantic_scholar `
  --live-probe-only
```

probe 只执行每个来源的最小请求，不写 CSV、不写 `seen_keys`。Semantic Scholar 当前配置为每次请求至少间隔 1.25 秒，并通过操作系统临时目录中的锁文件在多个本地进程间协调请求时间；仍建议一次只运行一个检索命令。锁文件不会写入仓库或提交到 Git。IEEE 激活后单独验证：

```powershell
python scripts/run_backfill.py --sources ieee --live-probe-only
```

必须看到 IEEE `status=ok` 和 HTTP 200；`Developer Inactive`/HTTP 403 表示 Key 仍未激活，不要开始回溯。

历史回溯的 arXiv 通道单独验证：

```powershell
python scripts/run_backfill.py --sources arxiv --live-probe-only
```

该命令探测官方 OAI-PMH `Identify` 接口，而不是容易对当前出口持续返回 429 的 Search API。若某个 CDN 地址在 TLS 阶段重置连接，OAI probe 会在 DNS 返回的其他地址上最多重试两次。每周增量命令的 `--live-probe-only` 仍探测 Search API，因为 14 天增量必须按实际投稿日期查询；两种 probe 的用途不能互换。

DBLP 当前 probe 返回 `Content-Type=text/html` 的反爬挑战页，而非 JSON。脚本会明确报告 `INVALID_RESPONSE`，不会把它伪装成零结果；在该 probe 通过前，正式回溯不要加入 DBLP。

然后在：

config/seed_titles.txt

填写 3–5 篇你已经确认高度相关的种子论文标题。第一次增量运行可使用：

```powershell
python scripts/run_incremental.py
python scripts/export_ris.py
```

输出：

exports/latest_new.csv
exports/latest_new.ris

---

## 五、Zotero

建议建立：

00_Inbox
10_Provisional_A
20_Provisional_B
30_Pending_Review
40_FullText
50_Final_A
60_Final_B
90_Excluded

将：

exports/latest_new.ris

导入 Zotero 的 00_Inbox。

注意：
脚本已经先执行一次 DOI / arXiv / 标题去重；
Zotero 主要用于处理预印本、会议版、期刊扩展版等“学术版本关系”。
有 DOI 或 arXiv 标识的记录只按强标识匹配历史状态；旧版题名键仅用于无强标识记录，避免同题名的不同 DOI 被误判为已见。

---

## 六、给 Codex 的文件

优先直接把：

exports/latest_new.csv

交给 Codex。

检索层 CSV 固定采用 canonical 22 列及当前顺序；不要按某一批次的临时筛选表重新定义列顺序，batch-001/batch-002 均按此格式读取。

不要必须经过 Zotero 再导 CSV，因为原始 CSV 保留：

source_database
query_id
original_record_id
retrieved_at
is_preprint
arxiv_id
formal_doi

这些字段属于检索证据链。

---

## 七、初始历史回溯

### 1. 先做真实小范围试跑

预检通过后，只运行 Q1 的 2024–2025 两年，并将结果写入验证目录：

```powershell
python scripts/run_backfill.py `
  --start-year 2024 `
  --end-year 2025 `
  --query-id Q1 `
  --sources openalex,semantic_scholar,arxiv,ieee `
  --max-per-query-year 50 `
  --dry-run `
  --run-id backfill-smoke-v3
```

`--dry-run` 仍会真实访问所选数据库，但不会提交 `seen_keys`。只有输出 `run_status=ok`、`failures=0` 且 `remaining_tasks=0` 才算通过；`degraded` 或 `failed` 会返回退出码 2，不能据此开始正式回溯。

### 2. 执行 2014–2026 正式回溯

在 IEEE probe 和 arXiv OAI-PMH probe 均通过后，严格限定所有查询族为 2014–2026。当前先使用已验证的四个自动来源；DBLP 暂不加入：

```powershell
python scripts/run_backfill.py `
  --start-year 2014 `
  --end-year 2026 `
  --sources openalex,semantic_scholar,arxiv,ieee `
  --max-per-query-year 300 `
  --run-id backfill-2014-2026-v3
```

若要保留协议默认的基础研究召回范围，不传 `--start-year`：Q1–Q5、Q7 从 2014 年开始，Q6 从 2000 年开始；其余参数不变。Q6 的 `foundation` 变体只在回溯中启用，避免每周增量被宽泛的基础程序分析结果淹没。

回溯按“来源 × 查询族 × 年份”保存 part CSV 和 checkpoint。某来源失败时，程序立即停止当前回溯，但此前已成功来源的 part 文件会保留；输出 `.partial.csv`，不提交 `seen_keys`，也不允许 `--allow-partial-commit`。修复临时限流或网络问题后，必须使用原来的全部参数和同一个 `run-id`，只额外加入 `--resume`：

```powershell
python scripts/run_backfill.py `
  --start-year 2014 `
  --end-year 2026 `
  --sources openalex,semantic_scholar,arxiv,ieee `
  --max-per-query-year 300 `
  --run-id backfill-2014-2026-v3 `
  --resume
```

续跑会跳过已完成的来源任务，只重跑失败和未开始任务。旧版 `backfill-smoke-v2` 使用已废弃的整年任务格式，不能迁移；请使用新的 `backfill-smoke-v3`/`backfill-2014-2026-v3`。参数不同会被拒绝；不要删除 checkpoint，也不要用新 `run-id` 冒充续跑。全部任务成功后才生成正式 CSV 并一次性提交历史去重状态。

若某一个来源遇到持续数小时或按日重置的配额限制，可在同一次续跑中加入 `--defer-failed-sources`。程序只尝试该来源的首个未完成任务一次；失败后保留其失败任务和所有后续任务为未完成，同时继续处理其他来源。此模式仍输出 `.partial.csv`、返回退出码 2，并且绝不提交 `seen_keys`：

```powershell
python scripts/run_backfill.py `
  --start-year 2014 `
  --end-year 2026 `
  --sources openalex,semantic_scholar,arxiv,ieee `
  --max-per-query-year 300 `
  --run-id backfill-2014-2026-v3 `
  --resume `
  --defer-failed-sources
```

配额恢复后使用上方普通 `--resume` 命令（不带 `--defer-failed-sources`）补齐该来源。只有 `run_status=ok`、`failures=0`、`remaining_tasks=0`、`state_committed=true` 才表示正式回溯完成。

历史回溯中的 arXiv 使用官方 OAI-PMH 批量元数据接口。第一次进入 arXiv 任务时，会把 `cs.CR`、`cs.SE`（选择 Q3 时还包括 `cs.AI`）从整个回溯下界开始收割到 checkpoint 内的 `arxiv_oai_cache.sqlite3`；每一页和 resumption token 都会立即落盘。OAI 的 `from` 表示元数据更新时间，因此脚本还会依据记录内的 `created` 日期进行本地年份过滤，并按原 Q1–Q7 arXiv 布尔式筛选。中断后使用相同命令加 `--resume`，会从缓存 token 继续；缓存完成后，其余 arXiv 年份/查询不会再访问网络。

官方接口说明：[arXiv OAI-PMH](https://info.arxiv.org/help/oa/index.html)；每周增量仍遵守 [arXiv Search API](https://info.arxiv.org/help/api/user-manual.html) 的请求间隔要求。

只有年份而没有日级日期的记录默认不进入窄增量窗口；历史回溯按年接收此类记录。`--allow-year-only-dates` 仅用于增量命令。

---

## 八、每周增量逻辑

每周增量继续使用 arXiv Search API，不使用 OAI-PMH。原因是 OAI datestamp 表示元数据更新日期，不能替代“最近 14 天投稿”条件；Search API 偶发 429 时本次运行会明确失败且不提交状态，稍后用新的增量 run 重试即可。

推荐：

每 7 天运行一次
+
每次回看最近 14 天
+
seen_keys.txt 排除以前已经出现的记录

例如：

第 1 周检索最近 14 天 → A/B/C
第 2 周再次检索最近 14 天 → B/C/D/E
seen_keys 自动去掉 B/C
最终只输出 D/E

这样可以容忍数据库收录延迟。

每次运行前会执行凭据预检；请求阶段按来源独立节流，并对 429 使用退避和熔断。只要有来源失败，默认不提交 `seen_keys`，命令返回退出码 2，避免把不完整检索误当成完整成功。日常任务不要使用 `--allow-partial-commit`。

---

## 九、GitHub Actions

项目已经包含 `.github/workflows/weekly-literature.yml`。其中：

- `cron: '23 0 * * 1'` 是每周一 UTC 00:23，即北京时间周一 08:23；GitHub 的定时任务可能因平台负载延迟几分钟。
- 自动任务固定使用 OpenAlex、Semantic Scholar、arXiv 和 IEEE Xplore，DBLP 不会被隐式启用。
- `concurrency` 会阻止同一分支的手动运行与定时运行重叠。
- 只有完整成功的增量运行才会提交 `seen_keys`、运行 manifest、增量 CSV 和 `latest_new` CSV/RIS；历史回溯 checkpoint 与 arXiv OAI 缓存不会加入每周提交。

### 配置 Secrets

在 GitHub 仓库中打开 `Settings` → `Secrets and variables` → `Actions` → `New repository secret`，逐一创建以下名称（名称必须完全一致）：

```text
OPENALEX_API_KEY
OPENALEX_POLITE_EMAIL
S2_API_KEY
IEEE_API_KEY
```

只填写值，不要把 Key 写入 YAML、`settings.json` 或代码。Windows 用户环境变量不会自动传给 GitHub Actions；GitHub Secrets 必须单独配置。

### 首次测试

1. 确认工作流文件已经位于仓库默认分支，并在 GitHub 的 `Actions` 页面启用 Actions。
2. 打开 `Actions` → `Weekly Literature Monitor` → `Run workflow`，选择默认分支和 `preflight`（手动触发的安全默认值）。该模式只检查 Secrets，不访问数据库、不写入结果；日志中的 `credentials_configured` 应全部为 `true`，不会显示 Key 内容。
3. 再运行一次 `probe`。该模式每个来源只发起最小探针，不写入 `seen_keys`；四个来源均成功后才进入下一步。
4. 运行 `dry-run`。它执行真实检索但不提交状态，日志应显示 `state_committed=false`；结果会作为该次运行的 Artifact 保存。
5. 确认前三步无误后，选择 `live` 手动运行一次。成功日志应包含 `run_status=ok`、`failures=0` 和 `state_committed=true`；仓库中应出现新的 `data/state/runs/weekly-<run_id>.json`、增量 CSV 以及 `exports/latest_new.csv` / `exports/latest_new.ris` 提交。

### 确认定时运行

定时任务只会在默认分支上的工作流文件生效。下一次运行应在 Actions 列表中显示事件为 `schedule`，并在日志中看到 14 天窗口的 `start`、`end`、四个来源调用和状态提交结果。GitHub 使用 UTC 解释 cron，因此无需在 YAML 中填写北京时间。

若工作流成功但无法推送提交，请在 `Settings` → `Actions` → `General` → `Workflow permissions` 选择 `Read and write permissions`，并确认目标分支没有阻止 GitHub Actions 直接推送的保护规则。若仓库长期没有活动，GitHub 也可能暂停定时工作流；在 Actions 页面重新启用即可。

---

## 十、启用 IEEE

当前 `config/settings.json` 已启用 IEEE。没有 `IEEE_API_KEY` 时，严格预检会在任何网络请求发出前终止本次运行，避免为每个查询重复记录同一凭据错误。

可以只对 IEEE 的 Q1 做一次 14 天 dry-run：

```powershell
python scripts/run_incremental.py `
  --sources ieee `
  --query-id Q1 `
  --limit 20 `
  --dry-run `
  --run-id ieee-check
```

IEEE 结果使用 `start_record` 分页。若本次明确不使用 IEEE，可传 `--no-ieee`；这会降低来源覆盖率，应在检索记录中注明。

DBLP 默认关闭。当前真实 probe 返回 DBLP 的反爬 HTML，自动 JSON 接口在本运行环境不可用；不要把它加入正式回溯。需要复查时先单独 probe：

```powershell
python scripts/run_backfill.py --sources dblp --live-probe-only
```

只有 probe 返回 `status=ok` 后，才可在新 run 中把 `dblp` 加入 `--sources`。其年份级日期在窄增量中默认排除，历史回溯仍可按年接收。

---

## 十一、ACM

ACM 默认不自动抓网页。

每周人工执行短检索并导出 RIS / BibTeX，
进入 Zotero 00_Inbox，
再进行 DOI / 标题 / 版本关系去重。

也可以直接导入本地导出文件，支持 RIS、CSV/TSV 和简化 BibTeX：

```powershell
python scripts/import_acm.py "D:\path\acm-export.ris" --dry-run
```

导入不会抓取 ACM 网页；解析异常或未知列会写入 manifest，并阻止默认状态提交（需修正字段映射后重试）。确认附加列不会影响规范字段后，可显式使用 `--allow-unmapped-columns` 放行状态提交。

---

## 十二、你每周最终需要做的事

自动程序：
检索 → 合并 → 去重 → 生成 CSV/RIS

Codex：
S1 标题摘要筛选

你：
重点复核 provisional-A、PENDING_REVIEW 和少量随机 EXCLUDE，
然后对高价值论文做全文复筛。
