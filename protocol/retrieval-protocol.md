# C/C++ 静态漏洞文献检索协议 v1.0

## 范围
- C/C++ 静态漏洞告警确认与误报分析
- Joern / CPG / CodeQL / Semgrep / SARIF
- CWE-78 为核心，CWE-120 / CWE-787 为迁移验证
- source / sink / taint / data-control dependence / call context / sanitization / reachability
- LLM evidence grounding / hallucination control / Unknown / abstention
- Juliet / SARD / 少量真实 CVE
- 强调可追溯、可复现的 Evidence Package

## 数据源
自动：OpenAlex、Semantic Scholar、arXiv
可选自动：IEEE Xplore（配置 API key 后）
人工补充：ACM Digital Library
核验：DBLP、Crossref、出版社/会议官网

## 时间
- Q1–Q5、Q7：2014-01-01 至今
- Q6：2000-01-01 至今
- 增量：每周运行一次，每次回看最近 14 天
- seen_keys.txt 去除已经出现的文献

## 去重优先级
DOI > arXiv ID > 规范化标题

## S1 输出
provisional-A / provisional-B / PENDING_REVIEW / EXCLUDE
标题摘要阶段不得输出 final-A。
