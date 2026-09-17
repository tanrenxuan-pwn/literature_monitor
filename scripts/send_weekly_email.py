from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formatdate, make_msgid, parseaddr
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CSV = ROOT / "exports" / "latest_new.csv"
DEFAULT_RIS = ROOT / "exports" / "latest_new.ris"
DEFAULT_RUNS_DIR = ROOT / "data" / "state" / "runs"
DEFAULT_MAX_ITEMS = 25


def env_text(name: str) -> str:
    return os.environ.get(name, "").strip()


def read_csv_records(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise FileNotFoundError(f"CSV file does not exist: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {path}")
        return [
            {str(key): value or "" for key, value in row.items() if key is not None}
            for row in reader
        ]


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"Run manifest must contain a JSON object: {path}")
    return value


def manifest_sort_key(path: Path, manifest: dict[str, Any]) -> tuple[str, float]:
    timestamp = str(manifest.get("created_at") or manifest.get("finished_at") or "")
    return timestamp, path.stat().st_mtime


def resolve_manifest(
    explicit_path: Path | None,
    run_id: str,
    runs_dir: Path = DEFAULT_RUNS_DIR,
) -> tuple[Path | None, dict[str, Any]]:
    if explicit_path is not None:
        if not explicit_path.is_file():
            raise FileNotFoundError(f"Run manifest does not exist: {explicit_path}")
        return explicit_path, read_json(explicit_path)

    if run_id:
        direct = runs_dir / f"{run_id}.json"
        if direct.is_file():
            return direct, read_json(direct)

    candidates: list[tuple[tuple[str, float], Path, dict[str, Any]]] = []
    if runs_dir.is_dir():
        for path in runs_dir.glob("*.json"):
            try:
                manifest = read_json(path)
            except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
                continue
            if str(manifest.get("mode", "")).casefold() != "incremental":
                continue
            candidates.append((manifest_sort_key(path, manifest), path, manifest))

    if not candidates:
        return None, {}
    _, path, manifest = max(candidates, key=lambda item: item[0])
    return path, manifest


def integer_value(value: Any, fallback: int) -> int:
    if isinstance(value, bool):
        return fallback
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def failure_count(value: Any) -> int:
    if value in (None, "", [], {}):
        return 0
    if isinstance(value, (list, tuple, set, dict)):
        return len(value)
    return integer_value(value, 1)


def compact(value: Any, limit: int = 180) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def http_url(value: Any) -> str:
    text = compact(value, 1000)
    if not text:
        return ""
    try:
        parsed = urlsplit(text)
    except ValueError:
        return ""
    return text if parsed.scheme.casefold() in {"http", "https"} and parsed.netloc else ""


def doi_url(value: Any) -> str:
    doi = compact(value, 500)
    if not doi:
        return ""
    direct = http_url(doi)
    if direct:
        return direct
    doi = re.sub(r"^(?:doi\s*:\s*)", "", doi, flags=re.IGNORECASE)
    return f"https://doi.org/{doi}" if doi else ""


def paper_link(row: dict[str, str]) -> str:
    return http_url(row.get("url")) or doi_url(row.get("doi"))


def status_text(manifest: dict[str, Any]) -> str:
    run_status = compact(manifest.get("run_status")) or "unknown"
    committed = manifest.get("state_committed")
    if run_status == "ok" and committed is True:
        return "成功，检索状态已提交"
    if run_status == "ok" and committed is False:
        reason = compact(manifest.get("state_commit_reason"))
        return f"检索成功，状态未提交（{reason or '原因未记录'}）"
    if run_status == "ok":
        return "检索成功"
    return f"需要检查（run_status={run_status}）"


def report_values(manifest: dict[str, Any], exported_rows: int) -> dict[str, Any]:
    return {
        "start": compact(manifest.get("start")) or "未知",
        "end": compact(manifest.get("end")) or "未知",
        "created_at": compact(manifest.get("created_at")) or "未知",
        "run_id": compact(manifest.get("run_id")) or "未找到运行记录",
        "status": status_text(manifest),
        "raw": integer_value(manifest.get("raw_rows"), exported_rows),
        "unique": integer_value(manifest.get("unique_rows"), exported_rows),
        "new": integer_value(manifest.get("new_rows"), exported_rows),
        "failures": failure_count(manifest.get("source_failures")),
        "exported": exported_rows,
    }


def plain_paper(index: int, row: dict[str, str]) -> list[str]:
    title = compact(row.get("title"), 260) or "（题名缺失）"
    authors = compact(row.get("authors"), 220) or "作者未知"
    date = compact(row.get("publication_date") or row.get("publication_year")) or "日期未知"
    venue = compact(row.get("venue"), 160) or "来源刊物未知"
    source = compact(row.get("source_database"), 100) or "数据库未知"
    link = paper_link(row)
    lines = [f"{index}. {title}", f"   {authors}", f"   {date} | {venue} | {source}"]
    if link:
        lines.append(f"   {link}")
    return lines


def html_paper(index: int, row: dict[str, str]) -> str:
    title = compact(row.get("title"), 260) or "（题名缺失）"
    authors = compact(row.get("authors"), 220) or "作者未知"
    date = compact(row.get("publication_date") or row.get("publication_year")) or "日期未知"
    venue = compact(row.get("venue"), 160) or "来源刊物未知"
    source = compact(row.get("source_database"), 100) or "数据库未知"
    link = paper_link(row)
    safe_title = html.escape(title)
    if link:
        safe_title = f'<a href="{html.escape(link, quote=True)}">{safe_title}</a>'
    return (
        '<li style="margin:0 0 14px 0;padding-left:4px">'
        f'<div style="font-weight:600;color:#17212b">{safe_title}</div>'
        f'<div style="margin-top:3px;color:#45515d">{html.escape(authors)}</div>'
        f'<div style="margin-top:3px;color:#6a737d;font-size:13px">'
        f'{html.escape(date)} &nbsp;|&nbsp; {html.escape(venue)} &nbsp;|&nbsp; '
        f'{html.escape(source)}</div></li>'
    )


def build_report(
    rows: list[dict[str, str]],
    manifest: dict[str, Any],
    run_url: str,
    max_items: int,
) -> tuple[str, str, str]:
    values = report_values(manifest, len(rows))
    shown = rows[:max_items]
    subject = (
        f"[文献检索周报] {values['start']} 至 {values['end']}："
        f"新增 {values['new']} 篇"
    )

    plain_lines = [
        "每周文献检索周报",
        "",
        f"检索窗口：{values['start']} 至 {values['end']}",
        f"运行状态：{values['status']}",
        f"原始记录：{values['raw']}",
        f"合并去重：{values['unique']}",
        f"新增记录：{values['new']}",
        f"来源失败：{values['failures']}",
        f"运行编号：{values['run_id']}",
    ]
    if values["new"] != values["exported"]:
        plain_lines.append(f"附件 CSV 实际记录：{values['exported']}（与运行记录不一致，请检查）")
    safe_run_url = http_url(run_url)
    if safe_run_url:
        plain_lines.append(f"GitHub 运行详情：{safe_run_url}")

    plain_lines.extend(["", f"新增文献（显示 {len(shown)}/{len(rows)}）：", ""])
    if shown:
        for index, row in enumerate(shown, 1):
            plain_lines.extend(plain_paper(index, row))
            plain_lines.append("")
    else:
        plain_lines.append("本次没有新增记录。")
        plain_lines.append("")
    plain_lines.extend(
        [
            "完整记录见邮件附件 latest_new.csv 和 latest_new.ris。",
            "RIS 可直接导入 Zotero。",
            "",
            "本邮件由 Weekly Literature Monitor 自动生成。",
        ]
    )
    plain = "\n".join(plain_lines)

    warning_html = ""
    if values["new"] != values["exported"]:
        warning_html = (
            '<p style="padding:10px;background:#fff4d6;border-left:4px solid #d69e00">'
            f"附件 CSV 实际有 {values['exported']} 条记录，与运行记录不一致，请检查。"
            "</p>"
        )
    run_link_html = ""
    if safe_run_url:
        run_link_html = (
            f'<p><a href="{html.escape(safe_run_url, quote=True)}">查看 GitHub 运行详情</a></p>'
        )
    papers_html = "".join(html_paper(index, row) for index, row in enumerate(shown, 1))
    if not papers_html:
        papers_html = '<p style="color:#45515d">本次没有新增记录。</p>'
    html_body = f"""<!doctype html>
<html lang="zh-CN">
<body style="margin:0;background:#f4f6f8;color:#17212b;font-family:Arial,'Microsoft YaHei',sans-serif">
  <div style="max-width:760px;margin:0 auto;padding:24px 16px">
    <div style="background:#ffffff;border:1px solid #d8dee4;padding:24px">
      <h1 style="margin:0 0 18px;font-size:22px;letter-spacing:0">每周文献检索周报</h1>
      <table role="presentation" style="border-collapse:collapse;width:100%;font-size:14px">
        <tr><td style="padding:5px 12px 5px 0;color:#6a737d">检索窗口</td><td>{html.escape(str(values['start']))} 至 {html.escape(str(values['end']))}</td></tr>
        <tr><td style="padding:5px 12px 5px 0;color:#6a737d">运行状态</td><td>{html.escape(str(values['status']))}</td></tr>
        <tr><td style="padding:5px 12px 5px 0;color:#6a737d">原始记录</td><td>{values['raw']}</td></tr>
        <tr><td style="padding:5px 12px 5px 0;color:#6a737d">合并去重</td><td>{values['unique']}</td></tr>
        <tr><td style="padding:5px 12px 5px 0;color:#6a737d">新增记录</td><td><strong>{values['new']}</strong></td></tr>
        <tr><td style="padding:5px 12px 5px 0;color:#6a737d">来源失败</td><td>{values['failures']}</td></tr>
        <tr><td style="padding:5px 12px 5px 0;color:#6a737d">运行编号</td><td>{html.escape(str(values['run_id']))}</td></tr>
      </table>
      {warning_html}
      {run_link_html}
      <h2 style="margin:24px 0 14px;font-size:17px;letter-spacing:0">新增文献（显示 {len(shown)}/{len(rows)}）</h2>
      <ol style="margin:0;padding-left:24px">{papers_html}</ol>
      <p style="margin:22px 0 0;color:#45515d;font-size:13px">完整记录见附件 latest_new.csv 和 latest_new.ris；RIS 可直接导入 Zotero。</p>
    </div>
  </div>
</body>
</html>
"""
    return subject, plain, html_body


def split_recipients(value: str) -> list[str]:
    recipients: list[str] = []
    for token in re.split(r"[,;\n]+", value):
        token = token.strip()
        if not token:
            continue
        _, address = parseaddr(token)
        if not address or "@" not in address or any(char in address for char in "\r\n"):
            raise ValueError(f"Invalid recipient address: {token!r}")
        recipients.append(address)
    if not recipients:
        raise ValueError("MAIL_TO must contain at least one email address")
    return list(dict.fromkeys(recipients))


def required_setting(name: str) -> str:
    value = env_text(name)
    if not value:
        raise ValueError(f"Required environment variable is missing: {name}")
    return value


def build_message(
    subject: str,
    plain: str,
    html_body: str,
    csv_path: Path,
    ris_path: Path,
    sender: str,
    recipients: list[str],
) -> EmailMessage:
    if not ris_path.is_file():
        raise FileNotFoundError(f"RIS file does not exist: {ris_path}")
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message["Date"] = formatdate(localtime=False)
    message["Message-ID"] = make_msgid(domain=sender.rsplit("@", 1)[-1])
    message.set_content(plain, charset="utf-8")
    message.add_alternative(html_body, subtype="html", charset="utf-8")
    message.add_attachment(
        csv_path.read_bytes(),
        maintype="text",
        subtype="csv",
        filename="latest_new.csv",
    )
    message.add_attachment(
        ris_path.read_bytes(),
        maintype="application",
        subtype="x-research-info-systems",
        filename="latest_new.ris",
    )
    return message


def send_message(message: EmailMessage, recipients: list[str]) -> None:
    host = required_setting("SMTP_HOST")
    user = required_setting("SMTP_USER")
    password = required_setting("SMTP_PASSWORD")
    port_text = env_text("SMTP_PORT") or "465"
    timeout_text = env_text("SMTP_TIMEOUT") or "30"
    try:
        port = int(port_text)
        timeout = int(timeout_text)
    except ValueError as exc:
        raise ValueError("SMTP_PORT and SMTP_TIMEOUT must be integers") from exc
    if not 1 <= port <= 65535 or timeout <= 0:
        raise ValueError("SMTP_PORT or SMTP_TIMEOUT is outside the valid range")

    context = ssl.create_default_context()
    if port == 465:
        with smtplib.SMTP_SSL(host, port, timeout=timeout, context=context) as smtp:
            smtp.login(user, password)
            smtp.send_message(message, to_addrs=recipients)
    else:
        with smtplib.SMTP(host, port, timeout=timeout) as smtp:
            smtp.ehlo()
            smtp.starttls(context=context)
            smtp.ehlo()
            smtp.login(user, password)
            smtp.send_message(message, to_addrs=recipients)


def main() -> None:
    parser = argparse.ArgumentParser(description="Email the latest weekly literature report.")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--ris", type=Path, default=DEFAULT_RIS)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--max-items", type=int, default=DEFAULT_MAX_ITEMS)
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Build and print the plain-text report without connecting to SMTP.",
    )
    args = parser.parse_args()
    if args.max_items < 0:
        parser.error("--max-items cannot be negative")

    csv_path = args.csv.resolve()
    ris_path = args.ris.resolve()
    rows = read_csv_records(csv_path)
    manifest_path, manifest = resolve_manifest(args.manifest, env_text("RUN_ID"))
    subject, plain, html_body = build_report(
        rows,
        manifest,
        env_text("RUN_URL"),
        args.max_items,
    )

    if args.preview:
        print("Preview only: no email was sent.")
        print(f"Manifest: {manifest_path or 'not found'}")
        print(f"CSV records: {len(rows)}")
        print(f"RIS attachment: {ris_path}")
        print(f"Subject: {subject}")
        print("\n--- Plain-text body ---\n")
        print(plain)
        return

    user = required_setting("SMTP_USER")
    sender = env_text("MAIL_FROM") or user
    _, sender_address = parseaddr(sender)
    if not sender_address or "@" not in sender_address:
        raise ValueError("MAIL_FROM or SMTP_USER must be a valid email address")
    recipients = split_recipients(required_setting("MAIL_TO"))
    message = build_message(
        subject,
        plain,
        html_body,
        csv_path,
        ris_path,
        sender_address,
        recipients,
    )
    send_message(message, recipients)
    print(f"Weekly email sent successfully to {len(recipients)} recipient(s).")


if __name__ == "__main__":
    main()
