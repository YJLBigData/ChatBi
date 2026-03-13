import base64
import io
import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pymysql
from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.shared import Inches, Pt


BASE_DIR = Path(__file__).resolve().parent
REPORT_TEMPLATE_DIR = BASE_DIR / "report_templates"
DEFAULT_TEMPLATE_ID = "default-management-report"
DEFAULT_TEMPLATE_FILENAME = "default_management_report_template.docx"
TEMPLATE_PLACEHOLDER_GUIDE = [
    "{{report_title}}",
    "{{executive_summary}}",
    "{{management_summary}}",
    "{{professional_analysis}}",
    "{{strategy_recommendations}}",
    "{{management_actions}}",
    "{{risk_watchouts}}",
    "{{dashboard_snapshot}}",
    "{{detail_table}}",
]

REPORT_TEMPLATE_DDL = """
CREATE TABLE IF NOT EXISTS `report_template` (
    `template_id` VARCHAR(80) NOT NULL COMMENT '模板ID',
    `template_name` VARCHAR(255) NOT NULL COMMENT '模板名称',
    `template_kind` VARCHAR(32) NOT NULL DEFAULT 'custom' COMMENT '模板类型',
    `file_name` VARCHAR(255) NOT NULL COMMENT '原始文件名',
    `file_path` VARCHAR(512) NOT NULL COMMENT '文件路径',
    `style_profile_json` LONGTEXT NULL COMMENT '样式画像',
    `placeholders_json` LONGTEXT NULL COMMENT '占位符列表',
    `is_default` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否默认模板',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`template_id`),
    KEY `idx_report_template_updated_at` (`updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ChatBI报告模板表';
"""


def ensure_reporting_runtime(conn: pymysql.connections.Connection) -> None:
    REPORT_TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
    with conn.cursor() as cursor:
        cursor.execute(REPORT_TEMPLATE_DDL)
    seed_default_template(conn)


def seed_default_template(conn: pymysql.connections.Connection) -> None:
    default_path = REPORT_TEMPLATE_DIR / DEFAULT_TEMPLATE_FILENAME
    if not default_path.exists():
        create_default_template_file(default_path)

    metadata = parse_template_file(default_path)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO `report_template`
            (`template_id`, `template_name`, `template_kind`, `file_name`, `file_path`,
             `style_profile_json`, `placeholders_json`, `is_default`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE
                `template_name` = VALUES(`template_name`),
                `template_kind` = VALUES(`template_kind`),
                `file_name` = VALUES(`file_name`),
                `file_path` = VALUES(`file_path`),
                `style_profile_json` = VALUES(`style_profile_json`),
                `placeholders_json` = VALUES(`placeholders_json`),
                `is_default` = 1,
                `updated_at` = NOW()
            """,
            (
                DEFAULT_TEMPLATE_ID,
                "默认管理层商业分析报告模板",
                "default",
                default_path.name,
                str(default_path),
                json.dumps(metadata["style_profile"], ensure_ascii=False),
                json.dumps(metadata["placeholders"], ensure_ascii=False),
            ),
        )


def create_default_template_file(path: Path) -> None:
    document = Document()
    title = document.add_paragraph(style="Title")
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    title.add_run("ChatBI 管理层商业分析报告模板")

    subtitle = document.add_paragraph(style="Subtitle")
    subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
    subtitle.add_run("用于生成经营概览、看板快照、专业分析与策略建议")

    document.add_paragraph("样式说明：上传自定义模板时，系统会自动解析标题、正文、表格和列表样式。", style="Normal")

    document.add_paragraph("报告占位建议", style="Heading 1")
    document.add_paragraph("以下标记仅作为模板样例，系统会自动识别并提取模板风格：", style="Normal")
    for placeholder in TEMPLATE_PLACEHOLDER_GUIDE:
        document.add_paragraph(placeholder, style="List Bullet")

    document.add_paragraph("章节示例", style="Heading 1")
    document.add_paragraph("一、执行摘要", style="Heading 2")
    document.add_paragraph("建议保留管理层摘要、指标看板、问题诊断、策略建议、行动计划和风险提示等结构。", style="Normal")

    document.add_paragraph("二、看板快照", style="Heading 2")
    document.add_paragraph("可放置业务图表、关键指标表和明细摘要。", style="Normal")

    document.add_paragraph("三、策略建议", style="Heading 2")
    document.add_paragraph("适合沉淀经营策略、经营动作和复盘建议。", style="Normal")

    document.save(path)


def parse_template_file(file_path: str | Path) -> dict[str, Any]:
    path = Path(file_path)
    document = Document(path)
    style_names = {style.name for style in document.styles if getattr(style, "name", "")}
    placeholders = extract_placeholders(document)
    style_profile = {
        "title_style": pick_style_name(style_names, ["Title", "标题"]),
        "subtitle_style": pick_style_name(style_names, ["Subtitle", "副标题", "Normal"]),
        "heading_1_style": pick_style_name(style_names, ["Heading 1", "标题 1"]),
        "heading_2_style": pick_style_name(style_names, ["Heading 2", "标题 2"]),
        "body_style": pick_style_name(style_names, ["Normal", "正文"]),
        "bullet_style": pick_style_name(style_names, ["List Bullet", "项目符号", "Normal"]),
        "table_style": pick_table_style(document),
    }
    return {
        "style_profile": style_profile,
        "placeholders": placeholders,
        "file_name": path.name,
    }


def extract_placeholders(document: Document) -> list[str]:
    pattern = re.compile(r"\{\{[^{}]+\}\}")
    placeholders: list[str] = []
    for paragraph in document.paragraphs:
        for match in pattern.findall(paragraph.text or ""):
            if match not in placeholders:
                placeholders.append(match)
    for table in document.tables:
        for row in table.rows:
            for cell in row.cells:
                for match in pattern.findall(cell.text or ""):
                    if match not in placeholders:
                        placeholders.append(match)
    return placeholders


def pick_style_name(style_names: set[str], candidates: list[str]) -> str:
    lower_map = {name.lower(): name for name in style_names}
    for candidate in candidates:
        if candidate in style_names:
            return candidate
        matched = lower_map.get(candidate.lower())
        if matched:
            return matched
    return next(iter(style_names), "")


def pick_table_style(document: Document) -> str:
    if document.tables and document.tables[0].style and document.tables[0].style.name:
        return document.tables[0].style.name
    style_names = {style.name for style in document.styles if getattr(style, "name", "")}
    return pick_style_name(style_names, ["Table Grid", "网格型"])


def list_report_templates(conn: pymysql.connections.Connection) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT `template_id`, `template_name`, `template_kind`, `file_name`, `file_path`,
                   `style_profile_json`, `placeholders_json`, `is_default`, `updated_at`
            FROM `report_template`
            ORDER BY `is_default` DESC, `updated_at` DESC
            """
        )
        rows = list(cursor.fetchall())

    templates: list[dict[str, Any]] = []
    for row in rows:
        if not Path(row["file_path"]).exists():
            continue
        templates.append(
            {
                "template_id": row["template_id"],
                "template_name": row["template_name"],
                "template_kind": row["template_kind"],
                "file_name": row["file_name"],
                "is_default": bool(row["is_default"]),
                "style_profile": safe_json_dict(row.get("style_profile_json")),
                "placeholders": safe_json_list(row.get("placeholders_json")),
                "updated_at": str(row["updated_at"]),
            }
        )
    return templates


def get_report_template(conn: pymysql.connections.Connection, template_id: str | None) -> dict[str, Any]:
    if template_id:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT `template_id`, `template_name`, `template_kind`, `file_name`, `file_path`,
                       `style_profile_json`, `placeholders_json`, `is_default`
                FROM `report_template`
                WHERE `template_id` = %s
                """,
                (template_id,),
            )
            row = cursor.fetchone()
        if row and Path(row["file_path"]).exists():
            return {
                "template_id": row["template_id"],
                "template_name": row["template_name"],
                "template_kind": row["template_kind"],
                "file_name": row["file_name"],
                "file_path": row["file_path"],
                "style_profile": safe_json_dict(row.get("style_profile_json")),
                "placeholders": safe_json_list(row.get("placeholders_json")),
                "is_default": bool(row["is_default"]),
            }

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT `template_id`, `template_name`, `template_kind`, `file_name`, `file_path`,
                   `style_profile_json`, `placeholders_json`, `is_default`
            FROM `report_template`
            WHERE `is_default` = 1
            ORDER BY `updated_at` DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
    if not row:
        raise ValueError("未找到可用报告模板")
    return {
        "template_id": row["template_id"],
        "template_name": row["template_name"],
        "template_kind": row["template_kind"],
        "file_name": row["file_name"],
        "file_path": row["file_path"],
        "style_profile": safe_json_dict(row.get("style_profile_json")),
        "placeholders": safe_json_list(row.get("placeholders_json")),
        "is_default": bool(row["is_default"]),
    }


def save_uploaded_template(conn: pymysql.connections.Connection, file_storage: Any) -> dict[str, Any]:
    original_name = str(getattr(file_storage, "filename", "") or "").strip()
    if not original_name.lower().endswith(".docx"):
        raise ValueError("只支持上传 .docx 报告模板")

    safe_name = sanitize_filename(original_name)
    template_id = f"tpl_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
    target_path = REPORT_TEMPLATE_DIR / f"{template_id}_{safe_name}"
    file_storage.save(target_path)

    metadata = parse_template_file(target_path)
    template_name = target_path.stem.replace("_", " ")
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO `report_template`
            (`template_id`, `template_name`, `template_kind`, `file_name`, `file_path`,
             `style_profile_json`, `placeholders_json`, `is_default`)
            VALUES (%s, %s, 'custom', %s, %s, %s, %s, 0)
            """,
            (
                template_id,
                template_name,
                original_name,
                str(target_path),
                json.dumps(metadata["style_profile"], ensure_ascii=False),
                json.dumps(metadata["placeholders"], ensure_ascii=False),
            ),
        )
    return {
        "template_id": template_id,
        "template_name": template_name,
        "template_kind": "custom",
        "file_name": original_name,
        "style_profile": metadata["style_profile"],
        "placeholders": metadata["placeholders"],
        "is_default": False,
    }


def sanitize_filename(name: str) -> str:
    base_name = re.sub(r"[^\w\-.]+", "_", name.strip())
    return base_name or "report_template.docx"


def safe_json_dict(raw_value: Any) -> dict[str, Any]:
    if isinstance(raw_value, dict):
        return raw_value
    if not raw_value:
        return {}
    try:
        payload = json.loads(str(raw_value))
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def safe_json_list(raw_value: Any) -> list[Any]:
    if isinstance(raw_value, list):
        return raw_value
    if not raw_value:
        return []
    try:
        payload = json.loads(str(raw_value))
    except Exception:  # noqa: BLE001
        return []
    return payload if isinstance(payload, list) else []


def build_csv_bytes(latest_result: dict[str, Any]) -> bytes:
    import csv

    buffer = io.StringIO()
    rows = latest_result.get("rows", []) or []
    columns = latest_result.get("columns", []) or []
    if not columns and rows:
        columns = list(rows[0].keys())

    csv_writer = csv.writer(buffer)
    if columns:
        csv_writer.writerow(columns)
        for row in rows:
            csv_writer.writerow([row.get(column, "") for column in columns])
    return buffer.getvalue().encode("utf-8-sig")


def build_chart_word_bytes(
    template_row: dict[str, Any],
    latest_result: dict[str, Any],
    chart_images: list[dict[str, Any]],
) -> bytes:
    document = prepare_document(template_row)
    styles = template_row.get("style_profile", {})

    add_title(document, styles, latest_result.get("chart_title") or latest_result.get("metric_definition") or "图表快照导出")
    add_subtitle(
        document,
        styles,
        f"指标定义：{latest_result.get('metric_definition', '--')} | 指标：{', '.join(latest_result.get('metrics', [])) or '--'}",
    )

    add_heading(document, styles, "看板概览", level=1)
    dashboard_pairs = [
        ("问题", latest_result.get("question", "--")),
        ("指标定义", latest_result.get("metric_definition", "--")),
        ("指标描述", latest_result.get("metric_description", "--")),
        ("维度", "、".join(latest_result.get("dimensions", [])) or "整体汇总"),
        ("指标", "、".join(latest_result.get("metrics", [])) or "--"),
        ("返回行数", str(latest_result.get("row_count", 0))),
    ]
    add_key_value_table(document, dashboard_pairs, styles)

    add_heading(document, styles, "图表快照", level=1)
    if chart_images:
        add_chart_snapshots(document, chart_images, styles)
    else:
        add_body_paragraph(document, styles, "当前结果不适合生成柱图或饼图，因此本次导出仅包含明细结果。")

    add_heading(document, styles, "结果明细", level=1)
    add_result_table(document, latest_result.get("columns", []), latest_result.get("rows", []), styles, max_rows=80)

    add_heading(document, styles, "生成 SQL", level=1)
    add_body_paragraph(document, styles, latest_result.get("generated_sql", "--"))
    return save_document_to_bytes(document)


def build_management_report_docx(
    template_row: dict[str, Any],
    latest_result: dict[str, Any],
    report_payload: dict[str, Any],
    chart_images: list[dict[str, Any]],
) -> bytes:
    document = prepare_document(template_row)
    styles = template_row.get("style_profile", {})

    add_title(document, styles, report_payload.get("report_title") or latest_result.get("metric_definition") or "商业分析报告")
    add_subtitle(
        document,
        styles,
        report_payload.get("report_subtitle")
        or f"问题：{latest_result.get('question', '--')} | 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    )

    add_heading(document, styles, "一、执行摘要", level=1)
    add_body_paragraph(document, styles, report_payload.get("executive_summary", ""))

    add_heading(document, styles, "二、管理层结论", level=1)
    add_body_paragraph(document, styles, report_payload.get("management_summary", ""))

    add_heading(document, styles, "三、看板快照", level=1)
    dashboard_pairs = [
        ("指标定义", latest_result.get("metric_definition", "--")),
        ("指标描述", latest_result.get("metric_description", "--")),
        ("维度", "、".join(latest_result.get("dimensions", [])) or "整体汇总"),
        ("指标", "、".join(latest_result.get("metrics", [])) or "--"),
        ("数据行数", str(latest_result.get("row_count", 0))),
        ("图表标题", latest_result.get("chart_title", "--")),
    ]
    add_key_value_table(document, dashboard_pairs, styles)
    add_chart_snapshots(document, chart_images, styles)

    add_heading(document, styles, "四、关键发现", level=1)
    add_bullet_list(document, report_payload.get("key_findings", []), styles)

    add_heading(document, styles, "五、专业分析", level=1)
    for section in report_payload.get("professional_analysis", []):
        add_heading(document, styles, section.get("title", "分析章节"), level=2)
        add_body_paragraph(document, styles, section.get("content", ""))

    add_heading(document, styles, "六、策略建议", level=1)
    add_bullet_list(document, report_payload.get("strategy_recommendations", []), styles)

    add_heading(document, styles, "七、行动计划", level=1)
    add_bullet_list(document, report_payload.get("management_actions", []), styles)

    add_heading(document, styles, "八、风险与关注点", level=1)
    add_bullet_list(document, report_payload.get("risk_watchouts", []), styles)

    add_heading(document, styles, "九、结果明细摘录", level=1)
    add_result_table(document, latest_result.get("columns", []), latest_result.get("rows", []), styles, max_rows=30)

    add_heading(document, styles, "十、附录", level=1)
    add_body_paragraph(document, styles, report_payload.get("appendix_note", "本报告由 ChatBI 自动生成，建议结合业务背景进行最终审阅。"))
    add_body_paragraph(document, styles, f"生成 SQL：{latest_result.get('generated_sql', '--')}")
    return save_document_to_bytes(document)


def prepare_document(template_row: dict[str, Any]) -> Document:
    path = Path(template_row["file_path"])
    document = Document(path)
    clear_document_body(document)
    return document


def clear_document_body(document: Document) -> None:
    body = document._element.body
    sect_pr = body.sectPr
    for child in list(body):
        if sect_pr is not None and child == sect_pr:
            continue
        body.remove(child)


def save_document_to_bytes(document: Document) -> bytes:
    buffer = io.BytesIO()
    document.save(buffer)
    buffer.seek(0)
    return buffer.getvalue()


def add_title(document: Document, styles: dict[str, Any], text: str) -> None:
    paragraph = document.add_paragraph(style=resolve_style(document, styles.get("title_style")))
    paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = paragraph.add_run(text or "商业分析报告")
    run.bold = True
    run.font.size = Pt(20)


def add_subtitle(document: Document, styles: dict[str, Any], text: str) -> None:
    paragraph = document.add_paragraph(style=resolve_style(document, styles.get("subtitle_style")))
    paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
    paragraph.add_run(text or "")


def add_heading(document: Document, styles: dict[str, Any], text: str, level: int = 1) -> None:
    style_name = styles.get("heading_1_style") if level == 1 else styles.get("heading_2_style")
    document.add_paragraph(text or "", style=resolve_style(document, style_name))


def add_body_paragraph(document: Document, styles: dict[str, Any], text: str) -> None:
    for chunk in split_paragraphs(text):
        paragraph = document.add_paragraph(style=resolve_style(document, styles.get("body_style")))
        paragraph.add_run(chunk)


def add_bullet_list(document: Document, items: list[str], styles: dict[str, Any]) -> None:
    if not items:
        add_body_paragraph(document, styles, "暂无补充建议。")
        return
    for item in items:
        document.add_paragraph(item, style=resolve_style(document, styles.get("bullet_style")))


def add_key_value_table(document: Document, pairs: list[tuple[str, str]], styles: dict[str, Any]) -> None:
    table = document.add_table(rows=1, cols=2)
    apply_table_style(table, styles)
    table.rows[0].cells[0].text = "项目"
    table.rows[0].cells[1].text = "内容"
    for key, value in pairs:
        row = table.add_row().cells
        row[0].text = str(key)
        row[1].text = str(value)


def add_result_table(
    document: Document,
    columns: list[str],
    rows: list[dict[str, Any]],
    styles: dict[str, Any],
    max_rows: int = 30,
) -> None:
    if not columns:
        add_body_paragraph(document, styles, "当前结果无可展示数据。")
        return
    table = document.add_table(rows=1, cols=len(columns))
    apply_table_style(table, styles)
    for index, column in enumerate(columns):
        table.rows[0].cells[index].text = str(column)
    for row_data in rows[:max_rows]:
        cells = table.add_row().cells
        for index, column in enumerate(columns):
            cells[index].text = str(row_data.get(column, ""))
    if len(rows) > max_rows:
        add_body_paragraph(document, styles, f"本报告仅展示前 {max_rows} 行数据，完整明细建议通过 CSV 导出。")


def add_chart_snapshots(document: Document, chart_images: list[dict[str, Any]], styles: dict[str, Any]) -> None:
    if not chart_images:
        add_body_paragraph(document, styles, "当前结果未生成可嵌入的图表快照。")
        return
    for chart in chart_images:
        add_heading(document, styles, chart.get("title", "图表快照"), level=2)
        image_bytes = decode_data_url(chart.get("png_data_url", ""))
        if not image_bytes:
            add_body_paragraph(document, styles, "图表图片生成失败，已跳过。")
            continue
        picture_stream = io.BytesIO(image_bytes)
        document.add_picture(picture_stream, width=Inches(6.5))
        if chart.get("caption"):
            add_body_paragraph(document, styles, chart["caption"])


def apply_table_style(table: Any, styles: dict[str, Any]) -> None:
    style_name = styles.get("table_style")
    if style_name:
        try:
            table.style = style_name
        except Exception:  # noqa: BLE001
            try:
                table.style = "Table Grid"
            except Exception:  # noqa: BLE001
                pass


def resolve_style(document: Document, style_name: str | None) -> str | None:
    if not style_name:
        return None
    try:
        document.styles[style_name]
        return style_name
    except Exception:  # noqa: BLE001
        return None


def split_paragraphs(text: str) -> list[str]:
    cleaned = str(text or "").strip()
    if not cleaned:
        return [""]
    return [item.strip() for item in re.split(r"\n{2,}", cleaned) if item.strip()]


def decode_data_url(data_url: str) -> bytes:
    if not data_url or "," not in data_url:
        return b""
    try:
        return base64.b64decode(data_url.split(",", 1)[1])
    except Exception:  # noqa: BLE001
        return b""
