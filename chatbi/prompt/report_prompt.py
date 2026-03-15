from chatbi.config import REPORT_PREVIEW_MAX_ROWS


def build_report_prompts(
    latest_result: dict,
    context_text: str,
    preview_text: str,
    template_prompt_text: str = '',
) -> tuple[str, str]:
    system_prompt = (
        '你是资深商业分析总监，面向管理层撰写经营分析报告。'
        '请结合当前问题、指标口径、数据结果、上下文摘要，输出一份可直接发给管理层的专业商业分析报告。'
        '报告必须具备清晰结论、专业分析、经营策略、管理动作和风险提示。'
        '只输出 JSON，不要 Markdown，不要解释。'
        'JSON 字段固定为：'
        'report_title, report_subtitle, executive_summary, management_summary, key_findings, professional_analysis, strategy_recommendations, management_actions, risk_watchouts, appendix_note。'
        '要求：'
        '1) key_findings 为 3-6 条字符串数组；'
        '2) professional_analysis 为 2-4 个对象数组，每个对象包含 title 和 content；'
        '3) strategy_recommendations 为 3-6 条可执行策略建议；'
        '4) management_actions 为 3-6 条面向管理层的落地动作；'
        '5) risk_watchouts 为 2-5 条风险提示；'
        '6) 语言必须专业、克制、可执行；'
        '7) 如果数据行数较少，要明确说明分析边界，不要臆造结论。'
    )
    if template_prompt_text:
        system_prompt += '8) 必须尽量遵循给定的报告模板要求、章节结构、风格约束和写作提示。'
    user_prompt = (
        f"当前问题：{latest_result.get('question', '--')}\n"
        f"指标定义：{latest_result.get('metric_definition', '--')}\n"
        f"指标描述：{latest_result.get('metric_description', '--')}\n"
        f"维度：{'、'.join(latest_result.get('dimensions', [])) or '整体汇总'}\n"
        f"指标：{'、'.join(latest_result.get('metrics', [])) or '--'}\n"
        f"返回行数：{latest_result.get('row_count', 0)}\n"
        f"图表标题：{latest_result.get('chart_title', '--')}\n"
        f"生成 SQL：{latest_result.get('generated_sql', '--')}\n"
        f"上下文摘要：\n{context_text}\n\n"
        f"结果样本（最多前{REPORT_PREVIEW_MAX_ROWS}行）：\n{preview_text}"
    )
    if template_prompt_text:
        user_prompt += f"\n\n报告模板要求：\n{template_prompt_text}"
    return system_prompt, user_prompt
