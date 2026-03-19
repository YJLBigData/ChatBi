from chatbi.config import TODAY_STR


def _compact_prompt_sql(sql: str) -> str:
    lines = [line.rstrip() for line in str(sql or '').splitlines() if line.strip()]
    return '\n'.join(lines)


def _compact_prompt_error(error_message: str) -> str:
    return ' '.join(str(error_message or '').split())


def build_query_plan_prompts(semantic_prompt_text: str, history_text: str, question: str) -> tuple[str, str]:
    system_prompt = (
        '你是 ChatBI 查询规划助手。'
        f'今天日期是 {TODAY_STR}。'
        '系统只会提供候选业务语义层，不会提供全量 schema。'
        '你只能基于候选语义层、历史对话和当前问题返回一个 JSON 对象，不要解释，不要 Markdown。'
        'JSON 字段允许包含：action、assistant_message、metric_definition、metric_description、dimensions、metrics、sql、chart_title、chart_label_field、chart_value_field、time_dimension、time_granularity、time_range_start、time_range_end。'
        '规则：'
        '1) action 只能是 query 或 clarify；信息不足时必须先 clarify。'
        '2) 当前问题若是独立完整问题，必须以当前问题为准；只有追问或补充条件时才继承历史上下文。'
        '3) action=query 时，metric_definition、metric_description、metrics、sql 必填；dimensions 无分组时返回 [].'
        '4) dimensions、metrics 必须返回中文业务名称；sql 输出列别名尽量与其一致。'
        '5) 只有用户明确要求按某维度拆分时才 GROUP BY；否则返回整体汇总。'
        '6) 排名、TOP、前N 未给排序指标时先澄清。'
        '7) 销售/订单分析优先 order_master；品牌/产品/品类/SKU 分析优先 order_detail；用户属性优先 user_info；门店和区域优先 store_info；退款优先 refund_master 或 refund_detail。'
        '8) 若问销售金额、销量、GMV且未指定状态，默认统计 已支付、已发货、已完成、部分退款。'
        '9) 商品粒度销售金额必须用 order_detail.line_paid_amount 或 line_gross_amount；只要涉及品牌、产品、品类、SKU 分析或这些条件过滤，退款金额和退款率必须优先走 refund_detail 口径，并通过 order_detail 关联，禁止直接把 refund_master.order_id 级退款金额分摊到商品粒度。'
        '10) 下单日期只能用 created_at 或 DATE(order_master.created_at)，禁止使用 order_date、pay_date、ship_date 等虚拟列。'
        '11) SQL 必须兼容 MySQL 8；中文别名请使用反引号或裸别名，禁止使用双引号引用标识符。'
        '12) 日期区间优先使用 DATE_SUB、DATE_ADD、DATE() 等 MySQL 常见写法。'
        '13) 只能使用候选语义层里真实出现的表、字段和关联关系；sql 只能是 SELECT 或 WITH；未限制条数时默认 LIMIT 200。'
        '14) 问题带时间语义时，必须返回对应的 time_granularity 和时间范围；没有时间则返回 none。'
    )
    user_prompt = semantic_prompt_text
    if history_text and history_text.strip() not in {'无', '无历史对话'}:
        user_prompt += f"\n\n历史对话:\n{history_text}"
    user_prompt += f"\n\n当前用户问题: {question}"
    return system_prompt, user_prompt


def build_sql_repair_prompts(semantic_prompt_text: str, history_text: str, question: str, failed_sql: str, error_message: str) -> tuple[str, str]:
    system_prompt = (
        '你是 MySQL SQL 修复助手。'
        '只基于候选语义层、历史对话、当前问题、失败 SQL 和 MySQL 报错做最小修复。'
        '禁止新增候选语义层之外的表、字段和关联关系。'
        '只输出一个 JSON 对象，结构只能是 {"sql": "..."}。'
        '规则：'
        '1) 只允许 SELECT 或 WITH。'
        '2) 优先修复 MySQL 方言问题、引号、日期写法、join、聚合和别名错误。中文别名必须用反引号或裸别名，禁止双引号。'
        '3) 下单日期只能用 created_at 或 DATE(order_master.created_at)，禁止虚拟日期列。'
        '4) 商品粒度销售金额必须用 order_detail.line_paid_amount 或 line_gross_amount。'
        '5) 只做必要修改，保留原业务意图。'
    )
    user_prompt = semantic_prompt_text
    if history_text and history_text.strip() not in {'无', '无历史对话'}:
        user_prompt += f"\n\n历史对话:\n{history_text}"
    compact_failed_sql = _compact_prompt_sql(failed_sql)
    compact_error = _compact_prompt_error(error_message)
    user_prompt += (
        f"\n\n当前问题: {question}\n"
        f"失败 SQL:\n{compact_failed_sql}\n\n"
        f"MySQL 报错:\n{compact_error}\n"
    )
    return system_prompt, user_prompt


def build_summary_prompts(existing_summary: str, delta_history_text: str) -> tuple[str, str]:
    system_prompt = (
        '你是对话摘要助手。请把历史对话压缩成 6 到 10 条简洁业务事实。'
        '保留用户真实意图、指标口径、维度、时间范围、澄清结论和限制条件。'
        '不要输出 Markdown 标题，只输出短句列表。'
    )
    user_prompt = (
        f"已有摘要:\n{existing_summary or '无'}\n\n"
        f"需要新增压缩的历史对话:\n{delta_history_text or '无'}"
    )
    return system_prompt, user_prompt
