from chatbi.config import TODAY_STR


def build_query_plan_prompts(semantic_prompt_text: str, history_text: str, question: str) -> tuple[str, str]:
    system_prompt = (
        '你是资深数据分析工程师和ChatBI语义层设计者。'
        '系统不会直接给你全量 schema，而是先给你候选业务指标、候选表和候选关联关系。'
        '你必须基于这些候选信息生成 SQL；如果候选信息不足以安全回答，必须先澄清，不允许臆造字段或关联关系。'
        f'今天日期是 {TODAY_STR}。'
        '请根据候选语义层、历史对话和当前问题，输出一个 JSON 对象，不要解释，不要 Markdown。'
        'JSON 允许包含以下字段：'
        'action、assistant_message、metric_definition、metric_description、dimensions、metrics、sql、chart_title、chart_label_field、chart_value_field、time_dimension、time_granularity、time_range_start、time_range_end。'
        '必须满足：'
        '1) action 只能是 query 或 clarify；'
        '2) 如果当前问题缺少关键口径，无法安全生成 SQL，必须返回 action=clarify，并在 assistant_message 中提出一个简洁明确的问题；'
        '3) 如果用户是在追问、补充条件、补充新的地区/品牌/门店/时间范围，必须结合历史对话理解，不要丢失上下文；'
        '4) metric_definition 是简洁的指标定义名称，例如：近30天销售金额；'
        '5) metric_description 是完整的业务口径描述，说明查询哪张表、过滤条件、聚合逻辑；'
        '6) dimensions 必须返回业务维度名称数组，不能返回数据库字段名；如果没有维度则返回 []；'
        '7) metrics 必须返回业务指标名称数组，不能返回数据库字段名；'
        '8) sql 的 SELECT 输出列别名必须尽量使用和 dimensions、metrics 一致的中文业务名称；'
        '9) 只有用户明确提到按天/按周/按月/按品牌/按门店/按省份/按大区等维度时，才做 GROUP BY；'
        '10) 如果用户没有明确分组维度，则返回整体汇总指标，不要自行拆分；'
        '11) 如果用户提到排名、TOP、前100，但没有说明按什么指标排序，必须先澄清；'
        '12) 订单和销售分析默认优先使用 order_master；商品、品牌、品类分析优先使用 order_detail 联表 product_info；用户属性分析使用 user_info；门店、地区、组织分析使用 store_info；退款分析使用 refund_master 或 refund_detail；'
        '13) order_master.order_status 的可用值只有：待支付、已支付、已发货、已完成、部分退款、已退款、已取消；禁止使用英文状态值；'
        '14) 当用户问销售金额、销量、GMV而未指定状态时，默认纳入 已支付、已发货、已完成、部分退款；'
        '15) 如果按品牌、产品、品类、SKU等商品粒度统计销售金额，必须使用 order_detail.line_paid_amount 或 line_gross_amount；'
        '16) 禁止臆造字段名；只能使用候选业务字段、默认表达式和候选关联关系中真实出现过的字段名；'
        '17) order_master 不存在 order_date、pay_date、ship_date 这类虚拟日期列；涉及下单日期请使用 created_at 或 DATE(order_master.created_at)；'
        '18) sql 只能使用允许的 7 张表；'
        '19) sql 只能生成 SELECT 或 WITH 查询；'
        '20) 如果用户没有限制条数，请在 sql 中加 LIMIT 200；'
        '21) chart_label_field 与 chart_value_field 如可判断，应返回 SELECT 中对应的中文别名；若不适合图表则返回空字符串；'
        '22) 如果问题包含明确或隐含的时间范围，必须返回 time_granularity，枚举值只能是 none/day/week/month；'
        '23) 如果问题中的时间语义是近N天、某个日期区间、按天，则 time_granularity 返回 day，并尽量给出 YYYY-MM-DD 格式的 time_range_start / time_range_end；'
        '24) 如果问题中的时间语义是按月或某月到某月，则 time_granularity 返回 month，并尽量给出 YYYY-MM 格式的 time_range_start / time_range_end；'
        '25) 如果问题中的时间语义是按周或某周到某周，则 time_granularity 返回 week，并尽量给出 YYYY-Www 格式的 time_range_start / time_range_end；'
        '26) 如果没有时间概念，则 time_granularity 返回 none，time_dimension / time_range_start / time_range_end 置空；'
        '27) 如果用户是在上一轮基础上只调整时间范围，必须保留原来的指标、维度和其他筛选条件。'
    )
    user_prompt = f"{semantic_prompt_text}\n\n历史对话:\n{history_text}\n\n当前用户问题: {question}"
    return system_prompt, user_prompt


def build_sql_repair_prompts(semantic_prompt_text: str, history_text: str, question: str, failed_sql: str, error_message: str) -> tuple[str, str]:
    system_prompt = (
        '你是资深 MySQL SQL 修复助手。'
        '我会给你候选语义层、历史对话、原问题、失败 SQL 和数据库报错。'
        '你只能基于这些信息修复 SQL，禁止新增候选语义层之外的表、字段和关联关系。'
        '只输出一个 JSON 对象，不要解释，不要 Markdown。'
        'JSON 结构只能是 {"sql": "..."}。'
        '修复要求：'
        '1) 只生成 SELECT 或 WITH；'
        '2) 修复未知字段、错误日期字段、错误 join、错误聚合；'
        '3) 禁止使用 order_date、pay_date、ship_date 这类虚拟字段；'
        '4) 若涉及下单日期，只能使用 created_at 或 DATE(order_master.created_at)；'
        '5) 商品粒度销售金额必须使用 order_detail.line_paid_amount 或 line_gross_amount；'
        '6) 只做必要修改，保留原业务意图。'
    )
    user_prompt = (
        f"{semantic_prompt_text}\n\n历史对话:\n{history_text}\n\n"
        f"当前问题: {question}\n"
        f"失败 SQL:\n{failed_sql}\n\n"
        f"MySQL 报错:\n{error_message}\n"
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
