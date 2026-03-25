from __future__ import annotations

import json
from collections import defaultdict
from functools import lru_cache
from typing import Any

from chatbi.repository.db import get_db_conn
from chatbi.schema.knowledge_schema import (
    KNOWLEDGE_DIMENSION_DICT_DDL,
    KNOWLEDGE_FIELD_GLOSSARY_DDL,
    KNOWLEDGE_JOIN_GRAPH_DDL,
    KNOWLEDGE_METRIC_DICT_DDL,
    KNOWLEDGE_SQL_EXAMPLE_DDL,
    KNOWLEDGE_SYNONYM_DICT_DDL,
)
from chatbi.utils.question_utils import compact_whitespace


DEFAULT_KNOWLEDGE_METRICS = [
    {
        'metric_key': 'gmv', 'metric_name': 'GMV', 'business_definition': '按支付成功及履约中的订单口径统计销售金额。商品/品牌粒度必须使用订单明细行金额。',
        'calculation_rule': '整体订单分析优先 SUM(order_master.paid_amount)；商品分析优先 SUM(order_detail.line_paid_amount)。',
        'security_level': 'S0', 'keywords': ['gmv', '营业额', '成交额', '销售额', '订单总金额'], 'related_tables': ['order_master', 'order_detail'],
    },
    {
        'metric_key': 'net_sales', 'metric_name': '净销售额', 'business_definition': '销售金额扣减退款金额后的净收入口径。',
        'calculation_rule': '净销售额 = 销售金额 - 退款金额；商品粒度退款优先使用 refund_detail.refund_amount。',
        'security_level': 'S0', 'keywords': ['净销售额', '净收入', '净gmv'], 'related_tables': ['order_master', 'order_detail', 'refund_master', 'refund_detail'],
    },
    {
        'metric_key': 'refund_rate', 'metric_name': '退款率', 'business_definition': '退款金额占销售金额的比例，或退款单数占订单数比例，默认金额口径。',
        'calculation_rule': '退款率 = 退款金额 / 销售金额；若用户指定退款单率，则改用退款单数/订单数。',
        'security_level': 'S0', 'keywords': ['退款率', '退货率', '退款占比'], 'related_tables': ['refund_master', 'refund_detail', 'order_master', 'order_detail'],
    },
    {
        'metric_key': 'fulfillment_rate', 'metric_name': '履约率', 'business_definition': '已发货或已完成订单占支付订单的比例。',
        'calculation_rule': '履约率 = 已发货+已完成订单数 / 已支付及以上订单数。',
        'security_level': 'S0', 'keywords': ['履约率', '发货率'], 'related_tables': ['order_master'],
    },
    {
        'metric_key': 'repurchase_rate', 'metric_name': '复购率', 'business_definition': '在指定周期内发生二次及以上购买的买家数占支付买家数的比例。',
        'calculation_rule': '复购率 = 周期内下单次数>=2的去重 buyer_id / 周期内支付买家数。',
        'security_level': 'S1', 'keywords': ['复购率', '复购用户占比'], 'related_tables': ['order_master', 'user_info'],
    },
    {
        'metric_key': 'avg_order_value', 'metric_name': '客单价', 'business_definition': '销售金额除以订单数。',
        'calculation_rule': '客单价 = 销售金额 / COUNT(DISTINCT order_master.order_id)。',
        'security_level': 'S0', 'keywords': ['客单价', '平均订单金额'], 'related_tables': ['order_master'],
    },
]

DEFAULT_KNOWLEDGE_DIMENSIONS = [
    {'dimension_key': 'channel', 'dimension_name': '渠道', 'business_definition': '订单经营渠道，如线下门店、天猫、京东、抖音、小程序。', 'business_scope': '默认优先使用 order_master.sales_channel。', 'security_level': 'S0', 'keywords': ['渠道', '销售渠道'], 'related_tables': ['order_master', 'order_detail', 'inventory_stock']},
    {'dimension_key': 'store', 'dimension_name': '店铺', 'business_definition': '实际经营门店或店铺主体。', 'business_scope': '默认优先使用 store_info.store_name。', 'security_level': 'S0', 'keywords': ['店铺', '门店'], 'related_tables': ['store_info', 'order_master', 'inventory_stock']},
    {'dimension_key': 'warehouse', 'dimension_name': '仓库', 'business_definition': '库存归属仓库。', 'business_scope': '默认优先使用 inventory_stock.warehouse_name。', 'security_level': 'S0', 'keywords': ['仓库', '库房'], 'related_tables': ['inventory_stock']},
    {'dimension_key': 'product', 'dimension_name': '商品', 'business_definition': '商品或 SKU 粒度的分析对象。', 'business_scope': '优先使用 order_detail.product_name 或 product_info.product_name。', 'security_level': 'S0', 'keywords': ['商品', '产品', 'sku'], 'related_tables': ['order_detail', 'product_info', 'inventory_stock']},
    {'dimension_key': 'series', 'dimension_name': '系列', 'business_definition': '蒙牛产品系列，如特仑苏、纯甄、真果粒。', 'business_scope': '当前默认映射为品牌或 SPU 层级。', 'security_level': 'S0', 'keywords': ['系列'], 'related_tables': ['product_info', 'order_detail']},
    {'dimension_key': 'brand', 'dimension_name': '品牌', 'business_definition': '品牌分析维度。', 'business_scope': '默认优先使用 order_detail.brand_name。', 'security_level': 'S0', 'keywords': ['品牌'], 'related_tables': ['order_detail', 'product_info', 'inventory_stock']},
    {'dimension_key': 'country', 'dimension_name': '国家', 'business_definition': '国家维度，目前门店和用户维度具备国家字段。', 'business_scope': '默认优先使用 store_info.country 或 user_info.country_code。', 'security_level': 'S0', 'keywords': ['国家'], 'related_tables': ['store_info', 'user_info']},
]

DEFAULT_KNOWLEDGE_SYNONYMS = [
    ('metric', 'gmv', 'GMV', '营业额', 'S0'),
    ('metric', 'gmv', 'GMV', '成交额', 'S0'),
    ('metric', 'net_sales', '净销售额', '净gmv', 'S0'),
    ('metric', 'refund_rate', '退款率', '退货率', 'S0'),
    ('metric', 'avg_order_value', '客单价', '平均订单金额', 'S0'),
    ('metric', 'refund_amount_success', '退款成功金额', 'refund_amount_success', 'S1'),
    ('dimension', 'channel', '渠道', '销售渠道', 'S0'),
    ('dimension', 'store', '店铺', '门店', 'S0'),
    ('dimension', 'warehouse', '仓库', '库房', 'S0'),
]

DEFAULT_KNOWLEDGE_JOINS = [
    {'join_key': 'order_to_detail', 'left_table': 'order_master', 'right_table': 'order_detail', 'join_condition': 'order_master.order_id = order_detail.order_id', 'business_meaning': '订单主表与订单明细一对多关联。', 'security_level': 'S1'},
    {'join_key': 'order_to_user', 'left_table': 'order_master', 'right_table': 'user_info', 'join_condition': 'order_master.buyer_id = user_info.user_id', 'business_meaning': '订单与买家用户关联。', 'security_level': 'S2'},
    {'join_key': 'order_to_store', 'left_table': 'order_master', 'right_table': 'store_info', 'join_condition': 'order_master.store_id = store_info.store_id', 'business_meaning': '订单与经营门店关联。', 'security_level': 'S1'},
    {'join_key': 'refund_to_order', 'left_table': 'refund_master', 'right_table': 'order_master', 'join_condition': 'refund_master.order_id = order_master.order_id', 'business_meaning': '退款主表回连订单主表。', 'security_level': 'S1'},
    {'join_key': 'refund_detail_to_order_detail', 'left_table': 'refund_detail', 'right_table': 'order_detail', 'join_condition': 'refund_detail.order_detail_id = order_detail.order_detail_id', 'business_meaning': '退款商品明细回连订单商品明细。', 'security_level': 'S1'},
    {'join_key': 'inventory_to_store', 'left_table': 'inventory_stock', 'right_table': 'store_info', 'join_condition': 'inventory_stock.store_id = store_info.store_id', 'business_meaning': '库存与经营门店关联。', 'security_level': 'S1'},
    {'join_key': 'inventory_to_product', 'left_table': 'inventory_stock', 'right_table': 'product_info', 'join_condition': 'inventory_stock.product_id = product_info.product_id', 'business_meaning': '库存与商品主数据关联。', 'security_level': 'S1'},
]

DEFAULT_FIELD_GLOSSARY = [
    ('order_master', 'paid_amount', '订单实付金额', '订单最终支付给平台或商家的金额，不含未支付订单。', 'S1'),
    ('order_master', 'sales_channel', '销售渠道', '订单成交渠道，用于电商经营渠道分析。', 'S0'),
    ('order_master', 'buyer_id', '买家ID', '订单所属买家唯一标识，涉及用户隐私，应按脱敏规则处理。', 'S2'),
    ('order_detail', 'line_paid_amount', '行实付金额', '订单明细行对应商品的实付金额，商品粒度分析默认使用该字段。', 'S1'),
    ('refund_master', 'refund_amount', '退款金额', '退款单级别退款金额，适用于订单粒度售后分析。', 'S1'),
    ('refund_detail', 'refund_amount', '退款明细金额', '退款商品行金额，适用于品牌、商品、品类粒度退款分析。', 'S1'),
    ('inventory_stock', 'available_qty', '可售库存', '当前可供销售的库存数量，已扣减预留库存。', 'S0'),
    ('inventory_stock', 'inventory_amount', '库存金额', '当前库存金额，通常按成本或标准价计算。', 'S1'),
    ('user_info', 'member_level', '会员等级', '用户会员分层，用于会员经营分析。', 'S1'),
]

DEFAULT_SQL_EXAMPLES = [
    {
        'example_key': 'sql_gmv_channel_30d',
        'title': '近30天按渠道统计GMV',
        'question_text': '统计近30天各渠道GMV，按渠道展示',
        'sql_text': "SELECT order_master.sales_channel AS `销售渠道`, SUM(order_master.paid_amount) AS `GMV` FROM order_master WHERE order_master.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND order_master.order_status IN ('已支付','已发货','已完成','部分退款') GROUP BY order_master.sales_channel LIMIT 200",
        'quality_score': 95,
        'security_level': 'S1',
        'related_tables': ['order_master'],
        'related_metrics': ['GMV'],
        'related_dimensions': ['渠道'],
    },
    {
        'example_key': 'sql_brand_refund_rate_30d',
        'title': '近30天按品牌统计退款率',
        'question_text': '统计近30天各品牌退款金额和退款率，按退款率降序',
        'sql_text': "WITH sales AS (SELECT od.brand_name AS brand_name, SUM(od.line_paid_amount) AS sales_amount FROM order_master om JOIN order_detail od ON om.order_id = od.order_id WHERE om.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND om.order_status IN ('已支付','已发货','已完成','部分退款') GROUP BY od.brand_name), refunds AS (SELECT od.brand_name AS brand_name, SUM(rd.refund_amount) AS refund_amount FROM refund_detail rd JOIN refund_master rm ON rd.refund_id = rm.refund_id JOIN order_detail od ON rd.order_detail_id = od.order_detail_id WHERE rm.applied_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) GROUP BY od.brand_name) SELECT sales.brand_name AS `品牌`, sales.sales_amount AS `销售金额`, COALESCE(refunds.refund_amount,0) AS `退款金额`, CASE WHEN sales.sales_amount = 0 THEN 0 ELSE COALESCE(refunds.refund_amount,0) / sales.sales_amount END AS `退款率` FROM sales LEFT JOIN refunds ON sales.brand_name = refunds.brand_name ORDER BY `退款率` DESC LIMIT 200",
        'quality_score': 96,
        'security_level': 'S1',
        'related_tables': ['order_master', 'order_detail', 'refund_master', 'refund_detail'],
        'related_metrics': ['退款率', '退款金额', 'GMV'],
        'related_dimensions': ['品牌'],
    },
]


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _loads(value: Any) -> list[Any]:
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return []
    return payload if isinstance(payload, list) else []


def ensure_knowledge_runtime(conn=None) -> None:
    owns_conn = conn is None
    if owns_conn:
        conn = get_db_conn()
    assert conn is not None
    with conn.cursor() as cursor:
        cursor.execute(KNOWLEDGE_METRIC_DICT_DDL)
        cursor.execute(KNOWLEDGE_DIMENSION_DICT_DDL)
        cursor.execute(KNOWLEDGE_SYNONYM_DICT_DDL)
        cursor.execute(KNOWLEDGE_JOIN_GRAPH_DDL)
        cursor.execute(KNOWLEDGE_FIELD_GLOSSARY_DDL)
        cursor.execute(KNOWLEDGE_SQL_EXAMPLE_DDL)
    _seed_defaults(conn)
    conn.commit()
    if owns_conn:
        conn.close()


def _seed_defaults(conn) -> None:
    with conn.cursor() as cursor:
        for row in DEFAULT_KNOWLEDGE_METRICS:
            cursor.execute(
                """
                INSERT INTO knowledge_metric_dict
                (metric_key, metric_name, business_definition, calculation_rule, security_level, keywords_json, related_tables_json, is_active)
                VALUES (%s,%s,%s,%s,%s,%s,%s,1)
                ON DUPLICATE KEY UPDATE
                    metric_name=VALUES(metric_name),
                    business_definition=VALUES(business_definition),
                    calculation_rule=VALUES(calculation_rule),
                    security_level=VALUES(security_level),
                    keywords_json=VALUES(keywords_json),
                    related_tables_json=VALUES(related_tables_json),
                    is_active=1,
                    updated_at=NOW()
                """,
                (row['metric_key'], row['metric_name'], row['business_definition'], row['calculation_rule'], row['security_level'], _json_dumps(row['keywords']), _json_dumps(row['related_tables'])),
            )
        for row in DEFAULT_KNOWLEDGE_DIMENSIONS:
            cursor.execute(
                """
                INSERT INTO knowledge_dimension_dict
                (dimension_key, dimension_name, business_definition, business_scope, security_level, keywords_json, related_tables_json, is_active)
                VALUES (%s,%s,%s,%s,%s,%s,%s,1)
                ON DUPLICATE KEY UPDATE
                    dimension_name=VALUES(dimension_name),
                    business_definition=VALUES(business_definition),
                    business_scope=VALUES(business_scope),
                    security_level=VALUES(security_level),
                    keywords_json=VALUES(keywords_json),
                    related_tables_json=VALUES(related_tables_json),
                    is_active=1,
                    updated_at=NOW()
                """,
                (row['dimension_key'], row['dimension_name'], row['business_definition'], row['business_scope'], row['security_level'], _json_dumps(row['keywords']), _json_dumps(row['related_tables'])),
            )
        for target_type, target_key, standard_term, synonym_term, security_level in DEFAULT_KNOWLEDGE_SYNONYMS:
            cursor.execute(
                """
                INSERT INTO knowledge_synonym_dict
                (target_type, target_key, standard_term, synonym_term, security_level, is_active)
                VALUES (%s,%s,%s,%s,%s,1)
                ON DUPLICATE KEY UPDATE
                    standard_term=VALUES(standard_term),
                    security_level=VALUES(security_level),
                    is_active=1,
                    updated_at=NOW()
                """,
                (target_type, target_key, standard_term, synonym_term, security_level),
            )
        for row in DEFAULT_KNOWLEDGE_JOINS:
            cursor.execute(
                """
                INSERT INTO knowledge_join_graph
                (join_key, left_table, right_table, join_condition, business_meaning, security_level, is_active)
                VALUES (%s,%s,%s,%s,%s,%s,1)
                ON DUPLICATE KEY UPDATE
                    left_table=VALUES(left_table),
                    right_table=VALUES(right_table),
                    join_condition=VALUES(join_condition),
                    business_meaning=VALUES(business_meaning),
                    security_level=VALUES(security_level),
                    is_active=1,
                    updated_at=NOW()
                """,
                (row['join_key'], row['left_table'], row['right_table'], row['join_condition'], row['business_meaning'], row['security_level']),
            )
        for table_name, column_name, business_name, business_meaning, security_level in DEFAULT_FIELD_GLOSSARY:
            cursor.execute(
                """
                INSERT INTO knowledge_field_glossary
                (table_name, column_name, business_name, business_meaning, security_level, is_active)
                VALUES (%s,%s,%s,%s,%s,1)
                ON DUPLICATE KEY UPDATE
                    business_name=VALUES(business_name),
                    business_meaning=VALUES(business_meaning),
                    security_level=VALUES(security_level),
                    is_active=1,
                    updated_at=NOW()
                """,
                (table_name, column_name, business_name, business_meaning, security_level),
            )
        for row in DEFAULT_SQL_EXAMPLES:
            cursor.execute(
                """
                INSERT INTO knowledge_sql_example
                (example_key, title, question_text, sql_text, quality_score, security_level, related_tables_json, related_metrics_json, related_dimensions_json, is_active)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,1)
                ON DUPLICATE KEY UPDATE
                    title=VALUES(title),
                    question_text=VALUES(question_text),
                    sql_text=VALUES(sql_text),
                    quality_score=VALUES(quality_score),
                    security_level=VALUES(security_level),
                    related_tables_json=VALUES(related_tables_json),
                    related_metrics_json=VALUES(related_metrics_json),
                    related_dimensions_json=VALUES(related_dimensions_json),
                    is_active=1,
                    updated_at=NOW()
                """,
                (row['example_key'], row['title'], row['question_text'], row['sql_text'], row['quality_score'], row['security_level'], _json_dumps(row['related_tables']), _json_dumps(row['related_metrics']), _json_dumps(row['related_dimensions'])),
            )


def _normalize(text: str) -> str:
    return compact_whitespace(str(text or '').lower())


def _score_text(question: str, *terms: str) -> float:
    normalized_question = _normalize(question)
    score = 0.0
    for term in terms:
        normalized = _normalize(term)
        if normalized and normalized in normalized_question:
            score += max(1.0, min(6.0, len(normalized) / 2))
    return score


@lru_cache(maxsize=1)
def _load_knowledge_cache() -> dict[str, list[dict[str, Any]]]:
    ensure_knowledge_runtime()
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM knowledge_metric_dict WHERE is_active = 1")
            metrics = list(cursor.fetchall())
            cursor.execute("SELECT * FROM knowledge_dimension_dict WHERE is_active = 1")
            dimensions = list(cursor.fetchall())
            cursor.execute("SELECT * FROM knowledge_synonym_dict WHERE is_active = 1")
            synonyms = list(cursor.fetchall())
            cursor.execute("SELECT * FROM knowledge_join_graph WHERE is_active = 1")
            joins = list(cursor.fetchall())
            cursor.execute("SELECT * FROM knowledge_field_glossary WHERE is_active = 1")
            glossary = list(cursor.fetchall())
            cursor.execute("SELECT * FROM knowledge_sql_example WHERE is_active = 1 ORDER BY quality_score DESC")
            examples = list(cursor.fetchall())
    for row in metrics:
        row['keywords'] = _loads(row.get('keywords_json'))
        row['related_tables'] = _loads(row.get('related_tables_json'))
    for row in dimensions:
        row['keywords'] = _loads(row.get('keywords_json'))
        row['related_tables'] = _loads(row.get('related_tables_json'))
    for row in examples:
        row['related_tables'] = _loads(row.get('related_tables_json'))
        row['related_metrics'] = _loads(row.get('related_metrics_json'))
        row['related_dimensions'] = _loads(row.get('related_dimensions_json'))
    return {
        'metrics': metrics,
        'dimensions': dimensions,
        'synonyms': synonyms,
        'joins': joins,
        'field_glossary': glossary,
        'sql_examples': examples,
    }


def invalidate_knowledge_cache() -> None:
    _load_knowledge_cache.cache_clear()


def _calc_max_security(rows: list[dict[str, Any]]) -> str:
    max_level = 'S0'
    for row in rows:
        level = str(row.get('security_level') or 'S0').upper()
        if level == 'S2':
            return 'S2'
        if level == 'S1':
            max_level = 'S1'
    return max_level


def compose_knowledge_prompt_text(context: dict[str, Any]) -> str:
    if not context:
        return ''
    sections: list[str] = []
    metric_rows = context.get('metrics') or []
    dimension_rows = context.get('dimensions') or []
    synonym_rows = context.get('synonyms') or []
    join_rows = context.get('joins') or []
    glossary_rows = context.get('field_glossary') or []
    example_rows = context.get('sql_examples') or []

    if metric_rows:
        sections.append('结构化指标字典:')
        for row in metric_rows:
            sections.append(f"- {row['metric_name']}：{row.get('business_definition', '')}；规则：{row.get('calculation_rule', '')}")
    if dimension_rows:
        sections.append('结构化维度字典:')
        for row in dimension_rows:
            sections.append(f"- {row['dimension_name']}：{row.get('business_definition', '')}；范围：{row.get('business_scope', '')}")
    if synonym_rows:
        sections.append('同义词词典:')
        for row in synonym_rows:
            sections.append(f"- {row.get('synonym_term', '')} = {row.get('standard_term', '')}")
    if join_rows:
        sections.append('表关系图谱:')
        for row in join_rows:
            sections.append(f"- {row.get('join_condition', '')}；含义：{row.get('business_meaning', '')}")
    if glossary_rows:
        sections.append('字段业务释义:')
        for row in glossary_rows:
            sections.append(
                f"- {row.get('table_name')}.{row.get('column_name')}（{row.get('business_name', '')}）：{row.get('business_meaning', '')}"
            )
    if example_rows:
        sections.append('高质量标准SQL样例库:')
        for row in example_rows:
            sections.append(f"- {row.get('title', '')}：{row.get('question_text', '')}\n  SQL: {row.get('sql_text', '')}")
    return '\n'.join(sections).strip()


def retrieve_knowledge_context(
    question: str,
    candidate_tables: list[str],
    candidate_metrics: list[str],
    candidate_dimensions: list[str],
    *,
    top_n: int = 3,
) -> dict[str, Any]:
    cache = _load_knowledge_cache()
    table_set = {str(item).strip() for item in candidate_tables if str(item).strip()}
    metric_set = {str(item).strip() for item in candidate_metrics if str(item).strip()}
    dimension_set = {str(item).strip() for item in candidate_dimensions if str(item).strip()}
    semantic_terms = metric_set.union(dimension_set)

    metric_rows = sorted(
        [
            row for row in cache['metrics']
            if row['metric_name'] in metric_set or _score_text(question, row['metric_name'], *row.get('keywords', [])) > 0
        ],
        key=lambda row: _score_text(question, row['metric_name'], *row.get('keywords', [])),
        reverse=True,
    )[:top_n]
    dimension_rows = sorted(
        [
            row for row in cache['dimensions']
            if row['dimension_name'] in dimension_set or _score_text(question, row['dimension_name'], *row.get('keywords', [])) > 0
        ],
        key=lambda row: _score_text(question, row['dimension_name'], *row.get('keywords', [])),
        reverse=True,
    )[:top_n]
    synonym_rows = sorted(
        [row for row in cache['synonyms'] if _score_text(question, row['synonym_term'], row['standard_term']) > 0],
        key=lambda row: _score_text(question, row['synonym_term'], row['standard_term']),
        reverse=True,
    )[:top_n]
    join_rows = [row for row in cache['joins'] if row['left_table'] in table_set and row['right_table'] in table_set][:top_n + 2]
    glossary_rows = sorted(
        [
            row for row in cache['field_glossary']
            if row['table_name'] in table_set and (
                _score_text(question, row.get('business_name', ''), row.get('column_name', ''), row.get('business_meaning', '')) > 0
                or any(term and term in row.get('business_meaning', '') for term in semantic_terms)
                or any(term and term in row.get('business_name', '') for term in semantic_terms)
            )
        ],
        key=lambda row: _score_text(question, row.get('business_name', ''), row.get('column_name', ''), row.get('business_meaning', '')),
        reverse=True,
    )[:6]
    example_rows = [
        row for row in cache['sql_examples']
        if (
            set(row.get('related_metrics', [])).intersection(metric_set)
            or set(row.get('related_dimensions', [])).intersection(dimension_set)
            or (
                set(row.get('related_tables', [])).intersection(table_set)
                and _score_text(question, row.get('title', ''), row.get('question_text', '')) > 0
            )
        )
    ][:2]

    all_rows = metric_rows + dimension_rows + synonym_rows + join_rows + glossary_rows + example_rows
    field_glossary_text = compose_knowledge_prompt_text({'field_glossary': glossary_rows}) if glossary_rows else ''
    sql_examples_text = compose_knowledge_prompt_text({'sql_examples': example_rows}) if example_rows else ''
    prompt_text = compose_knowledge_prompt_text(
        {
            'metrics': metric_rows,
            'dimensions': dimension_rows,
            'synonyms': synonym_rows,
            'joins': join_rows,
            'field_glossary': glossary_rows,
            'sql_examples': example_rows,
        }
    )
    return {
        'metrics': metric_rows,
        'dimensions': dimension_rows,
        'synonyms': synonym_rows,
        'joins': join_rows,
        'field_glossary': glossary_rows,
        'sql_examples': example_rows,
        'field_glossary_text': field_glossary_text,
        'sql_examples_text': sql_examples_text,
        'prompt_text': prompt_text,
        'max_security_level': _calc_max_security(all_rows),
    }
