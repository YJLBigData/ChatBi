from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable


@dataclass(frozen=True)
class EvalCase:
    case_id: str
    question: str
    expected_metrics: tuple[str, ...]
    expected_dimensions: tuple[str, ...]
    expected_tables: tuple[str, ...]
    expect_clarify: bool
    security_level: str
    domain: str
    expected_time_granularity: str = 'none'
    note: str = ''

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data['expected_metrics'] = list(self.expected_metrics)
        data['expected_dimensions'] = list(self.expected_dimensions)
        data['expected_tables'] = list(self.expected_tables)
        return data


SALES_DIMENSIONS: list[tuple[str, str, tuple[str, ...], str]] = [
    ('sales_channel', '销售渠道', ('order_master',), 'S0'),
    ('sales_region', '销售大区', ('order_master', 'store_info'), 'S0'),
    ('store_province', '省份', ('order_master', 'store_info'), 'S0'),
    ('platform', '平台', ('order_master',), 'S0'),
    ('payment_method', '支付方式', ('order_master',), 'S0'),
    ('channel_type', '渠道类型', ('order_master',), 'S0'),
    ('brand_name', '品牌', ('order_master', 'order_detail'), 'S0'),
    ('category_l1', '一级品类', ('order_master', 'order_detail'), 'S0'),
    ('member_level', '会员等级', ('order_master', 'user_info'), 'S1'),
    ('city_tier', '城市等级', ('order_master', 'user_info'), 'S1'),
    ('register_channel', '注册渠道', ('order_master', 'user_info'), 'S1'),
    ('promotion_type', '促销类型', ('order_master',), 'S1'),
    ('gender', '性别', ('order_master', 'user_info'), 'S2'),
    ('age', '年龄', ('order_master', 'user_info'), 'S2'),
    ('device_type', '设备类型', ('order_master', 'user_info'), 'S2'),
]

REFUND_DIMENSIONS: list[tuple[str, str, tuple[str, ...], str]] = [
    ('sales_channel', '销售渠道', ('refund_master', 'order_master'), 'S0'),
    ('sales_region', '销售大区', ('refund_master', 'order_master', 'store_info'), 'S0'),
    ('store_province', '省份', ('refund_master', 'order_master', 'store_info'), 'S0'),
    ('brand_name', '品牌', ('refund_master', 'refund_detail', 'order_detail', 'order_master'), 'S0'),
    ('category_l1', '一级品类', ('refund_master', 'refund_detail', 'order_detail', 'order_master'), 'S0'),
    ('member_level', '会员等级', ('refund_master', 'user_info', 'order_master'), 'S1'),
    ('register_channel', '注册渠道', ('refund_master', 'user_info', 'order_master'), 'S1'),
    ('refund_reason', '退款原因', ('refund_master', 'refund_detail'), 'S1'),
    ('refund_type', '退款类型', ('refund_master',), 'S1'),
]

INVENTORY_DIMENSIONS: list[tuple[str, str, tuple[str, ...], str]] = [
    ('sales_channel', '销售渠道', ('inventory_stock',), 'S1'),
    ('sales_region', '销售大区', ('inventory_stock', 'store_info'), 'S1'),
    ('warehouse_name', '仓库名称', ('inventory_stock',), 'S1'),
    ('warehouse_type', '仓库类型', ('inventory_stock',), 'S1'),
    ('stock_status', '库存状态', ('inventory_stock',), 'S1'),
    ('brand_name', '品牌', ('inventory_stock', 'product_info'), 'S1'),
    ('category_l1', '一级品类', ('inventory_stock', 'product_info'), 'S1'),
    ('product_name', '产品名称', ('inventory_stock', 'product_info'), 'S1'),
]

USER_PRODUCT_DIMENSIONS: list[tuple[str, str, tuple[str, ...], str]] = [
    ('member_level', '会员等级', ('order_master', 'user_info'), 'S1'),
    ('city_tier', '城市等级', ('order_master', 'user_info'), 'S1'),
    ('register_channel', '注册渠道', ('order_master', 'user_info'), 'S1'),
    ('target_group', '目标人群', ('order_detail', 'product_info', 'order_master'), 'S1'),
    ('temperature_zone', '温层', ('order_detail', 'product_info', 'order_master'), 'S1'),
    ('gender', '性别', ('order_master', 'user_info'), 'S2'),
    ('age', '年龄', ('order_master', 'user_info'), 'S2'),
    ('device_type', '设备类型', ('order_master', 'user_info'), 'S2'),
]

SALES_TEMPLATES: list[tuple[str, tuple[str, ...], str]] = [
    ('统计近30天各{label}的销售金额和订单数，按销售金额降序展示。', ('销售金额', '订单数'), 'day'),
    ('统计近30天各{label}的销售金额、退款金额和退款率，按销售金额降序展示。', ('销售金额', '退款金额', '退款率'), 'day'),
    ('统计近30天各{label}的销售金额、支付买家数和客单价，按销售金额降序展示。', ('销售金额', '支付买家数', '客单价'), 'day'),
]

REFUND_TEMPLATES: list[tuple[str, tuple[str, ...], str]] = [
    ('统计近30天各{label}的退款金额和退款单数，按退款金额降序展示。', ('退款金额', '退款单数'), 'day'),
    ('统计近30天各{label}的退款金额、退款件数和退款率，按退款金额降序展示。', ('退款金额', '退款件数', '退款率'), 'day'),
    ('统计近30天各{label}的退款金额、退款单数和退款件数，按退款单数降序展示。', ('退款金额', '退款单数', '退款件数'), 'day'),
]

INVENTORY_TEMPLATES: list[tuple[str, tuple[str, ...], str]] = [
    ('按{label}统计当前可售库存、在途库存和库存金额，按库存金额降序展示。', ('可售库存', '在途库存', '库存金额'), 'none'),
    ('按{label}统计当前可售库存和缺货SKU数，按缺货SKU数降序展示。', ('可售库存', '缺货SKU数'), 'none'),
    ('按{label}统计当前在库量、可售库存和库存金额。', ('在库量', '可售库存', '库存金额'), 'none'),
]

USER_PRODUCT_TEMPLATES: list[tuple[str, tuple[str, ...], str]] = [
    ('统计近30天各{label}的支付买家数、订单数和销售金额，按销售金额降序展示。', ('支付买家数', '订单数', '销售金额'), 'day'),
    ('统计近30天各{label}的支付买家数、退款单数和退款率，按支付买家数降序展示。', ('支付买家数', '退款单数', '退款率'), 'day'),
    ('统计近30天各{label}的销售金额、单均件数和客单价，按销售金额降序展示。', ('销售金额', '单均件数', '客单价'), 'day'),
]

FEATURED_BRANDS = ['蒙牛', '特仑苏', '纯甄', '真果粒']
FEATURED_PRODUCTS = ['特仑苏', '纯甄', '真果粒', '未来星', '冠益乳', '每日鲜语', '蒂兰圣雪']

SENSITIVE_USER_DIMENSIONS = {'gender', 'age', 'device_type'}
INTERNAL_USER_DIMENSIONS = {'member_level', 'city_tier', 'register_channel', 'target_group', 'temperature_zone'}

METRIC_TABLE_HINTS: dict[str, tuple[str, ...]] = {
    '销售金额': ('order_master',),
    '订单数': ('order_master',),
    '客单价': ('order_master',),
    '销量': ('order_detail', 'order_master'),
    '商品金额': ('order_detail', 'order_master'),
    '退款金额': ('refund_master', 'refund_detail'),
    '退款率': ('order_master', 'refund_master', 'refund_detail', 'order_detail'),
    '退款单数': ('refund_master',),
    '退款件数': ('refund_master', 'refund_detail'),
    '支付买家数': ('order_master',),
    '优惠金额': ('order_master', 'order_detail'),
    '优惠率': ('order_master', 'order_detail'),
    '单均件数': ('order_master', 'order_detail'),
    '用户数': ('user_info',),
    '在库量': ('inventory_stock',),
    '可售库存': ('inventory_stock',),
    '在途库存': ('inventory_stock',),
    '库存金额': ('inventory_stock',),
    '缺货SKU数': ('inventory_stock',),
}

DIMENSION_TABLE_HINTS: dict[str, tuple[str, ...]] = {
    'sales_channel': ('order_master', 'order_detail', 'inventory_stock'),
    'sales_region': ('store_info', 'order_master', 'inventory_stock'),
    'store_province': ('store_info', 'order_master'),
    'platform': ('order_master',),
    'payment_method': ('order_master',),
    'channel_type': ('order_master', 'store_info', 'product_info'),
    'brand_name': ('order_master', 'order_detail', 'product_info', 'inventory_stock'),
    'category_l1': ('order_master', 'order_detail', 'product_info', 'inventory_stock'),
    'member_level': ('order_master', 'user_info', 'refund_master'),
    'city_tier': ('order_master', 'user_info'),
    'register_channel': ('order_master', 'user_info'),
    'promotion_type': ('order_master',),
    'gender': ('order_master', 'user_info'),
    'age': ('order_master', 'user_info'),
    'device_type': ('order_master', 'user_info'),
    'refund_reason': ('refund_master', 'refund_detail'),
    'refund_type': ('refund_master',),
    'warehouse_name': ('inventory_stock',),
    'warehouse_type': ('inventory_stock',),
    'stock_status': ('inventory_stock',),
    'target_group': ('order_master', 'order_detail', 'product_info', 'inventory_stock'),
    'temperature_zone': ('order_master', 'order_detail', 'product_info', 'inventory_stock'),
    'product_name': ('order_master', 'order_detail', 'product_info', 'inventory_stock'),
}

EXPLICIT_CLARIFY_CASES: list[tuple[str, str]] = [
    ('clarify_order_top100', '查询订单排名前100的数据'),
    ('clarify_sales_region_metric', '统计近30天各大区的表现'),
    ('clarify_brand_metric', '统计近30天各品牌的数据'),
    ('clarify_refund_metric', '统计近30天退款情况'),
    ('clarify_inventory_metric', '统计当前库存情况'),
    ('clarify_region_sort', '按省份统计前100'),
    ('clarify_user_metric', '统计近30天用户情况'),
    ('clarify_channel_metric', '统计近30天各渠道表现'),
    ('clarify_product_metric', '统计近30天热销商品'),
    ('clarify_refund_rank', '退款排名前100的数据'),
    ('clarify_inventory_rank', '库存排名前100的数据'),
    ('clarify_brand_top', '蒙牛品牌近30天分析一下'),
]


def _unique_extend(cases: list[EvalCase], candidate: EvalCase) -> None:
    if candidate.case_id in {item.case_id for item in cases}:
        return
    cases.append(candidate)


def _security_level_for_dimensions(dimension_codes: Iterable[str]) -> str:
    dimension_codes = set(dimension_codes)
    if dimension_codes.intersection(SENSITIVE_USER_DIMENSIONS):
        return 'S2'
    if dimension_codes.intersection(INTERNAL_USER_DIMENSIONS):
        return 'S1'
    return 'S0'


def _expected_tables(metric_names: Iterable[str], dimension_codes: Iterable[str], extra_tables: Iterable[str] = ()) -> tuple[str, ...]:
    tables: list[str] = []
    for metric_name in metric_names:
        for table_name in METRIC_TABLE_HINTS.get(metric_name, ()):
            if table_name not in tables:
                tables.append(table_name)
    for dimension_code in dimension_codes:
        for table_name in DIMENSION_TABLE_HINTS.get(dimension_code, ()):
            if table_name not in tables:
                tables.append(table_name)
    for table_name in extra_tables:
        if table_name not in tables:
            tables.append(table_name)
    return tuple(tables)


def _build_cases_from_dimensions(
    *,
    prefix: str,
    dimensions: list[tuple[str, str, tuple[str, ...], str]],
    templates: list[tuple[str, tuple[str, ...], str]],
    extra_tables: tuple[str, ...] = (),
    question_prefix: str = '',
    note_prefix: str = '',
) -> list[EvalCase]:
    cases: list[EvalCase] = []
    for dim_index, (dimension_code, label, base_tables, security_level_hint) in enumerate(dimensions, start=1):
        for tpl_index, (template, metrics, time_granularity) in enumerate(templates, start=1):
            question = f"{question_prefix}{template.format(label=label)}"
            case_id = f'{prefix}_{dim_index:02d}_{tpl_index:02d}'
            tables = _expected_tables(metrics, (dimension_code,), extra_tables=base_tables + extra_tables)
            security_level = security_level_hint or _security_level_for_dimensions((dimension_code,))
            note = f'{note_prefix}{label} / {"、".join(metrics)}'
            cases.append(
                EvalCase(
                    case_id=case_id,
                    question=question,
                    expected_metrics=tuple(metrics),
                    expected_dimensions=(label,),
                    expected_tables=tables,
                    expect_clarify=False,
                    security_level=security_level,
                    domain=prefix,
                    expected_time_granularity=time_granularity,
                    note=note,
                )
            )
    return cases


def _build_brand_filter_cases() -> list[EvalCase]:
    cases: list[EvalCase] = []
    blueprints = [
        ('brand_sales_channel', '统计近30天{brand}各销售渠道的销售金额、退款金额和退款率，按销售金额降序展示。', ('销售金额', '退款金额', '退款率'), ('销售渠道',), ('order_master', 'order_detail', 'refund_master', 'refund_detail')),
        ('brand_region_sales', '统计近30天{brand}各销售大区的销售金额、销量和退款金额，按销售金额降序展示。', ('销售金额', '销量', '退款金额'), ('销售大区',), ('order_master', 'order_detail', 'store_info', 'refund_master', 'refund_detail')),
        ('brand_member_value', '统计近30天{brand}各会员等级的销售金额、订单数和客单价，按销售金额降序展示。', ('销售金额', '订单数', '客单价'), ('会员等级',), ('order_master', 'order_detail', 'user_info')),
        ('brand_province_refund', '统计近30天{brand}各省份的退款金额、退款单数和退款率，按退款金额降序展示。', ('退款金额', '退款单数', '退款率'), ('省份',), ('refund_master', 'refund_detail', 'order_master', 'store_info', 'order_detail')),
        ('brand_category_discount', '统计近30天{brand}各一级品类的销售金额、优惠金额和优惠率，按销售金额降序展示。', ('销售金额', '优惠金额', '优惠率'), ('一级品类',), ('order_master', 'order_detail', 'product_info')),
        ('brand_inventory_region', '按销售大区统计{brand}当前可售库存、在途库存和库存金额，按库存金额降序展示。', ('可售库存', '在途库存', '库存金额'), ('销售大区',), ('inventory_stock', 'store_info', 'product_info')),
    ]
    brands = ['蒙牛', '特仑苏', '纯甄', '真果粒', '未来星']
    for idx, brand in enumerate(brands, start=1):
        for tpl_index, (case_prefix, template, metrics, dimensions, tables) in enumerate(blueprints, start=1):
            security_level = _security_level_for_dimensions({'sales_region' if '销售大区' in dimensions else '', 'member_level' if '会员等级' in dimensions else '', 'store_province' if '省份' in dimensions else '', 'category_l1' if '一级品类' in dimensions else ''})
            if '库存' in template:
                security_level = 'S1'
            question = template.format(brand=brand)
            case_id = f'{case_prefix}_{idx:02d}_{tpl_index:02d}'
            cases.append(
                EvalCase(
                    case_id=case_id,
                    question=question,
                    expected_metrics=metrics,
                    expected_dimensions=dimensions,
                    expected_tables=tables,
                    expect_clarify=False,
                    security_level=security_level,
                    domain='brand_focus',
                    expected_time_granularity='day' if '近30天' in template else 'none',
                    note=f'{brand} / {case_prefix}',
                )
            )
    return cases


def _build_clarify_cases() -> list[EvalCase]:
    cases: list[EvalCase] = []
    for case_id, question in EXPLICIT_CLARIFY_CASES:
        cases.append(
            EvalCase(
                case_id=case_id,
                question=question,
                expected_metrics=(),
                expected_dimensions=(),
                expected_tables=(),
                expect_clarify=True,
                security_level='S0',
                domain='clarify',
                expected_time_granularity='none',
                note='意图缺失或排序口径不明确，应该澄清。',
            )
        )
    return cases


def build_eval_cases() -> list[EvalCase]:
    cases: list[EvalCase] = []
    cases.extend(
        _build_cases_from_dimensions(
            prefix='sales',
            dimensions=SALES_DIMENSIONS,
            templates=SALES_TEMPLATES,
            question_prefix='',
            note_prefix='销售分析 / ',
        )
    )
    cases.extend(
        _build_cases_from_dimensions(
            prefix='refund',
            dimensions=REFUND_DIMENSIONS,
            templates=REFUND_TEMPLATES,
            extra_tables=('order_master',),
            question_prefix='',
            note_prefix='退款分析 / ',
        )
    )
    cases.extend(
        _build_cases_from_dimensions(
            prefix='inventory',
            dimensions=INVENTORY_DIMENSIONS,
            templates=INVENTORY_TEMPLATES,
            extra_tables=(),
            question_prefix='',
            note_prefix='库存分析 / ',
        )
    )
    cases.extend(
        _build_cases_from_dimensions(
            prefix='user_product',
            dimensions=USER_PRODUCT_DIMENSIONS,
            templates=USER_PRODUCT_TEMPLATES,
            extra_tables=('product_info',),
            question_prefix='',
            note_prefix='用户与商品分析 / ',
        )
    )
    cases.extend(_build_brand_filter_cases())
    cases.extend(_build_clarify_cases())
    cases = sorted(cases, key=lambda item: item.case_id)
    if len(cases) < 100:
        raise AssertionError(f'评测用例数量不足 100，当前只有 {len(cases)} 条')
    return cases
