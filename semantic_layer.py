from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections import defaultdict, deque
from functools import lru_cache
from typing import Any

import pymysql
from dotenv import load_dotenv
from openai import OpenAI

from chatbi.config import (
    EMBEDDING_PROVIDER,
    KNOWLEDGE_CONTEXT_TOPN,
    LOCAL_EMBEDDING_BASE_URL,
    LOCAL_EMBEDDING_MODEL,
    SEMANTIC_RERANK_FINAL_N,
    SEMANTIC_RERANK_TOPK,
    TASK_TYPE_SEMANTIC_REBUILD,
)
from chatbi.repository.task_repository import get_latest_task_by_type, get_query_plan_quality_stats
from chatbi.service.data_quality_service import get_latest_data_quality_summary
from chatbi.service.knowledge_service import ensure_knowledge_runtime, invalidate_knowledge_cache, retrieve_knowledge_context
from chatbi.service.rerank_service import rerank_semantic_docs
from chatbi.utils.question_utils import is_context_dependent_question


load_dotenv()


DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "toor"),
    "database": os.getenv("MYSQL_DATABASE", "chatbi"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": False,
}

DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "")
DASHSCOPE_BASE_URL = os.getenv(
    "DASHSCOPE_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"
)
DASHSCOPE_EMBEDDING_MODEL = os.getenv("DASHSCOPE_EMBEDDING_MODEL", "text-embedding-v4")
LOCAL_EMBEDDING_PROVIDER = os.getenv("LOCAL_EMBEDDING_PROVIDER", EMBEDDING_PROVIDER).lower()
SEMANTIC_VECTOR_TOPK = int(os.getenv("SEMANTIC_VECTOR_TOPK", "12"))
SEMANTIC_FULLTEXT_TOPK = int(os.getenv("SEMANTIC_FULLTEXT_TOPK", "12"))
SEMANTIC_RUNTIME_READY = False
COLUMN_REF_PATTERN = re.compile(r"([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)")


PROMPT_FIELD_HINTS: dict[str, list[str]] = {
    "order_master": ["order_id", "buyer_id", "store_id", "created_at", "order_status", "paid_amount", "gross_amount", "discount_amount", "coupon_amount", "promotion_type", "item_count", "sales_channel", "channel_type", "platform", "payment_method", "receiver_province", "receiver_city", "receiver_district"],
    "order_detail": ["order_detail_id", "order_id", "brand_name", "product_id", "product_name", "category_l1", "category_l2", "line_paid_amount", "line_gross_amount", "line_discount_amount", "quantity", "sales_channel"],
    "refund_master": ["refund_id", "order_id", "buyer_id", "store_id", "refund_amount", "refund_item_count", "refund_status", "refund_type", "refund_reason", "applied_at"],
    "refund_detail": ["refund_detail_id", "refund_id", "order_detail_id", "product_id", "refund_amount", "refund_reason", "refund_quantity"],
    "inventory_stock": ["inventory_id", "snapshot_date", "store_id", "product_id", "sales_channel", "warehouse_name", "warehouse_type", "on_hand_qty", "reserved_qty", "available_qty", "in_transit_qty", "safety_stock_qty", "damaged_qty", "inventory_amount", "days_of_supply", "stock_status"],
    "user_info": ["user_id", "member_level", "gender", "age", "province", "province_code", "city", "city_code", "city_tier", "register_channel", "preferred_channel", "customer_tag", "device_type"],
    "store_info": ["store_id", "store_name", "store_type", "sales_region", "channel_name", "channel_type", "province", "province_code", "city", "city_code", "district", "org_level_1"],
    "product_info": ["product_id", "sku_code", "barcode", "brand_name", "product_name", "category_l1", "category_l2", "channel_type", "target_group", "temperature_zone", "list_price", "cost_price", "shelf_life_days"],
}

DIMENSION_VALUE_SOURCES: dict[str, list[tuple[str, str]]] = {
    "sales_channel": [("order_master", "sales_channel"), ("order_detail", "sales_channel"), ("inventory_stock", "sales_channel")],
    "channel_type": [("order_master", "channel_type"), ("store_info", "channel_type"), ("product_info", "channel_type")],
    "platform": [("order_master", "platform")],
    "sales_region": [("store_info", "sales_region")],
    "store_province": [("store_info", "province")],
    "store_city": [("store_info", "city")],
    "store_district": [("store_info", "district")],
    "store_name": [("store_info", "store_name")],
    "store_type": [("store_info", "store_type")],
    "receiver_province": [("order_master", "receiver_province")],
    "receiver_city": [("order_master", "receiver_city")],
    "receiver_district": [("order_master", "receiver_district")],
    "brand_name": [("order_detail", "brand_name"), ("product_info", "brand_name")],
    "product_name": [("order_detail", "product_name"), ("product_info", "product_name")],
    "category_l1": [("order_detail", "category_l1"), ("product_info", "category_l1")],
    "category_l2": [("order_detail", "category_l2"), ("product_info", "category_l2")],
    "payment_method": [("order_master", "payment_method")],
    "promotion_type": [("order_master", "promotion_type")],
    "member_level": [("user_info", "member_level")],
    "gender": [("user_info", "gender")],
    "city_tier": [("user_info", "city_tier")],
    "register_channel": [("user_info", "register_channel")],
    "target_group": [("product_info", "target_group")],
    "temperature_zone": [("product_info", "temperature_zone")],
    "org_level_1": [("store_info", "org_level_1")],
    "inventory_snapshot_date": [("inventory_stock", "snapshot_date")],
    "warehouse_name": [("inventory_stock", "warehouse_name")],
    "warehouse_type": [("inventory_stock", "warehouse_type")],
    "stock_status": [("inventory_stock", "stock_status")],
    "refund_reason": [("refund_master", "refund_reason"), ("refund_detail", "refund_reason")],
    "refund_type": [("refund_master", "refund_type")],
}
MAX_MATCHED_DIMENSION_VALUES = 3


DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS `semantic_domain` (
        `domain_key` VARCHAR(64) NOT NULL COMMENT '业务域编码',
        `domain_name` VARCHAR(128) NOT NULL COMMENT '业务域名称',
        `description` TEXT NULL COMMENT '业务域说明',
        `priority_score` INT NOT NULL DEFAULT 50 COMMENT '优先级分数',
        `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        PRIMARY KEY (`domain_key`),
        KEY `idx_semantic_domain_active` (`is_active`, `priority_score`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语义层业务域表';
    """,
    """
    CREATE TABLE IF NOT EXISTS `semantic_table` (
        `table_name` VARCHAR(64) NOT NULL COMMENT '物理表名',
        `domain_key` VARCHAR(64) NOT NULL COMMENT '所属业务域编码',
        `business_name` VARCHAR(128) NOT NULL COMMENT '业务表名称',
        `table_role` VARCHAR(32) NOT NULL COMMENT '表角色，例如事实表/维度表',
        `description` TEXT NULL COMMENT '业务表说明',
        `table_comment` TEXT NULL COMMENT '数据库表备注',
        `keywords_json` LONGTEXT NULL COMMENT '关键词JSON数组',
        `business_dimensions_json` LONGTEXT NULL COMMENT '常用业务维度JSON数组',
        `business_metrics_json` LONGTEXT NULL COMMENT '常用业务指标JSON数组',
        `priority_score` INT NOT NULL DEFAULT 50 COMMENT '优先级分数',
        `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        PRIMARY KEY (`table_name`),
        KEY `idx_semantic_table_active` (`is_active`, `priority_score`),
        KEY `idx_semantic_table_domain` (`domain_key`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语义层业务表定义';
    """,
    """
    CREATE TABLE IF NOT EXISTS `semantic_column` (
        `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
        `table_name` VARCHAR(64) NOT NULL COMMENT '物理表名',
        `column_name` VARCHAR(64) NOT NULL COMMENT '物理字段名',
        `business_name` VARCHAR(128) NULL COMMENT '业务字段名称',
        `column_comment` TEXT NULL COMMENT '字段备注',
        `data_type` VARCHAR(64) NOT NULL COMMENT '字段类型',
        `ordinal_position` INT NOT NULL COMMENT '字段顺序',
        `is_time_dimension` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否时间字段',
        `is_dimension_candidate` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否维度候选字段',
        `is_metric_candidate` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否指标候选字段',
        `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_semantic_column_table_col` (`table_name`, `column_name`),
        KEY `idx_semantic_column_table` (`table_name`, `ordinal_position`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语义层字段定义';
    """,
    """
    CREATE TABLE IF NOT EXISTS `semantic_metric` (
        `metric_code` VARCHAR(64) NOT NULL COMMENT '指标编码',
        `metric_name` VARCHAR(128) NOT NULL COMMENT '指标名称',
        `domain_key` VARCHAR(64) NOT NULL COMMENT '所属业务域编码',
        `definition_name` VARCHAR(128) NULL COMMENT '指标定义名称',
        `description` TEXT NULL COMMENT '指标口径描述',
        `default_expression` TEXT NULL COMMENT '默认SQL表达式',
        `default_filters` TEXT NULL COMMENT '默认过滤条件描述',
        `related_tables_json` LONGTEXT NULL COMMENT '相关表JSON数组',
        `keywords_json` LONGTEXT NULL COMMENT '关键词JSON数组',
        `priority_score` INT NOT NULL DEFAULT 50 COMMENT '优先级分数',
        `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        PRIMARY KEY (`metric_code`),
        KEY `idx_semantic_metric_active` (`is_active`, `priority_score`),
        KEY `idx_semantic_metric_domain` (`domain_key`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语义层指标定义';
    """,
    """
    CREATE TABLE IF NOT EXISTS `semantic_dimension` (
        `dimension_code` VARCHAR(64) NOT NULL COMMENT '维度编码',
        `dimension_name` VARCHAR(128) NOT NULL COMMENT '维度名称',
        `domain_key` VARCHAR(64) NOT NULL COMMENT '所属业务域编码',
        `description` TEXT NULL COMMENT '维度说明',
        `source_expression` TEXT NULL COMMENT '默认字段或表达式',
        `related_tables_json` LONGTEXT NULL COMMENT '相关表JSON数组',
        `keywords_json` LONGTEXT NULL COMMENT '关键词JSON数组',
        `priority_score` INT NOT NULL DEFAULT 50 COMMENT '优先级分数',
        `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        PRIMARY KEY (`dimension_code`),
        KEY `idx_semantic_dimension_active` (`is_active`, `priority_score`),
        KEY `idx_semantic_dimension_domain` (`domain_key`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语义层维度定义';
    """,
    """
    CREATE TABLE IF NOT EXISTS `semantic_join` (
        `join_code` VARCHAR(64) NOT NULL COMMENT '关联编码',
        `domain_key` VARCHAR(64) NOT NULL COMMENT '所属业务域编码',
        `left_table` VARCHAR(64) NOT NULL COMMENT '左表',
        `right_table` VARCHAR(64) NOT NULL COMMENT '右表',
        `join_type` VARCHAR(32) NOT NULL DEFAULT 'INNER JOIN' COMMENT '关联类型',
        `join_condition` TEXT NOT NULL COMMENT '关联条件',
        `description` TEXT NULL COMMENT '关联说明',
        `keywords_json` LONGTEXT NULL COMMENT '关键词JSON数组',
        `priority_score` INT NOT NULL DEFAULT 50 COMMENT '优先级分数',
        `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        PRIMARY KEY (`join_code`),
        KEY `idx_semantic_join_active` (`is_active`, `priority_score`),
        KEY `idx_semantic_join_tables` (`left_table`, `right_table`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语义层表关联关系';
    """,
    """
    CREATE TABLE IF NOT EXISTS `semantic_synonym` (
        `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
        `target_type` VARCHAR(32) NOT NULL COMMENT '目标对象类型',
        `target_key` VARCHAR(64) NOT NULL COMMENT '目标对象编码',
        `standard_name` VARCHAR(128) NOT NULL COMMENT '标准名称',
        `synonym_term` VARCHAR(128) NOT NULL COMMENT '同义词',
        `related_tables_json` LONGTEXT NULL COMMENT '相关表JSON数组',
        `weight_score` INT NOT NULL DEFAULT 10 COMMENT '规则召回权重',
        `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_semantic_synonym_unique` (`target_type`, `target_key`, `synonym_term`),
        KEY `idx_semantic_synonym_target` (`target_type`, `target_key`),
        KEY `idx_semantic_synonym_active` (`is_active`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语义层同义词定义';
    """,
    """
    CREATE TABLE IF NOT EXISTS `semantic_example` (
        `example_key` VARCHAR(64) NOT NULL COMMENT '示例编码',
        `domain_key` VARCHAR(64) NOT NULL COMMENT '所属业务域编码',
        `question_text` TEXT NOT NULL COMMENT '示例问法',
        `summary_text` TEXT NULL COMMENT '示例说明',
        `related_tables_json` LONGTEXT NULL COMMENT '相关表JSON数组',
        `related_metrics_json` LONGTEXT NULL COMMENT '相关指标JSON数组',
        `related_dimensions_json` LONGTEXT NULL COMMENT '相关维度JSON数组',
        `sql_example` LONGTEXT NULL COMMENT '示例SQL',
        `priority_score` INT NOT NULL DEFAULT 50 COMMENT '优先级分数',
        `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        PRIMARY KEY (`example_key`),
        KEY `idx_semantic_example_active` (`is_active`, `priority_score`),
        KEY `idx_semantic_example_domain` (`domain_key`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语义层问法示例';
    """,
    """
    CREATE TABLE IF NOT EXISTS `semantic_search_doc` (
        `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
        `source_type` VARCHAR(32) NOT NULL COMMENT '来源对象类型',
        `source_key` VARCHAR(64) NOT NULL COMMENT '来源对象编码',
        `source_name` VARCHAR(255) NOT NULL COMMENT '来源对象名称',
        `domain_key` VARCHAR(64) NULL COMMENT '所属业务域编码',
        `related_tables_json` LONGTEXT NULL COMMENT '相关表JSON数组',
        `related_metrics_json` LONGTEXT NULL COMMENT '相关指标JSON数组',
        `related_dimensions_json` LONGTEXT NULL COMMENT '相关维度JSON数组',
        `priority_score` INT NOT NULL DEFAULT 50 COMMENT '优先级分数',
        `search_text` LONGTEXT NOT NULL COMMENT '全文检索文本',
        `payload_json` LONGTEXT NULL COMMENT '检索对象载荷JSON',
        `content_hash` CHAR(32) NOT NULL COMMENT '内容哈希',
        `embedding_json` LONGTEXT NULL COMMENT '向量JSON',
        `embedding_model` VARCHAR(64) NULL COMMENT '向量模型',
        `embedding_status` VARCHAR(32) NOT NULL DEFAULT 'pending' COMMENT '向量状态',
        `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_semantic_search_source` (`source_type`, `source_key`),
        KEY `idx_semantic_search_active` (`is_active`, `source_type`, `priority_score`),
        KEY `idx_semantic_search_domain` (`domain_key`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语义层检索文档表';
    """,
]


DEFAULT_DOMAINS = [
    {"domain_key": "transaction", "domain_name": "交易域", "description": "订单、销售、支付和履约分析", "priority_score": 100, "is_active": 1},
    {"domain_key": "user", "domain_name": "用户域", "description": "用户属性、会员分层和人群分析", "priority_score": 85, "is_active": 1},
    {"domain_key": "product", "domain_name": "产品域", "description": "产品、品牌、品类和SKU分析", "priority_score": 85, "is_active": 1},
    {"domain_key": "store", "domain_name": "门店域", "description": "门店、渠道、大区和组织分析", "priority_score": 85, "is_active": 1},
    {"domain_key": "refund", "domain_name": "售后域", "description": "退款、退货和售后分析", "priority_score": 80, "is_active": 1},
    {"domain_key": "inventory", "domain_name": "库存域", "description": "库存、缺货、在途和库存金额分析", "priority_score": 82, "is_active": 1},
]

DEFAULT_TABLES = [
    {
        "table_name": "order_master",
        "domain_key": "transaction",
        "business_name": "订单主表",
        "table_role": "事实表",
        "description": "记录订单主单级别的销售、支付、履约和收货信息，是订单金额、订单数、销售地区分析的主事实表。",
        "keywords": ["订单", "销售", "销售额", "销售金额", "GMV", "实付", "支付", "下单", "履约", "收货", "订单数", "客单价", "支付买家数", "优惠金额", "优惠率", "单均件数", "平台", "渠道", "支付方式", "排名", "top", "前100", "地区销售"],
        "business_dimensions": ["销售渠道", "渠道类型", "平台", "订单状态", "支付方式", "收货省份", "收货城市", "下单日期", "完成日期"],
        "business_metrics": ["销售金额", "订单数", "退款金额", "客单价", "支付买家数", "优惠金额", "优惠率", "单均件数"],
        "priority_score": 95,
        "is_active": 1,
    },
    {
        "table_name": "order_detail",
        "domain_key": "transaction",
        "business_name": "订单明细子表",
        "table_role": "事实表",
        "description": "记录订单行级商品明细，是商品销量、品牌销售、品类分析和SKU分析的核心事实表。",
        "keywords": ["商品", "产品", "sku", "明细", "品牌", "品类", "销量", "销售件数", "商品金额", "平均成交单价", "单品", "产品排名", "产品销售", "优惠金额", "优惠率", "温层", "目标人群"],
        "business_dimensions": ["产品名称", "品牌", "一级品类", "二级品类", "销售渠道", "目标人群", "温层"],
        "business_metrics": ["销量", "商品金额", "销售金额", "件数", "平均成交单价", "优惠金额", "优惠率"],
        "priority_score": 92,
        "is_active": 1,
    },
    {
        "table_name": "user_info",
        "domain_key": "user",
        "business_name": "用户信息维度表",
        "table_role": "维度表",
        "description": "提供用户属性和会员分层，用于性别、年龄、城市、注册渠道、会员等级等用户维度分析。",
        "keywords": ["用户", "会员", "人群", "性别", "年龄", "注册", "注册渠道", "标签", "城市等级", "母婴", "职业", "积分", "支付买家", "复购用户", "设备类型", "偏好渠道"],
        "business_dimensions": ["性别", "年龄", "常住省份", "常住城市", "城市等级", "会员等级", "注册渠道", "用户标签", "是否母婴人群", "设备类型"],
        "business_metrics": ["用户数", "支付买家数", "会员销售金额", "新客销售金额"],
        "priority_score": 88,
        "is_active": 1,
    },
    {
        "table_name": "product_info",
        "domain_key": "product",
        "business_name": "产品信息维度表",
        "table_role": "维度表",
        "description": "维护蒙牛产品、品牌、品类、规格和定价信息，用于品牌、SPU、SKU和品类分析。",
        "keywords": ["产品", "商品", "sku", "spu", "品牌", "品类", "规格", "定价", "价格", "特仑苏", "纯甄", "真果粒", "未来星", "冠益乳", "每日鲜语", "蒂兰圣雪", "蒙牛"],
        "business_dimensions": ["品牌", "产品名称", "一级品类", "二级品类", "规格", "温层类型", "目标人群"],
        "business_metrics": ["建议零售价", "成本单价", "销量", "品牌销售金额"],
        "priority_score": 90,
        "is_active": 1,
    },
    {
        "table_name": "store_info",
        "domain_key": "store",
        "business_name": "门店信息维度表",
        "table_role": "维度表",
        "description": "提供门店、渠道、销售大区和组织架构信息，用于门店和区域经营分析。",
        "keywords": ["门店", "店铺", "大区", "销售大区", "组织", "区域", "华东", "华南", "华北", "华中", "西南", "西北", "省份", "城市", "河南", "江苏", "浙江", "广东", "湖北", "山东", "四川", "陕西", "北京", "上海", "渠道", "渠道类型", "抖音", "京东", "天猫", "小程序", "线下", "社区团购", "O2O"],
        "business_dimensions": ["门店名称", "门店类型", "渠道名称", "渠道类型", "销售大区", "省份", "城市", "一级组织", "二级组织"],
        "business_metrics": ["门店销售金额", "门店订单数", "大区销售金额", "退款金额"],
        "priority_score": 91,
        "is_active": 1,
    },
    {
        "table_name": "inventory_stock",
        "domain_key": "inventory",
        "business_name": "库存快照表",
        "table_role": "事实表",
        "description": "记录门店或渠道维度下商品的库存快照，可用于在库量、可售库存、在途库存、安全库存、缺货SKU数和库存金额分析。",
        "keywords": ["库存", "可售库存", "可用库存", "在库", "库存量", "在途库存", "安全库存", "缺货", "库存金额", "货值", "库存状态", "库存预警", "库存天数", "周转天数", "仓库"],
        "business_dimensions": ["销售渠道", "销售大区", "省份", "仓库名称", "仓库类型", "库存状态", "品牌", "产品名称", "一级品类", "快照日期"],
        "business_metrics": ["在库量", "可售库存", "在途库存", "库存金额", "缺货SKU数"],
        "priority_score": 93,
        "is_active": 1,
    },
    {
        "table_name": "refund_master",
        "domain_key": "refund",
        "business_name": "退款主表",
        "table_role": "事实表",
        "description": "记录退款申请、退款金额、退款状态和退款原因，用于售后和退款口径分析。",
        "keywords": ["退款", "退货", "售后", "退款金额", "退款单", "退款率", "售后金额", "售后单数", "退款原因", "退款件数", "退款类型"],
        "business_dimensions": ["退款状态", "退款类型", "退款原因", "退款申请日期"],
        "business_metrics": ["退款金额", "退款单数", "退款件数", "退款率"],
        "priority_score": 89,
        "is_active": 1,
    },
    {
        "table_name": "refund_detail",
        "domain_key": "refund",
        "business_name": "退款明细子表",
        "table_role": "事实表",
        "description": "记录退款商品明细，是退款产品、退款品牌和退款SKU分析的核心事实表。",
        "keywords": ["退款商品", "退款产品", "退款品牌", "退款sku", "售后商品", "售后品牌", "退款明细"],
        "business_dimensions": ["退款产品名称", "退款品牌", "退款原因"],
        "business_metrics": ["退款商品金额", "退款件数"],
        "priority_score": 84,
        "is_active": 1,
    },
]

DEFAULT_METRICS = [
    {
        "metric_code": "sales_amount",
        "metric_name": "销售金额",
        "domain_key": "transaction",
        "definition_name": "销售金额",
        "description": "订单级销售金额默认取订单主表 paid_amount 的汇总；如果按品牌、产品、品类、SKU 分析，则必须改用订单明细表 line_paid_amount，避免订单主表金额被重复放大。",
        "default_expression": "SUM(order_master.paid_amount) 或 SUM(order_detail.line_paid_amount)",
        "default_filters": "若用户未限定订单状态，默认统计 已支付、已发货、已完成、部分退款。",
        "related_tables": ["order_master", "order_detail"],
        "keywords": ["销售金额", "销售额", "GMV", "订单金额", "实付", "成交额"],
        "priority_score": 100,
        "is_active": 1,
    },
    {
        "metric_code": "order_count",
        "metric_name": "订单数",
        "domain_key": "transaction",
        "definition_name": "订单数",
        "description": "默认取 COUNT(DISTINCT order_master.order_id)。",
        "default_expression": "COUNT(DISTINCT order_master.order_id)",
        "default_filters": "若用户未限定订单状态，默认统计 已支付、已发货、已完成、部分退款。",
        "related_tables": ["order_master"],
        "keywords": ["订单数", "订单量", "单量"],
        "priority_score": 95,
        "is_active": 1,
    },
    {
        "metric_code": "avg_order_value",
        "metric_name": "客单价",
        "domain_key": "transaction",
        "definition_name": "客单价",
        "description": "默认取销售金额 / 订单数。",
        "default_expression": "SUM(order_master.paid_amount) / COUNT(DISTINCT order_master.order_id)",
        "default_filters": "通常与销售金额、订单数口径保持一致。",
        "related_tables": ["order_master"],
        "keywords": ["客单价", "平均订单金额"],
        "priority_score": 78,
        "is_active": 1,
    },
    {
        "metric_code": "sales_volume",
        "metric_name": "销量",
        "domain_key": "product",
        "definition_name": "销量",
        "description": "默认取订单明细表 quantity 的汇总。",
        "default_expression": "SUM(order_detail.quantity)",
        "default_filters": "若用户未限定订单状态，默认统计 已支付、已发货、已完成、部分退款。",
        "related_tables": ["order_detail", "order_master"],
        "keywords": ["销量", "件数", "销售件数", "商品件数"],
        "priority_score": 92,
        "is_active": 1,
    },
    {
        "metric_code": "gross_merchandise_amount",
        "metric_name": "商品金额",
        "domain_key": "product",
        "definition_name": "商品金额",
        "description": "默认取订单明细表 line_gross_amount 的汇总。",
        "default_expression": "SUM(order_detail.line_gross_amount)",
        "default_filters": "适用于商品原价口径分析。",
        "related_tables": ["order_detail", "order_master"],
        "keywords": ["商品金额", "原价金额", "行金额"],
        "priority_score": 80,
        "is_active": 1,
    },
    {
        "metric_code": "refund_amount",
        "metric_name": "退款金额",
        "domain_key": "refund",
        "definition_name": "退款金额",
        "description": "订单级退款金额可汇总 refund_master.refund_amount；只要涉及品牌、产品、品类、SKU 等商品粒度分析或这些条件过滤，必须改用 refund_detail.refund_amount，并通过 order_detail.order_detail_id 关联，避免订单级退款金额被重复分摊。",
        "default_expression": "SUM(refund_master.refund_amount) 或 SUM(refund_detail.refund_amount)",
        "default_filters": "未特别指定时默认统计退款成功和退款处理中记录。",
        "related_tables": ["refund_master", "refund_detail"],
        "keywords": ["退款金额", "退款额", "售后金额"],
        "priority_score": 94,
        "is_active": 1,
    },
    {
        "metric_code": "refund_rate",
        "metric_name": "退款率",
        "domain_key": "refund",
        "definition_name": "退款率",
        "description": "默认取退款金额 / 销售金额；订单级使用 refund_master.refund_amount 与 order_master.paid_amount，品牌、产品、品类、SKU 等商品粒度或相关过滤场景必须使用 refund_detail.refund_amount 与 order_detail.line_paid_amount，保持同一分析粒度。",
        "default_expression": "SUM(refund_master.refund_amount) / NULLIF(SUM(order_master.paid_amount) 或 SUM(order_detail.line_paid_amount), 0)",
        "default_filters": "退款率需要与销售金额保持同一时间范围、同一筛选条件和同一分析粒度。",
        "related_tables": ["order_master", "order_detail", "refund_master", "refund_detail"],
        "keywords": ["退款率", "退货率", "售后率"],
        "priority_score": 90,
        "is_active": 1,
    },
    {
        "metric_code": "refund_count",
        "metric_name": "退款单数",
        "domain_key": "refund",
        "definition_name": "退款单数",
        "description": "默认取 COUNT(DISTINCT refund_master.refund_id)。",
        "default_expression": "COUNT(DISTINCT refund_master.refund_id)",
        "default_filters": "默认统计退款申请单。",
        "related_tables": ["refund_master"],
        "keywords": ["退款单数", "退款数", "退款订单数", "退货订单数", "退货量", "售后单数"],
        "priority_score": 86,
        "is_active": 1,
    },
    {
        "metric_code": "refund_item_count",
        "metric_name": "退款件数",
        "domain_key": "refund",
        "definition_name": "退款件数",
        "description": "默认取退款主表 refund_item_count 的汇总；若按商品粒度分析，则优先使用 refund_detail.refund_quantity。",
        "default_expression": "SUM(refund_master.refund_item_count) 或 SUM(refund_detail.refund_quantity)",
        "default_filters": "退款件数需要与退款金额保持同一时间范围和同一筛选条件。",
        "related_tables": ["refund_master", "refund_detail"],
        "keywords": ["退款件数", "退款数量", "退货件数", "售后件数"],
        "priority_score": 84,
        "is_active": 1,
    },
    {
        "metric_code": "pay_buyer_count",
        "metric_name": "支付买家数",
        "domain_key": "user",
        "definition_name": "支付买家数",
        "description": "默认取订单主表中完成支付或履约订单的去重 buyer_id 数量。",
        "default_expression": "COUNT(DISTINCT order_master.buyer_id)",
        "default_filters": "若用户未限定订单状态，默认统计 已支付、已发货、已完成、部分退款。",
        "related_tables": ["order_master"],
        "keywords": ["支付买家数", "支付买家", "支付的买家", "用户量", "下单用户数", "成交用户数", "支付用户数"],
        "priority_score": 87,
        "is_active": 1,
    },
    {
        "metric_code": "discount_amount",
        "metric_name": "优惠金额",
        "domain_key": "transaction",
        "definition_name": "优惠金额",
        "description": "订单级默认取订单主表 discount_amount 汇总；若按商品粒度分析则优先使用订单明细行优惠金额。",
        "default_expression": "SUM(order_master.discount_amount) 或 SUM(order_detail.line_discount_amount)",
        "default_filters": "通常与销售金额保持同一时间范围和同一筛选条件。",
        "related_tables": ["order_master", "order_detail"],
        "keywords": ["优惠金额", "折扣金额", "立减金额", "促销优惠"],
        "priority_score": 83,
        "is_active": 1,
    },
    {
        "metric_code": "discount_rate",
        "metric_name": "优惠率",
        "domain_key": "transaction",
        "definition_name": "优惠率",
        "description": "订单级默认取 discount_amount / gross_amount；若按品牌、产品、品类、SKU 等商品粒度分析，则改用订单明细行优惠金额 / 行原价金额。",
        "default_expression": "SUM(order_master.discount_amount) / NULLIF(SUM(order_master.gross_amount), 0) 或 SUM(order_detail.line_discount_amount) / NULLIF(SUM(order_detail.line_gross_amount), 0)",
        "default_filters": "优惠率需要与销售金额保持同一时间范围和同一筛选条件。",
        "related_tables": ["order_master", "order_detail"],
        "keywords": ["优惠率", "折扣率", "折扣占比", "让利率"],
        "priority_score": 81,
        "is_active": 1,
    },
    {
        "metric_code": "avg_selling_price",
        "metric_name": "平均成交单价",
        "domain_key": "product",
        "definition_name": "平均成交单价",
        "description": "默认取订单明细实付金额 / 销量，用于衡量商品真实成交单价。",
        "default_expression": "SUM(order_detail.line_paid_amount) / NULLIF(SUM(order_detail.quantity), 0)",
        "default_filters": "通常与销量、销售金额保持同一时间范围和同一筛选条件。",
        "related_tables": ["order_detail", "order_master"],
        "keywords": ["平均成交单价", "平均售价", "平均销售单价", "ASP"],
        "priority_score": 79,
        "is_active": 1,
    },
    {
        "metric_code": "items_per_order",
        "metric_name": "单均件数",
        "domain_key": "transaction",
        "definition_name": "单均件数",
        "description": "默认取商品件数 / 订单数，用于衡量平均每单购买件数。",
        "default_expression": "SUM(order_master.item_count) / COUNT(DISTINCT order_master.order_id) 或 SUM(order_detail.quantity) / COUNT(DISTINCT order_detail.order_id)",
        "default_filters": "通常与订单数、销量保持同一时间范围和同一筛选条件。",
        "related_tables": ["order_master", "order_detail"],
        "keywords": ["单均件数", "件单量", "平均每单件数", "平均件数"],
        "priority_score": 77,
        "is_active": 1,
    },
    {
        "metric_code": "user_count",
        "metric_name": "用户数",
        "domain_key": "user",
        "definition_name": "用户数",
        "description": "默认取 COUNT(DISTINCT user_info.user_id)。",
        "default_expression": "COUNT(DISTINCT user_info.user_id)",
        "default_filters": "如果与订单联查，需明确是下单用户还是全部用户。",
        "related_tables": ["user_info", "order_master"],
        "keywords": ["用户数", "会员数", "买家数", "人数"],
        "priority_score": 85,
        "is_active": 1,
    },
    {
        "metric_code": "on_hand_inventory",
        "metric_name": "在库量",
        "domain_key": "inventory",
        "definition_name": "在库量",
        "description": "默认取库存快照表 on_hand_qty 的汇总，用于衡量当前账面库存总量。",
        "default_expression": "SUM(inventory_stock.on_hand_qty)",
        "default_filters": "默认取最新库存快照，可与品牌、产品、门店、大区、渠道等维度联动分析。",
        "related_tables": ["inventory_stock"],
        "keywords": ["在库量", "库存量", "当前库存", "库存总量"],
        "priority_score": 88,
        "is_active": 1,
    },
    {
        "metric_code": "available_inventory",
        "metric_name": "可售库存",
        "domain_key": "inventory",
        "definition_name": "可售库存",
        "description": "默认取库存快照表 available_qty 的汇总，用于衡量当前可销售库存。",
        "default_expression": "SUM(inventory_stock.available_qty)",
        "default_filters": "默认取最新库存快照，是库存预警、缺货分析和补货分析的核心口径。",
        "related_tables": ["inventory_stock"],
        "keywords": ["可售库存", "可用库存", "可卖库存", "可售量"],
        "priority_score": 94,
        "is_active": 1,
    },
    {
        "metric_code": "in_transit_inventory",
        "metric_name": "在途库存",
        "domain_key": "inventory",
        "definition_name": "在途库存",
        "description": "默认取库存快照表 in_transit_qty 的汇总，用于衡量已经在途但尚未入仓或上架的库存量。",
        "default_expression": "SUM(inventory_stock.in_transit_qty)",
        "default_filters": "默认取最新库存快照，可结合缺货与预警状态分析补货节奏。",
        "related_tables": ["inventory_stock"],
        "keywords": ["在途库存", "运输中库存", "途中新货"],
        "priority_score": 83,
        "is_active": 1,
    },
    {
        "metric_code": "inventory_amount",
        "metric_name": "库存金额",
        "domain_key": "inventory",
        "definition_name": "库存金额",
        "description": "默认取库存快照表 inventory_amount 的汇总，按成本口径衡量库存货值。",
        "default_expression": "SUM(inventory_stock.inventory_amount)",
        "default_filters": "默认取最新库存快照，可用于库存资金占用和滞销库存分析。",
        "related_tables": ["inventory_stock"],
        "keywords": ["库存金额", "库存货值", "库存成本", "货值"],
        "priority_score": 85,
        "is_active": 1,
    },
    {
        "metric_code": "stockout_sku_count",
        "metric_name": "缺货SKU数",
        "domain_key": "inventory",
        "definition_name": "缺货SKU数",
        "description": "默认取可售库存小于等于0的去重 product_id 数量，用于识别缺货风险。",
        "default_expression": "COUNT(DISTINCT CASE WHEN inventory_stock.available_qty <= 0 THEN inventory_stock.product_id END)",
        "default_filters": "默认取最新库存快照，可按渠道、品牌、大区、门店等维度拆分。",
        "related_tables": ["inventory_stock"],
        "keywords": ["缺货SKU数", "缺货商品数", "缺货数", "断货SKU数"],
        "priority_score": 82,
        "is_active": 1,
    },
]

DEFAULT_DIMENSIONS = [
    {"dimension_code": "sales_channel", "dimension_name": "销售渠道", "domain_key": "transaction", "description": "订单或库存所属销售渠道，例如线下门店、天猫、京东、抖音。", "source_expression": "order_master.sales_channel 或 inventory_stock.sales_channel", "related_tables": ["order_master", "order_detail", "inventory_stock"], "keywords": ["销售渠道", "渠道"], "priority_score": 90, "is_active": 1},
    {"dimension_code": "channel_type", "dimension_name": "渠道类型", "domain_key": "transaction", "description": "渠道所属类型，例如传统电商、兴趣电商、私域直营。", "source_expression": "order_master.channel_type 或 store_info.channel_type", "related_tables": ["order_master", "store_info"], "keywords": ["渠道类型"], "priority_score": 82, "is_active": 1},
    {"dimension_code": "platform", "dimension_name": "平台", "domain_key": "transaction", "description": "订单来源平台，例如天猫、京东、抖音、小程序。", "source_expression": "order_master.platform", "related_tables": ["order_master"], "keywords": ["平台", "来源平台"], "priority_score": 84, "is_active": 1},
    {"dimension_code": "payment_method", "dimension_name": "支付方式", "domain_key": "transaction", "description": "订单支付方式，例如微信支付、支付宝、银行卡。", "source_expression": "order_master.payment_method", "related_tables": ["order_master"], "keywords": ["支付方式", "微信支付", "支付宝", "银行卡"], "priority_score": 82, "is_active": 1},
    {"dimension_code": "sales_region", "dimension_name": "销售大区", "domain_key": "store", "description": "门店所在销售大区，例如华东大区、华南大区。", "source_expression": "store_info.sales_region", "related_tables": ["store_info", "order_master", "inventory_stock"], "keywords": ["销售大区", "大区", "华东", "华南", "华北", "华中", "西南", "西北"], "priority_score": 95, "is_active": 1},
    {"dimension_code": "store_province", "dimension_name": "省份", "domain_key": "store", "description": "门店或经营区域所在省份。若问题同时出现大区与省份，优先使用该维度。", "source_expression": "store_info.province", "related_tables": ["store_info", "order_master", "inventory_stock"], "keywords": ["省份", "所在省份", "门店省份", "省区"], "priority_score": 93, "is_active": 1},
    {"dimension_code": "store_city", "dimension_name": "城市", "domain_key": "store", "description": "门店或经营区域所在城市。", "source_expression": "store_info.city", "related_tables": ["store_info", "order_master", "inventory_stock"], "keywords": ["城市", "所在城市", "门店城市"], "priority_score": 84, "is_active": 1},
    {"dimension_code": "store_district", "dimension_name": "门店区县", "domain_key": "store", "description": "门店所在区县或经营片区。", "source_expression": "store_info.district", "related_tables": ["store_info", "order_master", "inventory_stock"], "keywords": ["门店区县", "区县", "片区"], "priority_score": 78, "is_active": 1},
    {"dimension_code": "store_name", "dimension_name": "门店名称", "domain_key": "store", "description": "门店或店铺名称。", "source_expression": "store_info.store_name", "related_tables": ["store_info", "order_master", "inventory_stock"], "keywords": ["门店", "店铺", "门店名称"], "priority_score": 88, "is_active": 1},
    {"dimension_code": "store_type", "dimension_name": "门店类型", "domain_key": "store", "description": "门店经营类型，例如直营门店、经销门店、社区前置仓。", "source_expression": "store_info.store_type", "related_tables": ["store_info", "order_master", "inventory_stock"], "keywords": ["门店类型", "店型", "直营门店", "经销门店"], "priority_score": 83, "is_active": 1},
    {"dimension_code": "org_level_1", "dimension_name": "一级组织", "domain_key": "store", "description": "门店所属一级组织，用于区域组织经营分析。", "source_expression": "store_info.org_level_1", "related_tables": ["store_info", "order_master", "inventory_stock"], "keywords": ["一级组织", "组织", "销售中心"], "priority_score": 79, "is_active": 1},
    {"dimension_code": "inventory_snapshot_date", "dimension_name": "快照日期", "domain_key": "inventory", "description": "库存快照日期。", "source_expression": "inventory_stock.snapshot_date", "related_tables": ["inventory_stock"], "keywords": ["快照日期", "库存日期", "库存快照"], "priority_score": 80, "is_active": 1},
    {"dimension_code": "warehouse_name", "dimension_name": "仓库名称", "domain_key": "inventory", "description": "库存所在仓库名称。", "source_expression": "inventory_stock.warehouse_name", "related_tables": ["inventory_stock"], "keywords": ["仓库", "仓库名称"], "priority_score": 82, "is_active": 1},
    {"dimension_code": "warehouse_type", "dimension_name": "仓库类型", "domain_key": "inventory", "description": "库存所在仓库类型，例如电商仓、门店仓、前置仓、冷冻仓。", "source_expression": "inventory_stock.warehouse_type", "related_tables": ["inventory_stock"], "keywords": ["仓库类型", "电商仓", "门店仓", "前置仓", "冷冻仓"], "priority_score": 79, "is_active": 1},
    {"dimension_code": "stock_status", "dimension_name": "库存状态", "domain_key": "inventory", "description": "库存状态，例如正常、预警、缺货、滞销。", "source_expression": "inventory_stock.stock_status", "related_tables": ["inventory_stock"], "keywords": ["库存状态", "预警", "缺货", "滞销"], "priority_score": 84, "is_active": 1},
    {"dimension_code": "receiver_province", "dimension_name": "收货省份", "domain_key": "transaction", "description": "订单收货地址中的省份。", "source_expression": "order_master.receiver_province", "related_tables": ["order_master"], "keywords": ["省份", "收货省份", "地区", "河南", "江苏", "浙江", "广东"], "priority_score": 90, "is_active": 1},
    {"dimension_code": "receiver_city", "dimension_name": "收货城市", "domain_key": "transaction", "description": "订单收货地址中的城市。", "source_expression": "order_master.receiver_city", "related_tables": ["order_master"], "keywords": ["城市", "收货城市"], "priority_score": 82, "is_active": 1},
    {"dimension_code": "receiver_district", "dimension_name": "收货区县", "domain_key": "transaction", "description": "订单收货地址中的区县。", "source_expression": "order_master.receiver_district", "related_tables": ["order_master"], "keywords": ["收货区县", "收货区域"], "priority_score": 75, "is_active": 1},
    {"dimension_code": "order_status", "dimension_name": "订单状态", "domain_key": "transaction", "description": "订单当前状态，仅可用中文状态值。", "source_expression": "order_master.order_status", "related_tables": ["order_master"], "keywords": ["订单状态", "已支付", "已完成", "已退款", "部分退款"], "priority_score": 84, "is_active": 1},
    {"dimension_code": "promotion_type", "dimension_name": "促销类型", "domain_key": "transaction", "description": "订单成交时生效的主要促销类型，例如满减、会员价、直播补贴。", "source_expression": "order_master.promotion_type", "related_tables": ["order_master"], "keywords": ["促销类型", "活动类型", "满减", "会员价", "直播补贴"], "priority_score": 80, "is_active": 1},
    {"dimension_code": "order_date", "dimension_name": "下单日期", "domain_key": "transaction", "description": "订单创建日期。", "source_expression": "DATE(order_master.created_at)", "related_tables": ["order_master"], "keywords": ["下单日期", "按天", "按日", "时间", "日期"], "priority_score": 87, "is_active": 1},
    {"dimension_code": "order_week", "dimension_name": "下单周", "domain_key": "transaction", "description": "订单创建所属周。", "source_expression": "YEARWEEK(order_master.created_at, 1)", "related_tables": ["order_master"], "keywords": ["按周", "周"], "priority_score": 76, "is_active": 1},
    {"dimension_code": "order_month", "dimension_name": "下单月", "domain_key": "transaction", "description": "订单创建所属月份。", "source_expression": "DATE_FORMAT(order_master.created_at, '%Y-%m')", "related_tables": ["order_master"], "keywords": ["按月", "月"], "priority_score": 78, "is_active": 1},
    {"dimension_code": "brand_name", "dimension_name": "品牌", "domain_key": "product", "description": "产品品牌名称。", "source_expression": "order_detail.brand_name 或 product_info.brand_name", "related_tables": ["order_detail", "product_info", "order_master", "inventory_stock"], "keywords": ["品牌", "特仑苏", "纯甄", "真果粒", "未来星", "冠益乳", "每日鲜语", "蒂兰圣雪", "蒙牛"], "priority_score": 96, "is_active": 1},
    {"dimension_code": "product_name", "dimension_name": "产品名称", "domain_key": "product", "description": "订单明细或产品维表中的产品名称。", "source_expression": "order_detail.product_name 或 product_info.product_name", "related_tables": ["order_detail", "product_info", "order_master", "inventory_stock"], "keywords": ["产品名称", "商品名称", "sku", "单品"], "priority_score": 89, "is_active": 1},
    {"dimension_code": "category_l1", "dimension_name": "一级品类", "domain_key": "product", "description": "产品一级品类。", "source_expression": "order_detail.category_l1 或 product_info.category_l1", "related_tables": ["order_detail", "product_info", "order_master", "inventory_stock"], "keywords": ["一级品类", "品类"], "priority_score": 86, "is_active": 1},
    {"dimension_code": "category_l2", "dimension_name": "二级品类", "domain_key": "product", "description": "产品二级品类。", "source_expression": "order_detail.category_l2 或 product_info.category_l2", "related_tables": ["order_detail", "product_info", "order_master", "inventory_stock"], "keywords": ["二级品类", "品类明细"], "priority_score": 84, "is_active": 1},
    {"dimension_code": "gender", "dimension_name": "性别", "domain_key": "user", "description": "用户性别。", "source_expression": "user_info.gender", "related_tables": ["user_info", "order_master"], "keywords": ["性别", "男", "女"], "priority_score": 88, "is_active": 1},
    {"dimension_code": "age", "dimension_name": "年龄", "domain_key": "user", "description": "用户年龄。", "source_expression": "user_info.age", "related_tables": ["user_info", "order_master"], "keywords": ["年龄", "18到25", "25到30"], "priority_score": 82, "is_active": 1},
    {"dimension_code": "city_tier", "dimension_name": "城市等级", "domain_key": "user", "description": "用户常住城市等级，例如一线、新一线、二线。", "source_expression": "user_info.city_tier", "related_tables": ["user_info", "order_master"], "keywords": ["城市等级", "城市层级", "一线城市", "新一线"], "priority_score": 81, "is_active": 1},
    {"dimension_code": "member_level", "dimension_name": "会员等级", "domain_key": "user", "description": "用户会员等级。", "source_expression": "user_info.member_level", "related_tables": ["user_info", "order_master"], "keywords": ["会员等级", "金卡", "黑金", "新客"], "priority_score": 86, "is_active": 1},
    {"dimension_code": "register_channel", "dimension_name": "注册渠道", "domain_key": "user", "description": "用户注册来源渠道。", "source_expression": "user_info.register_channel", "related_tables": ["user_info", "order_master"], "keywords": ["注册渠道", "注册来源", "拉新渠道"], "priority_score": 80, "is_active": 1},
    {"dimension_code": "customer_tag", "dimension_name": "用户标签", "domain_key": "user", "description": "用户分层标签，例如家庭囤货、品质白领。", "source_expression": "user_info.customer_tag", "related_tables": ["user_info", "order_master"], "keywords": ["用户标签", "标签", "家庭囤货", "品质白领"], "priority_score": 77, "is_active": 1},
    {"dimension_code": "device_type", "dimension_name": "设备类型", "domain_key": "user", "description": "用户常用设备类型，例如 iOS、Android。", "source_expression": "user_info.device_type", "related_tables": ["user_info", "order_master"], "keywords": ["设备类型", "终端类型", "iOS", "Android"], "priority_score": 73, "is_active": 1},
    {"dimension_code": "refund_reason", "dimension_name": "退款原因", "domain_key": "refund", "description": "退款或售后的原因分类。", "source_expression": "refund_master.refund_reason 或 refund_detail.refund_reason", "related_tables": ["refund_master", "refund_detail"], "keywords": ["退款原因", "售后原因", "包装破损", "配送超时"], "priority_score": 80, "is_active": 1},
    {"dimension_code": "refund_type", "dimension_name": "退款类型", "domain_key": "refund", "description": "退款单据的售后类型，例如仅退款、退货退款。", "source_expression": "refund_master.refund_type", "related_tables": ["refund_master"], "keywords": ["退款类型", "售后类型", "仅退款", "退货退款"], "priority_score": 78, "is_active": 1},
    {"dimension_code": "target_group", "dimension_name": "目标人群", "domain_key": "product", "description": "产品目标消费人群，例如儿童、家庭、职场白领。", "source_expression": "product_info.target_group", "related_tables": ["product_info", "order_detail", "order_master", "inventory_stock"], "keywords": ["目标人群", "儿童", "家庭", "白领"], "priority_score": 76, "is_active": 1},
    {"dimension_code": "temperature_zone", "dimension_name": "温层", "domain_key": "product", "description": "产品温层类型，例如常温、低温、冷冻。", "source_expression": "product_info.temperature_zone", "related_tables": ["product_info", "order_detail", "order_master", "inventory_stock"], "keywords": ["温层", "常温", "低温", "冷冻"], "priority_score": 75, "is_active": 1},
]

DEFAULT_JOINS = [
    {"join_code": "order_user", "domain_key": "transaction", "left_table": "order_master", "right_table": "user_info", "join_type": "INNER JOIN", "join_condition": "order_master.buyer_id = user_info.user_id", "description": "订单关联用户", "keywords": ["用户订单", "买家订单"], "priority_score": 92, "is_active": 1},
    {"join_code": "order_store", "domain_key": "transaction", "left_table": "order_master", "right_table": "store_info", "join_type": "INNER JOIN", "join_condition": "order_master.store_id = store_info.store_id", "description": "订单关联门店", "keywords": ["门店订单", "大区订单"], "priority_score": 92, "is_active": 1},
    {"join_code": "order_detail_master", "domain_key": "transaction", "left_table": "order_master", "right_table": "order_detail", "join_type": "INNER JOIN", "join_condition": "order_master.order_id = order_detail.order_id", "description": "订单主表关联订单明细", "keywords": ["订单商品", "商品销售"], "priority_score": 98, "is_active": 1},
    {"join_code": "detail_product", "domain_key": "product", "left_table": "order_detail", "right_table": "product_info", "join_type": "INNER JOIN", "join_condition": "order_detail.product_id = product_info.product_id", "description": "订单明细关联产品", "keywords": ["产品维度", "品牌维度"], "priority_score": 95, "is_active": 1},
    {"join_code": "refund_order", "domain_key": "refund", "left_table": "refund_master", "right_table": "order_master", "join_type": "INNER JOIN", "join_condition": "refund_master.order_id = order_master.order_id", "description": "退款主表关联订单主表", "keywords": ["售后订单", "退款订单"], "priority_score": 90, "is_active": 1},
    {"join_code": "refund_user", "domain_key": "refund", "left_table": "refund_master", "right_table": "user_info", "join_type": "INNER JOIN", "join_condition": "refund_master.buyer_id = user_info.user_id", "description": "退款主表关联用户", "keywords": ["退款用户"], "priority_score": 84, "is_active": 1},
    {"join_code": "refund_store", "domain_key": "refund", "left_table": "refund_master", "right_table": "store_info", "join_type": "INNER JOIN", "join_condition": "refund_master.store_id = store_info.store_id", "description": "退款主表关联门店", "keywords": ["退款门店", "售后门店"], "priority_score": 84, "is_active": 1},
    {"join_code": "refund_detail_master", "domain_key": "refund", "left_table": "refund_detail", "right_table": "refund_master", "join_type": "INNER JOIN", "join_condition": "refund_detail.refund_id = refund_master.refund_id", "description": "退款明细关联退款主表", "keywords": ["退款商品", "退款明细"], "priority_score": 93, "is_active": 1},
    {"join_code": "refund_detail_order_detail", "domain_key": "refund", "left_table": "refund_detail", "right_table": "order_detail", "join_type": "INNER JOIN", "join_condition": "refund_detail.order_detail_id = order_detail.order_detail_id", "description": "退款明细关联订单明细", "keywords": ["退款商品订单明细"], "priority_score": 88, "is_active": 1},
    {"join_code": "refund_detail_product", "domain_key": "refund", "left_table": "refund_detail", "right_table": "product_info", "join_type": "INNER JOIN", "join_condition": "refund_detail.product_id = product_info.product_id", "description": "退款明细关联产品", "keywords": ["退款品牌", "退款品类"], "priority_score": 82, "is_active": 1},
    {"join_code": "inventory_store", "domain_key": "inventory", "left_table": "inventory_stock", "right_table": "store_info", "join_type": "INNER JOIN", "join_condition": "inventory_stock.store_id = store_info.store_id", "description": "库存快照关联门店", "keywords": ["库存门店", "库存大区"], "priority_score": 92, "is_active": 1},
    {"join_code": "inventory_product", "domain_key": "inventory", "left_table": "inventory_stock", "right_table": "product_info", "join_type": "INNER JOIN", "join_condition": "inventory_stock.product_id = product_info.product_id", "description": "库存快照关联产品", "keywords": ["库存品牌", "库存品类"], "priority_score": 92, "is_active": 1},
]

DEFAULT_SYNONYMS = [
    {"target_type": "metric", "target_key": "sales_amount", "standard_name": "销售金额", "synonym_term": "GMV", "related_tables": ["order_master", "order_detail"], "weight_score": 18, "is_active": 1},
    {"target_type": "metric", "target_key": "sales_amount", "standard_name": "销售金额", "synonym_term": "成交额", "related_tables": ["order_master", "order_detail"], "weight_score": 15, "is_active": 1},
    {"target_type": "metric", "target_key": "sales_amount", "standard_name": "销售金额", "synonym_term": "订单金额", "related_tables": ["order_master", "order_detail"], "weight_score": 14, "is_active": 1},
    {"target_type": "metric", "target_key": "order_count", "standard_name": "订单数", "synonym_term": "单量", "related_tables": ["order_master"], "weight_score": 14, "is_active": 1},
    {"target_type": "metric", "target_key": "sales_volume", "standard_name": "销量", "synonym_term": "件数", "related_tables": ["order_detail", "order_master"], "weight_score": 14, "is_active": 1},
    {"target_type": "metric", "target_key": "refund_amount", "standard_name": "退款金额", "synonym_term": "售后金额", "related_tables": ["refund_master", "refund_detail"], "weight_score": 14, "is_active": 1},
    {"target_type": "metric", "target_key": "refund_item_count", "standard_name": "退款件数", "synonym_term": "退货件数", "related_tables": ["refund_master", "refund_detail"], "weight_score": 12, "is_active": 1},
    {"target_type": "metric", "target_key": "pay_buyer_count", "standard_name": "支付买家数", "synonym_term": "支付用户数", "related_tables": ["order_master", "user_info"], "weight_score": 13, "is_active": 1},
    {"target_type": "metric", "target_key": "pay_buyer_count", "standard_name": "支付买家数", "synonym_term": "用户量", "related_tables": ["order_master"], "weight_score": 11, "is_active": 1},
    {"target_type": "metric", "target_key": "pay_buyer_count", "standard_name": "支付买家数", "synonym_term": "支付的买家", "related_tables": ["order_master"], "weight_score": 12, "is_active": 1},
    {"target_type": "metric", "target_key": "refund_count", "standard_name": "退款单数", "synonym_term": "退款订单数", "related_tables": ["refund_master"], "weight_score": 12, "is_active": 1},
    {"target_type": "metric", "target_key": "refund_count", "standard_name": "退款单数", "synonym_term": "退货量", "related_tables": ["refund_master"], "weight_score": 10, "is_active": 1},
    {"target_type": "metric", "target_key": "discount_amount", "standard_name": "优惠金额", "synonym_term": "折扣金额", "related_tables": ["order_master", "order_detail"], "weight_score": 12, "is_active": 1},
    {"target_type": "metric", "target_key": "discount_rate", "standard_name": "优惠率", "synonym_term": "折扣率", "related_tables": ["order_master", "order_detail"], "weight_score": 11, "is_active": 1},
    {"target_type": "metric", "target_key": "discount_rate", "standard_name": "优惠率", "synonym_term": "让利率", "related_tables": ["order_master", "order_detail"], "weight_score": 10, "is_active": 1},
    {"target_type": "metric", "target_key": "avg_selling_price", "standard_name": "平均成交单价", "synonym_term": "平均售价", "related_tables": ["order_detail", "order_master"], "weight_score": 12, "is_active": 1},
    {"target_type": "metric", "target_key": "items_per_order", "standard_name": "单均件数", "synonym_term": "件单量", "related_tables": ["order_master", "order_detail"], "weight_score": 10, "is_active": 1},
    {"target_type": "metric", "target_key": "on_hand_inventory", "standard_name": "在库量", "synonym_term": "库存量", "related_tables": ["inventory_stock"], "weight_score": 13, "is_active": 1},
    {"target_type": "metric", "target_key": "available_inventory", "standard_name": "可售库存", "synonym_term": "可用库存", "related_tables": ["inventory_stock"], "weight_score": 14, "is_active": 1},
    {"target_type": "metric", "target_key": "available_inventory", "standard_name": "可售库存", "synonym_term": "可卖库存", "related_tables": ["inventory_stock"], "weight_score": 12, "is_active": 1},
    {"target_type": "metric", "target_key": "in_transit_inventory", "standard_name": "在途库存", "synonym_term": "在途量", "related_tables": ["inventory_stock"], "weight_score": 11, "is_active": 1},
    {"target_type": "metric", "target_key": "inventory_amount", "standard_name": "库存金额", "synonym_term": "库存货值", "related_tables": ["inventory_stock"], "weight_score": 12, "is_active": 1},
    {"target_type": "metric", "target_key": "stockout_sku_count", "standard_name": "缺货SKU数", "synonym_term": "缺货商品数", "related_tables": ["inventory_stock"], "weight_score": 12, "is_active": 1},
    {"target_type": "dimension", "target_key": "sales_region", "standard_name": "销售大区", "synonym_term": "大区", "related_tables": ["store_info", "order_master"], "weight_score": 12, "is_active": 1},
    {"target_type": "dimension", "target_key": "sales_region", "standard_name": "销售大区", "synonym_term": "区域", "related_tables": ["store_info", "order_master"], "weight_score": 10, "is_active": 1},
    {"target_type": "dimension", "target_key": "receiver_province", "standard_name": "收货省份", "synonym_term": "地区", "related_tables": ["order_master"], "weight_score": 8, "is_active": 1},
    {"target_type": "dimension", "target_key": "store_province", "standard_name": "省份", "synonym_term": "所在省份", "related_tables": ["store_info", "order_master"], "weight_score": 10, "is_active": 1},
    {"target_type": "dimension", "target_key": "store_province", "standard_name": "省份", "synonym_term": "门店省份", "related_tables": ["store_info", "order_master"], "weight_score": 9, "is_active": 1},
    {"target_type": "dimension", "target_key": "store_district", "standard_name": "门店区县", "synonym_term": "区县", "related_tables": ["store_info", "order_master"], "weight_score": 8, "is_active": 1},
    {"target_type": "dimension", "target_key": "receiver_district", "standard_name": "收货区县", "synonym_term": "收货区县", "related_tables": ["order_master"], "weight_score": 8, "is_active": 1},
    {"target_type": "dimension", "target_key": "store_name", "standard_name": "门店名称", "synonym_term": "店铺", "related_tables": ["store_info", "order_master"], "weight_score": 10, "is_active": 1},
    {"target_type": "dimension", "target_key": "brand_name", "standard_name": "品牌", "synonym_term": "牌子", "related_tables": ["order_detail", "product_info", "order_master"], "weight_score": 8, "is_active": 1},
    {"target_type": "dimension", "target_key": "platform", "standard_name": "平台", "synonym_term": "来源平台", "related_tables": ["order_master"], "weight_score": 10, "is_active": 1},
    {"target_type": "dimension", "target_key": "sales_channel", "standard_name": "销售渠道", "synonym_term": "渠道", "related_tables": ["order_master", "order_detail"], "weight_score": 12, "is_active": 1},
    {"target_type": "dimension", "target_key": "payment_method", "standard_name": "支付方式", "synonym_term": "付款方式", "related_tables": ["order_master"], "weight_score": 9, "is_active": 1},
    {"target_type": "dimension", "target_key": "promotion_type", "standard_name": "促销类型", "synonym_term": "活动类型", "related_tables": ["order_master"], "weight_score": 8, "is_active": 1},
    {"target_type": "dimension", "target_key": "city_tier", "standard_name": "城市等级", "synonym_term": "城市层级", "related_tables": ["user_info", "order_master"], "weight_score": 8, "is_active": 1},
    {"target_type": "dimension", "target_key": "org_level_1", "standard_name": "一级组织", "synonym_term": "组织", "related_tables": ["store_info", "order_master"], "weight_score": 8, "is_active": 1},
    {"target_type": "dimension", "target_key": "register_channel", "standard_name": "注册渠道", "synonym_term": "拉新渠道", "related_tables": ["user_info", "order_master"], "weight_score": 8, "is_active": 1},
    {"target_type": "dimension", "target_key": "target_group", "standard_name": "目标人群", "synonym_term": "人群", "related_tables": ["product_info", "order_detail"], "weight_score": 8, "is_active": 1},
    {"target_type": "dimension", "target_key": "temperature_zone", "standard_name": "温层", "synonym_term": "温区", "related_tables": ["product_info", "order_detail"], "weight_score": 8, "is_active": 1},
    {"target_type": "dimension", "target_key": "warehouse_name", "standard_name": "仓库名称", "synonym_term": "仓库", "related_tables": ["inventory_stock"], "weight_score": 10, "is_active": 1},
    {"target_type": "dimension", "target_key": "warehouse_type", "standard_name": "仓库类型", "synonym_term": "仓型", "related_tables": ["inventory_stock"], "weight_score": 8, "is_active": 1},
    {"target_type": "dimension", "target_key": "stock_status", "standard_name": "库存状态", "synonym_term": "缺货状态", "related_tables": ["inventory_stock"], "weight_score": 9, "is_active": 1},
    {"target_type": "table", "target_key": "refund_master", "standard_name": "退款主表", "synonym_term": "售后主表", "related_tables": ["refund_master"], "weight_score": 8, "is_active": 1},
    {"target_type": "table", "target_key": "order_detail", "standard_name": "订单明细子表", "synonym_term": "订单子表", "related_tables": ["order_detail"], "weight_score": 10, "is_active": 1},
    {"target_type": "table", "target_key": "product_info", "standard_name": "产品信息维度表", "synonym_term": "商品表", "related_tables": ["product_info"], "weight_score": 8, "is_active": 1},
    {"target_type": "table", "target_key": "inventory_stock", "standard_name": "库存快照表", "synonym_term": "库存表", "related_tables": ["inventory_stock"], "weight_score": 12, "is_active": 1},
    {"target_type": "dimension", "target_key": "sales_channel", "standard_name": "销售渠道", "synonym_term": "销售平台", "related_tables": ["order_master", "order_detail"], "weight_score": 8, "is_active": 1},
]

DEFAULT_EXAMPLES = [
    {
        "example_key": "ex_region_sales_30d",
        "domain_key": "transaction",
        "question_text": "按销售大区统计近30天销售金额和订单数，按销售金额降序",
        "summary_text": "需要使用订单主表关联门店表，按销售大区分组，并汇总销售金额与订单数。",
        "related_tables": ["order_master", "store_info"],
        "related_metrics": ["销售金额", "订单数"],
        "related_dimensions": ["销售大区"],
        "sql_example": "SELECT s.sales_region AS 销售大区, SUM(o.paid_amount) AS 销售金额, COUNT(DISTINCT o.order_id) AS 订单数 FROM order_master o JOIN store_info s ON o.store_id = s.store_id WHERE o.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND o.order_status IN ('已支付','已发货','已完成','部分退款') GROUP BY s.sales_region ORDER BY 销售金额 DESC LIMIT 200",
        "priority_score": 90,
        "is_active": 1,
    },
    {
        "example_key": "ex_brand_sales_30d",
        "domain_key": "product",
        "question_text": "按品牌统计近30天销量和销售金额，按销售金额降序",
        "summary_text": "需要使用订单明细关联订单主表，按品牌分组；金额必须使用订单明细行金额。",
        "related_tables": ["order_detail", "order_master", "product_info"],
        "related_metrics": ["销量", "销售金额"],
        "related_dimensions": ["品牌"],
        "sql_example": "SELECT od.brand_name AS 品牌, SUM(od.quantity) AS 销量, SUM(od.line_paid_amount) AS 销售金额 FROM order_detail od JOIN order_master om ON od.order_id = om.order_id WHERE om.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND om.order_status IN ('已支付','已发货','已完成','部分退款') GROUP BY od.brand_name ORDER BY 销售金额 DESC LIMIT 200",
        "priority_score": 92,
        "is_active": 1,
    },
    {
        "example_key": "ex_female_sales_30d",
        "domain_key": "user",
        "question_text": "统计近30天女性用户销售金额",
        "summary_text": "需要订单主表关联用户表，过滤女性用户后汇总销售金额。",
        "related_tables": ["order_master", "user_info"],
        "related_metrics": ["销售金额"],
        "related_dimensions": ["性别"],
        "sql_example": "SELECT SUM(o.paid_amount) AS 销售金额 FROM order_master o JOIN user_info u ON o.buyer_id = u.user_id WHERE u.gender = '女' AND o.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND o.order_status IN ('已支付','已发货','已完成','部分退款') LIMIT 200",
        "priority_score": 85,
        "is_active": 1,
    },
    {
        "example_key": "ex_refund_reason",
        "domain_key": "refund",
        "question_text": "按退款原因统计近30天退款金额和退款单数",
        "summary_text": "需要使用退款主表，按退款原因分组统计退款金额与退款单数。",
        "related_tables": ["refund_master"],
        "related_metrics": ["退款金额", "退款单数"],
        "related_dimensions": ["退款原因"],
        "sql_example": "SELECT refund_reason AS 退款原因, SUM(refund_amount) AS 退款金额, COUNT(DISTINCT refund_id) AS 退款单数 FROM refund_master WHERE applied_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) GROUP BY refund_reason ORDER BY 退款金额 DESC LIMIT 200",
        "priority_score": 82,
        "is_active": 1,
    },
    {
        "example_key": "ex_channel_gmv_refund_rate",
        "domain_key": "transaction",
        "question_text": "按销售渠道统计近30天GMV、退款金额和退款率，按销售渠道展示",
        "summary_text": "按销售渠道做订单级 GMV 与退款分析时，可用订单主表结合退款主表；如果问题额外限定品牌、产品、品类或 SKU，则退款金额和退款率必须切换到 refund_detail 口径。",
        "related_tables": ["order_master", "order_detail", "refund_master"],
        "related_metrics": ["销售金额", "退款金额", "退款率"],
        "related_dimensions": ["销售渠道"],
        "sql_example": "WITH sales AS (SELECT om.sales_channel AS 销售渠道, SUM(od.line_paid_amount) AS GMV FROM order_master om JOIN order_detail od ON om.order_id = od.order_id WHERE om.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND om.order_status IN ('已支付','已发货','已完成','部分退款') GROUP BY om.sales_channel), refunds AS (SELECT om.sales_channel AS 销售渠道, SUM(rm.refund_amount) AS 退款金额 FROM refund_master rm JOIN order_master om ON rm.order_id = om.order_id WHERE rm.applied_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) GROUP BY om.sales_channel) SELECT s.销售渠道 AS 销售渠道, s.GMV AS GMV, COALESCE(r.退款金额, 0) AS 退款金额, CASE WHEN s.GMV = 0 THEN 0 ELSE COALESCE(r.退款金额, 0) / s.GMV END AS 退款率 FROM sales s LEFT JOIN refunds r ON s.销售渠道 = r.销售渠道 ORDER BY 销售渠道 DESC LIMIT 200",
        "priority_score": 94,
        "is_active": 1,
    },
    {
        "example_key": "ex_brand_channel_gmv_refund_rate",
        "domain_key": "product",
        "question_text": "统计近30天各渠道蒙牛GMV、退款金额和退款率，按渠道降序展示",
        "summary_text": "问题包含品牌过滤但按渠道展示，销售金额必须使用订单明细 line_paid_amount，退款金额和退款率必须改用退款明细 refund_detail 口径，并通过 order_detail.order_detail_id 关联，避免订单级退款金额被重复累计。",
        "related_tables": ["order_master", "order_detail", "refund_master", "refund_detail"],
        "related_metrics": ["销售金额", "退款金额", "退款率"],
        "related_dimensions": ["销售渠道", "品牌"],
        "sql_example": "WITH sales AS (SELECT om.sales_channel AS 销售渠道, SUM(od.line_paid_amount) AS GMV FROM order_master om JOIN order_detail od ON om.order_id = od.order_id WHERE od.brand_name = '蒙牛' AND om.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND om.order_status IN ('已支付','已发货','已完成','部分退款') GROUP BY om.sales_channel), refunds AS (SELECT om.sales_channel AS 销售渠道, SUM(rd.refund_amount) AS 退款金额 FROM refund_detail rd JOIN refund_master rm ON rd.refund_id = rm.refund_id JOIN order_detail od ON rd.order_detail_id = od.order_detail_id JOIN order_master om ON od.order_id = om.order_id WHERE od.brand_name = '蒙牛' AND rm.applied_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) GROUP BY om.sales_channel) SELECT s.销售渠道 AS 销售渠道, s.GMV AS GMV, COALESCE(r.退款金额, 0) AS 退款金额, CASE WHEN s.GMV = 0 THEN 0 ELSE COALESCE(r.退款金额, 0) / s.GMV END AS 退款率 FROM sales s LEFT JOIN refunds r ON s.销售渠道 = r.销售渠道 ORDER BY GMV DESC LIMIT 200",
        "priority_score": 97,
        "is_active": 1,
    },
    {
        "example_key": "ex_brand_member_sales_refund",
        "domain_key": "product",
        "question_text": "按品牌和会员等级统计近30天销售金额、销量和退款金额",
        "summary_text": "需要订单主表、订单明细、用户信息和退款明细联查，按品牌和会员等级分组。",
        "related_tables": ["order_master", "order_detail", "user_info", "refund_master", "refund_detail"],
        "related_metrics": ["销售金额", "销量", "退款金额"],
        "related_dimensions": ["品牌", "会员等级"],
        "sql_example": "WITH sales_data AS (SELECT od.brand_name AS 品牌, ui.member_level AS 会员等级, SUM(od.line_paid_amount) AS 销售金额, SUM(od.quantity) AS 销量 FROM order_master om JOIN order_detail od ON om.order_id = od.order_id JOIN user_info ui ON om.buyer_id = ui.user_id WHERE om.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND om.order_status IN ('已支付','已发货','已完成','部分退款') GROUP BY od.brand_name, ui.member_level), refund_data AS (SELECT od.brand_name AS 品牌, ui.member_level AS 会员等级, SUM(rd.refund_amount) AS 退款金额 FROM refund_master rm JOIN refund_detail rd ON rm.refund_id = rd.refund_id JOIN order_detail od ON rd.order_detail_id = od.order_detail_id JOIN user_info ui ON rm.buyer_id = ui.user_id WHERE rm.applied_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) GROUP BY od.brand_name, ui.member_level) SELECT s.品牌 AS 品牌, s.会员等级 AS 会员等级, s.销售金额 AS 销售金额, s.销量 AS 销量, COALESCE(r.退款金额, 0) AS 退款金额 FROM sales_data s LEFT JOIN refund_data r ON s.品牌 = r.品牌 AND s.会员等级 = r.会员等级 ORDER BY 销售金额 DESC LIMIT 200",
        "priority_score": 91,
        "is_active": 1,
    },
    {
        "example_key": "ex_category_discount",
        "domain_key": "product",
        "question_text": "按一级品类统计近30天销售金额、优惠金额和平均成交单价",
        "summary_text": "需要订单明细关联订单主表，按一级品类分组统计销售、优惠和平均成交单价。",
        "related_tables": ["order_master", "order_detail"],
        "related_metrics": ["销售金额", "优惠金额", "平均成交单价"],
        "related_dimensions": ["一级品类"],
        "sql_example": "SELECT od.category_l1 AS 一级品类, SUM(od.line_paid_amount) AS 销售金额, SUM(od.line_discount_amount) AS 优惠金额, CASE WHEN SUM(od.quantity) = 0 THEN 0 ELSE SUM(od.line_paid_amount) / SUM(od.quantity) END AS 平均成交单价 FROM order_detail od JOIN order_master om ON od.order_id = om.order_id WHERE om.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND om.order_status IN ('已支付','已发货','已完成','部分退款') GROUP BY od.category_l1 ORDER BY 销售金额 DESC LIMIT 200",
        "priority_score": 88,
        "is_active": 1,
    },
    {
        "example_key": "ex_payment_method_sales",
        "domain_key": "transaction",
        "question_text": "按支付方式统计近30天销售金额、订单数和客单价",
        "summary_text": "需要使用订单主表，按支付方式分组，汇总销售金额、订单数，并计算客单价。",
        "related_tables": ["order_master"],
        "related_metrics": ["销售金额", "订单数", "客单价"],
        "related_dimensions": ["支付方式"],
        "sql_example": "SELECT om.payment_method AS 支付方式, SUM(om.paid_amount) AS 销售金额, COUNT(DISTINCT om.order_id) AS 订单数, CASE WHEN COUNT(DISTINCT om.order_id)=0 THEN 0 ELSE SUM(om.paid_amount)/COUNT(DISTINCT om.order_id) END AS 客单价 FROM order_master om WHERE om.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND om.order_status IN ('已支付','已发货','已完成','部分退款') GROUP BY om.payment_method ORDER BY 销售金额 DESC LIMIT 200",
        "priority_score": 82,
        "is_active": 1,
    },
    {
        "example_key": "ex_region_province_pay_buyer_sales_volume_refund_count",
        "domain_key": "store",
        "question_text": "按销售大区和省份统计近30天支付买家数、销量和退款单数",
        "summary_text": "需要使用 store_info 的销售大区和省份维度；支付买家数来自 order_master 去重 buyer_id，销量来自 order_detail.quantity，退款单数来自 refund_master 去重 refund_id。",
        "related_tables": ["order_master", "order_detail", "store_info", "refund_master"],
        "related_metrics": ["支付买家数", "销量", "退款单数"],
        "related_dimensions": ["销售大区", "省份"],
        "sql_example": "WITH sales_data AS (SELECT s.sales_region AS 销售大区, s.province AS 省份, COUNT(DISTINCT om.buyer_id) AS 支付买家数, SUM(od.quantity) AS 销量 FROM order_master om JOIN order_detail od ON om.order_id = od.order_id JOIN store_info s ON om.store_id = s.store_id WHERE om.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND om.order_status IN ('已支付','已发货','已完成','部分退款') GROUP BY s.sales_region, s.province), refund_data AS (SELECT s.sales_region AS 销售大区, s.province AS 省份, COUNT(DISTINCT rm.refund_id) AS 退款单数 FROM refund_master rm JOIN order_master om ON rm.order_id = om.order_id JOIN store_info s ON om.store_id = s.store_id WHERE rm.applied_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) GROUP BY s.sales_region, s.province) SELECT sd.销售大区 AS 销售大区, sd.省份 AS 省份, sd.支付买家数 AS 支付买家数, sd.销量 AS 销量, COALESCE(rd.退款单数, 0) AS 退款单数 FROM sales_data sd LEFT JOIN refund_data rd ON sd.销售大区 = rd.销售大区 AND sd.省份 = rd.省份 ORDER BY 销售大区, 省份 LIMIT 200",
        "priority_score": 96,
        "is_active": 1,
    },
    {
        "example_key": "ex_category_discount_rate",
        "domain_key": "product",
        "question_text": "按一级品类统计近30天优惠金额、优惠率和平均成交单价",
        "summary_text": "需要使用订单明细关联订单主表；商品粒度优惠率必须使用 line_discount_amount / line_gross_amount，平均成交单价取 line_paid_amount / quantity。",
        "related_tables": ["order_detail", "order_master"],
        "related_metrics": ["优惠金额", "优惠率", "平均成交单价"],
        "related_dimensions": ["一级品类"],
        "sql_example": "SELECT od.category_l1 AS 一级品类, SUM(od.line_discount_amount) AS 优惠金额, CASE WHEN SUM(od.line_gross_amount)=0 THEN 0 ELSE SUM(od.line_discount_amount)/SUM(od.line_gross_amount) END AS 优惠率, CASE WHEN SUM(od.quantity)=0 THEN 0 ELSE SUM(od.line_paid_amount)/SUM(od.quantity) END AS 平均成交单价 FROM order_detail od JOIN order_master om ON od.order_id = om.order_id WHERE om.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND om.order_status IN ('已支付','已发货','已完成','部分退款') GROUP BY od.category_l1 ORDER BY 优惠金额 DESC LIMIT 200",
        "priority_score": 86,
        "is_active": 1,
    },
    {
        "example_key": "ex_city_tier_pay_buyer",
        "domain_key": "user",
        "question_text": "按城市等级统计近30天支付买家数和销售金额",
        "summary_text": "需要使用订单主表关联用户表，按城市等级分组，统计支付买家数与销售金额。",
        "related_tables": ["order_master", "user_info"],
        "related_metrics": ["支付买家数", "销售金额"],
        "related_dimensions": ["城市等级"],
        "sql_example": "SELECT ui.city_tier AS 城市等级, COUNT(DISTINCT om.buyer_id) AS 支付买家数, SUM(om.paid_amount) AS 销售金额 FROM order_master om JOIN user_info ui ON om.buyer_id = ui.user_id WHERE om.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND om.order_status IN ('已支付','已发货','已完成','部分退款') GROUP BY ui.city_tier ORDER BY 销售金额 DESC LIMIT 200",
        "priority_score": 84,
        "is_active": 1,
    },
    {
        "example_key": "ex_region_brand_inventory",
        "domain_key": "inventory",
        "question_text": "按销售大区和品牌统计当前可售库存、在途库存和库存金额",
        "summary_text": "需要使用库存快照表关联门店和产品维表，按销售大区和品牌分组，统计可售库存、在途库存和库存金额。",
        "related_tables": ["inventory_stock", "store_info", "product_info"],
        "related_metrics": ["可售库存", "在途库存", "库存金额"],
        "related_dimensions": ["销售大区", "品牌"],
        "sql_example": "SELECT s.sales_region AS 销售大区, p.brand_name AS 品牌, SUM(i.available_qty) AS 可售库存, SUM(i.in_transit_qty) AS 在途库存, SUM(i.inventory_amount) AS 库存金额 FROM inventory_stock i JOIN store_info s ON i.store_id = s.store_id JOIN product_info p ON i.product_id = p.product_id WHERE i.snapshot_date = CURDATE() GROUP BY s.sales_region, p.brand_name ORDER BY 库存金额 DESC LIMIT 200",
        "priority_score": 92,
        "is_active": 1,
    },
    {
        "example_key": "ex_channel_stockout_inventory",
        "domain_key": "inventory",
        "question_text": "按销售渠道统计当前可售库存和缺货SKU数，按缺货SKU数降序",
        "summary_text": "需要使用库存快照表，按销售渠道分组，统计可售库存和缺货SKU数。",
        "related_tables": ["inventory_stock"],
        "related_metrics": ["可售库存", "缺货SKU数"],
        "related_dimensions": ["销售渠道"],
        "sql_example": "SELECT i.sales_channel AS 销售渠道, SUM(i.available_qty) AS 可售库存, COUNT(DISTINCT CASE WHEN i.available_qty <= 0 THEN i.product_id END) AS 缺货SKU数 FROM inventory_stock i WHERE i.snapshot_date = CURDATE() GROUP BY i.sales_channel ORDER BY 缺货SKU数 DESC, 可售库存 ASC LIMIT 200",
        "priority_score": 90,
        "is_active": 1,
    },
]

ADMIN_ENTITY_CONFIG = {
    "domains": {
        "table": "semantic_domain",
        "key_field": "domain_key",
        "fields": ["domain_key", "domain_name", "description", "priority_score", "is_active"],
        "json_fields": [],
        "order_by": "priority_score DESC, domain_name ASC",
    },
    "tables": {
        "table": "semantic_table",
        "key_field": "table_name",
        "fields": [
            "table_name",
            "domain_key",
            "business_name",
            "table_role",
            "description",
            "table_comment",
            "keywords_json",
            "business_dimensions_json",
            "business_metrics_json",
            "priority_score",
            "is_active",
        ],
        "json_fields": ["keywords_json", "business_dimensions_json", "business_metrics_json"],
        "order_by": "priority_score DESC, table_name ASC",
    },
    "columns": {
        "table": "semantic_column",
        "key_field": "id",
        "fields": [
            "id",
            "table_name",
            "column_name",
            "business_name",
            "column_comment",
            "data_type",
            "ordinal_position",
            "is_time_dimension",
            "is_dimension_candidate",
            "is_metric_candidate",
            "is_active",
        ],
        "json_fields": [],
        "order_by": "table_name ASC, ordinal_position ASC",
        "read_only": True,
    },
    "metrics": {
        "table": "semantic_metric",
        "key_field": "metric_code",
        "fields": [
            "metric_code",
            "metric_name",
            "domain_key",
            "definition_name",
            "description",
            "default_expression",
            "default_filters",
            "related_tables_json",
            "keywords_json",
            "priority_score",
            "is_active",
        ],
        "json_fields": ["related_tables_json", "keywords_json"],
        "order_by": "priority_score DESC, metric_name ASC",
    },
    "dimensions": {
        "table": "semantic_dimension",
        "key_field": "dimension_code",
        "fields": [
            "dimension_code",
            "dimension_name",
            "domain_key",
            "description",
            "source_expression",
            "related_tables_json",
            "keywords_json",
            "priority_score",
            "is_active",
        ],
        "json_fields": ["related_tables_json", "keywords_json"],
        "order_by": "priority_score DESC, dimension_name ASC",
    },
    "joins": {
        "table": "semantic_join",
        "key_field": "join_code",
        "fields": [
            "join_code",
            "domain_key",
            "left_table",
            "right_table",
            "join_type",
            "join_condition",
            "description",
            "keywords_json",
            "priority_score",
            "is_active",
        ],
        "json_fields": ["keywords_json"],
        "order_by": "priority_score DESC, join_code ASC",
    },
    "synonyms": {
        "table": "semantic_synonym",
        "key_field": "id",
        "fields": [
            "id",
            "target_type",
            "target_key",
            "standard_name",
            "synonym_term",
            "related_tables_json",
            "weight_score",
            "is_active",
        ],
        "json_fields": ["related_tables_json"],
        "order_by": "weight_score DESC, synonym_term ASC",
        "auto_increment": True,
    },
    "examples": {
        "table": "semantic_example",
        "key_field": "example_key",
        "fields": [
            "example_key",
            "domain_key",
            "question_text",
            "summary_text",
            "related_tables_json",
            "related_metrics_json",
            "related_dimensions_json",
            "sql_example",
            "priority_score",
            "is_active",
        ],
        "json_fields": ["related_tables_json", "related_metrics_json", "related_dimensions_json"],
        "order_by": "priority_score DESC, example_key ASC",
    },
    "search_docs": {
        "table": "semantic_search_doc",
        "key_field": "id",
        "fields": [
            "id",
            "source_type",
            "source_key",
            "source_name",
            "domain_key",
            "related_tables_json",
            "related_metrics_json",
            "related_dimensions_json",
            "priority_score",
            "embedding_status",
            "updated_at",
        ],
        "json_fields": ["related_tables_json", "related_metrics_json", "related_dimensions_json"],
        "order_by": "updated_at DESC, priority_score DESC",
        "read_only": True,
    },
}


def get_db_conn() -> pymysql.connections.Connection:
    return pymysql.connect(**DB_CONFIG)


def _json_dumps(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return json.dumps([], ensure_ascii=False)
        if stripped.startswith("["):
            try:
                json.loads(stripped)
                return stripped
            except json.JSONDecodeError:
                pass
        parts = [item.strip() for item in stripped.replace("\n", ",").split(",")]
        value = [item for item in parts if item]
    return json.dumps(value, ensure_ascii=False)


def _json_loads(value: Any) -> list[Any]:
    if value in (None, "", b""):
        return []
    if isinstance(value, list):
        return value
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return []
    return loaded if isinstance(loaded, list) else []


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _join_search_lines(parts: list[Any]) -> str:
    return "\n".join(_safe_text(part) for part in parts)


def _join_search_terms(values: list[Any] | None) -> str:
    return " ".join(_safe_text(value) for value in (values or []) if _safe_text(value))


def _bool_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    text = str(value or "").strip().lower()
    return 1 if text in {"1", "true", "yes", "y", "on"} else 0


def _normalize_for_match(text: str) -> str:
    return _safe_text(text).lower()


def _keyword_match_score(question_text: str, keywords: list[str]) -> int:
    normalized_question = _normalize_for_match(question_text)
    normalized_question_loose = normalized_question.replace('的', '')
    best_score = 0
    for keyword in keywords or []:
        normalized_keyword = _normalize_for_match(keyword)
        if not normalized_keyword:
            continue
        if normalized_keyword in normalized_question:
            best_score = max(best_score, len(normalized_keyword))
        normalized_keyword_loose = normalized_keyword.replace('的', '')
        if normalized_keyword_loose and normalized_keyword_loose in normalized_question_loose:
            best_score = max(best_score, len(normalized_keyword_loose))
    return best_score


def _content_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(a * b for a, b in zip(vec_a, vec_b, strict=False))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


@lru_cache(maxsize=64)
def get_distinct_dimension_values(table_name: str, column_name: str) -> tuple[str, ...]:
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT DISTINCT `{column_name}` AS value FROM `{table_name}` WHERE `{column_name}` IS NOT NULL AND `{column_name}` <> ''"
            )
            rows = cursor.fetchall()
    values: list[str] = []
    for row in rows:
        value = _safe_text(row.get("value"))
        if value and value not in values:
            values.append(value)
    return tuple(values)


def _get_embedding_client() -> tuple[OpenAI | None, str, str]:
    provider = str(LOCAL_EMBEDDING_PROVIDER or 'auto').lower()
    if provider in {'auto', 'local'} and LOCAL_EMBEDDING_BASE_URL:
        try:
            return OpenAI(api_key='local', base_url=LOCAL_EMBEDDING_BASE_URL), LOCAL_EMBEDDING_MODEL, 'local'
        except Exception:  # noqa: BLE001
            if provider == 'local':
                return None, '', ''
    if provider in {'auto', 'dashscope'} and DASHSCOPE_API_KEY:
        return OpenAI(api_key=DASHSCOPE_API_KEY, base_url=DASHSCOPE_BASE_URL), DASHSCOPE_EMBEDDING_MODEL, 'dashscope'
    return None, '', ''


def _embed_texts(texts: list[str]) -> list[list[float]]:
    client, model_name, provider_name = _get_embedding_client()
    if client is None or not texts:
        return []
    try:
        response = client.embeddings.create(model=model_name, input=texts)
    except Exception:  # noqa: BLE001
        if provider_name == 'local' and DASHSCOPE_API_KEY:
            try:
                fallback_client = OpenAI(api_key=DASHSCOPE_API_KEY, base_url=DASHSCOPE_BASE_URL)
                response = fallback_client.embeddings.create(model=DASHSCOPE_EMBEDDING_MODEL, input=texts)
            except Exception:  # noqa: BLE001
                return []
        else:
            return []
    return [item.embedding for item in response.data]


def _resolve_embedding_model_name() -> str:
    provider = str(LOCAL_EMBEDDING_PROVIDER or 'auto').lower()
    if provider in {'local', 'auto'} and LOCAL_EMBEDDING_BASE_URL:
        return LOCAL_EMBEDDING_MODEL
    return DASHSCOPE_EMBEDDING_MODEL


def _ensure_fulltext_index(cursor: pymysql.cursors.DictCursor) -> None:
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'semantic_search_doc'
          AND index_name = 'ft_semantic_search_doc_search_text'
        LIMIT 1
        """
    )
    if cursor.fetchone():
        return
    try:
        cursor.execute(
            "ALTER TABLE `semantic_search_doc` ADD FULLTEXT KEY `ft_semantic_search_doc_search_text` (`search_text`) WITH PARSER ngram"
        )
    except pymysql.MySQLError:
        cursor.execute(
            "ALTER TABLE `semantic_search_doc` ADD FULLTEXT KEY `ft_semantic_search_doc_search_text` (`search_text`)"
        )


def _seed_defaults(conn: pymysql.connections.Connection) -> None:
    with conn.cursor() as cursor:
        for item in DEFAULT_DOMAINS:
            cursor.execute(
                """
                INSERT IGNORE INTO `semantic_domain` (`domain_key`, `domain_name`, `description`, `priority_score`, `is_active`)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    item["domain_key"],
                    item["domain_name"],
                    item["description"],
                    item["priority_score"],
                    item["is_active"],
                ),
            )

        for item in DEFAULT_TABLES:
            cursor.execute(
                """
                INSERT IGNORE INTO `semantic_table` (
                    `table_name`, `domain_key`, `business_name`, `table_role`, `description`,
                    `keywords_json`, `business_dimensions_json`, `business_metrics_json`, `priority_score`, `is_active`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    item["table_name"],
                    item["domain_key"],
                    item["business_name"],
                    item["table_role"],
                    item["description"],
                    _json_dumps(item["keywords"]),
                    _json_dumps(item["business_dimensions"]),
                    _json_dumps(item["business_metrics"]),
                    item["priority_score"],
                    item["is_active"],
                ),
            )

        for item in DEFAULT_METRICS:
            cursor.execute(
                """
                INSERT IGNORE INTO `semantic_metric` (
                    `metric_code`, `metric_name`, `domain_key`, `definition_name`, `description`,
                    `default_expression`, `default_filters`, `related_tables_json`, `keywords_json`, `priority_score`, `is_active`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    item["metric_code"],
                    item["metric_name"],
                    item["domain_key"],
                    item["definition_name"],
                    item["description"],
                    item["default_expression"],
                    item["default_filters"],
                    _json_dumps(item["related_tables"]),
                    _json_dumps(item["keywords"]),
                    item["priority_score"],
                    item["is_active"],
                ),
            )

        for item in DEFAULT_DIMENSIONS:
            cursor.execute(
                """
                INSERT IGNORE INTO `semantic_dimension` (
                    `dimension_code`, `dimension_name`, `domain_key`, `description`, `source_expression`,
                    `related_tables_json`, `keywords_json`, `priority_score`, `is_active`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    item["dimension_code"],
                    item["dimension_name"],
                    item["domain_key"],
                    item["description"],
                    item["source_expression"],
                    _json_dumps(item["related_tables"]),
                    _json_dumps(item["keywords"]),
                    item["priority_score"],
                    item["is_active"],
                ),
            )

        for item in DEFAULT_JOINS:
            cursor.execute(
                """
                INSERT IGNORE INTO `semantic_join` (
                    `join_code`, `domain_key`, `left_table`, `right_table`, `join_type`,
                    `join_condition`, `description`, `keywords_json`, `priority_score`, `is_active`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    item["join_code"],
                    item["domain_key"],
                    item["left_table"],
                    item["right_table"],
                    item["join_type"],
                    item["join_condition"],
                    item["description"],
                    _json_dumps(item["keywords"]),
                    item["priority_score"],
                    item["is_active"],
                ),
            )

        for item in DEFAULT_SYNONYMS:
            cursor.execute(
                """
                INSERT IGNORE INTO `semantic_synonym` (
                    `target_type`, `target_key`, `standard_name`, `synonym_term`,
                    `related_tables_json`, `weight_score`, `is_active`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    item["target_type"],
                    item["target_key"],
                    item["standard_name"],
                    item["synonym_term"],
                    _json_dumps(item["related_tables"]),
                    item["weight_score"],
                    item["is_active"],
                ),
            )

        for item in DEFAULT_EXAMPLES:
            cursor.execute(
                """
                INSERT IGNORE INTO `semantic_example` (
                    `example_key`, `domain_key`, `question_text`, `summary_text`,
                    `related_tables_json`, `related_metrics_json`, `related_dimensions_json`,
                    `sql_example`, `priority_score`, `is_active`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    item["example_key"],
                    item["domain_key"],
                    item["question_text"],
                    item["summary_text"],
                    _json_dumps(item["related_tables"]),
                    _json_dumps(item["related_metrics"]),
                    _json_dumps(item["related_dimensions"]),
                    item["sql_example"],
                    item["priority_score"],
                    item["is_active"],
                ),
            )


def sync_semantic_schema(conn: pymysql.connections.Connection | None = None) -> None:
    owns_conn = conn is None
    if owns_conn:
        conn = get_db_conn()
    assert conn is not None
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT t.TABLE_NAME, t.TABLE_COMMENT
            FROM information_schema.TABLES t
            WHERE t.TABLE_SCHEMA = DATABASE()
              AND t.TABLE_NAME IN (
                  'order_master', 'order_detail', 'user_info', 'product_info',
                  'store_info', 'refund_master', 'refund_detail', 'inventory_stock'
              )
            ORDER BY t.TABLE_NAME
            """
        )
        tables = cursor.fetchall()
        for row in tables:
            cursor.execute(
                """
                UPDATE `semantic_table`
                SET `table_comment` = %s,
                    `business_name` = CASE WHEN `business_name` IS NULL OR `business_name` = '' THEN %s ELSE `business_name` END,
                    `updated_at` = NOW()
                WHERE `table_name` = %s
                """,
                (row["TABLE_COMMENT"], row["TABLE_COMMENT"] or row["TABLE_NAME"], row["TABLE_NAME"]),
            )

        cursor.execute(
            """
            SELECT
                c.TABLE_NAME,
                c.COLUMN_NAME,
                c.COLUMN_COMMENT,
                c.DATA_TYPE,
                c.ORDINAL_POSITION
            FROM information_schema.COLUMNS c
            WHERE c.TABLE_SCHEMA = DATABASE()
              AND c.TABLE_NAME IN (
                  'order_master', 'order_detail', 'user_info', 'product_info',
                  'store_info', 'refund_master', 'refund_detail', 'inventory_stock'
              )
            ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
            """
        )
        column_rows = cursor.fetchall()
        for row in column_rows:
            column_name = row["COLUMN_NAME"]
            is_time_dimension = 1 if column_name.endswith("_at") or column_name.endswith("_date") else 0
            is_metric_candidate = 1 if any(token in column_name for token in ["amount", "count", "price", "quantity", "points"]) else 0
            is_dimension_candidate = 1 if not is_metric_candidate or is_time_dimension else 0
            business_name = row["COLUMN_COMMENT"] or row["COLUMN_NAME"]
            cursor.execute(
                """
                INSERT INTO `semantic_column` (
                    `table_name`, `column_name`, `business_name`, `column_comment`, `data_type`, `ordinal_position`,
                    `is_time_dimension`, `is_dimension_candidate`, `is_metric_candidate`, `is_active`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                ON DUPLICATE KEY UPDATE
                    `column_comment` = VALUES(`column_comment`),
                    `data_type` = VALUES(`data_type`),
                    `ordinal_position` = VALUES(`ordinal_position`),
                    `business_name` = CASE WHEN `business_name` IS NULL OR `business_name` = '' THEN VALUES(`business_name`) ELSE `business_name` END,
                    `is_time_dimension` = VALUES(`is_time_dimension`),
                    `is_dimension_candidate` = VALUES(`is_dimension_candidate`),
                    `is_metric_candidate` = VALUES(`is_metric_candidate`),
                    `is_active` = 1,
                    `updated_at` = NOW()
                """,
                (
                    row["TABLE_NAME"],
                    row["COLUMN_NAME"],
                    business_name,
                    row["COLUMN_COMMENT"],
                    row["DATA_TYPE"],
                    row["ORDINAL_POSITION"],
                    is_time_dimension,
                    is_dimension_candidate,
                    is_metric_candidate,
                ),
            )

        existing_column_keys = {(row["TABLE_NAME"], row["COLUMN_NAME"]) for row in column_rows}
        cursor.execute("SELECT `table_name`, `column_name` FROM `semantic_column`")
        stored_column_keys = {(row["table_name"], row["column_name"]) for row in cursor.fetchall()}
        stale_columns = stored_column_keys - existing_column_keys
        if stale_columns:
            cursor.executemany(
                "UPDATE `semantic_column` SET `is_active` = 0, `updated_at` = NOW() WHERE `table_name` = %s AND `column_name` = %s",
                list(stale_columns),
            )
    conn.commit()
    if owns_conn:
        conn.close()


def _fetch_rows(cursor: pymysql.cursors.DictCursor, sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    cursor.execute(sql, params or ())
    return list(cursor.fetchall())


def _load_semantic_entities(conn: pymysql.connections.Connection) -> dict[str, Any]:
    with conn.cursor() as cursor:
        domains = _fetch_rows(cursor, "SELECT * FROM `semantic_domain` WHERE `is_active` = 1 ORDER BY `priority_score` DESC, `domain_name`")
        tables = _fetch_rows(cursor, "SELECT * FROM `semantic_table` WHERE `is_active` = 1 ORDER BY `priority_score` DESC, `table_name`")
        metrics = _fetch_rows(cursor, "SELECT * FROM `semantic_metric` WHERE `is_active` = 1 ORDER BY `priority_score` DESC, `metric_name`")
        dimensions = _fetch_rows(cursor, "SELECT * FROM `semantic_dimension` WHERE `is_active` = 1 ORDER BY `priority_score` DESC, `dimension_name`")
        joins = _fetch_rows(cursor, "SELECT * FROM `semantic_join` WHERE `is_active` = 1 ORDER BY `priority_score` DESC, `join_code`")
        synonyms = _fetch_rows(cursor, "SELECT * FROM `semantic_synonym` WHERE `is_active` = 1 ORDER BY `weight_score` DESC, `synonym_term`")
        examples = _fetch_rows(cursor, "SELECT * FROM `semantic_example` WHERE `is_active` = 1 ORDER BY `priority_score` DESC, `example_key`")
        columns = _fetch_rows(cursor, "SELECT * FROM `semantic_column` WHERE `is_active` = 1 ORDER BY `table_name`, `ordinal_position`")

    for row in tables:
        row["keywords"] = _json_loads(row.pop("keywords_json", None))
        row["business_dimensions"] = _json_loads(row.pop("business_dimensions_json", None))
        row["business_metrics"] = _json_loads(row.pop("business_metrics_json", None))
    for row in metrics:
        row["keywords"] = _json_loads(row.pop("keywords_json", None))
        row["related_tables"] = _json_loads(row.pop("related_tables_json", None))
    for row in dimensions:
        row["keywords"] = _json_loads(row.pop("keywords_json", None))
        row["related_tables"] = _json_loads(row.pop("related_tables_json", None))
    for row in joins:
        row["keywords"] = _json_loads(row.pop("keywords_json", None))
    for row in synonyms:
        row["related_tables"] = _json_loads(row.pop("related_tables_json", None))
    for row in examples:
        row["related_tables"] = _json_loads(row.pop("related_tables_json", None))
        row["related_metrics"] = _json_loads(row.pop("related_metrics_json", None))
        row["related_dimensions"] = _json_loads(row.pop("related_dimensions_json", None))

    column_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for column in columns:
        column_map[column["table_name"]].append(column)

    return {
        "domains": domains,
        "tables": tables,
        "metrics": metrics,
        "dimensions": dimensions,
        "joins": joins,
        "synonyms": synonyms,
        "examples": examples,
        "columns": columns,
        "column_map": column_map,
    }


def rebuild_semantic_search(conn: pymysql.connections.Connection | None = None, refresh_embeddings: bool = False) -> dict[str, int]:
    invalidate_knowledge_cache()
    owns_conn = conn is None
    if owns_conn:
        conn = get_db_conn()
    assert conn is not None

    entities = _load_semantic_entities(conn)
    column_map = entities["column_map"]
    docs: list[dict[str, Any]] = []

    for table in entities["tables"]:
        key_fields = []
        for column in column_map.get(table["table_name"], [])[:10]:
            key_fields.append(f"{column['column_name']}({column.get('business_name') or column.get('column_comment') or column['column_name']})")
        search_text = _join_search_lines(
            [
                table["table_name"],
                table["business_name"],
                table.get("table_role", ""),
                table.get("description", ""),
                table.get("table_comment", ""),
                _join_search_terms(table.get("keywords", [])),
                _join_search_terms(table.get("business_dimensions", [])),
                _join_search_terms(table.get("business_metrics", [])),
                _join_search_terms(key_fields),
            ]
        )
        docs.append(
            {
                "source_type": "table",
                "source_key": table["table_name"],
                "source_name": table["business_name"],
                "domain_key": table["domain_key"],
                "related_tables": [table["table_name"]],
                "related_metrics": table.get("business_metrics", []),
                "related_dimensions": table.get("business_dimensions", []),
                "priority_score": table["priority_score"],
                "search_text": search_text,
                "payload": {
                    "table_name": table["table_name"],
                    "table_role": table.get("table_role"),
                    "description": table.get("description", ""),
                    "keywords": table.get("keywords", []),
                },
            }
        )

    for metric in entities["metrics"]:
        search_text = _join_search_lines(
            [
                metric["metric_code"],
                metric["metric_name"],
                metric.get("definition_name", ""),
                metric.get("description", ""),
                metric.get("default_expression", ""),
                metric.get("default_filters", ""),
                _join_search_terms(metric.get("keywords", [])),
                _join_search_terms(metric.get("related_tables", [])),
            ]
        )
        docs.append(
            {
                "source_type": "metric",
                "source_key": metric["metric_code"],
                "source_name": metric["metric_name"],
                "domain_key": metric["domain_key"],
                "related_tables": metric.get("related_tables", []),
                "related_metrics": [metric["metric_name"]],
                "related_dimensions": [],
                "priority_score": metric["priority_score"],
                "search_text": search_text,
                "payload": metric,
            }
        )

    for dimension in entities["dimensions"]:
        search_text = _join_search_lines(
            [
                dimension["dimension_code"],
                dimension["dimension_name"],
                dimension.get("description", ""),
                dimension.get("source_expression", ""),
                _join_search_terms(dimension.get("keywords", [])),
                _join_search_terms(dimension.get("related_tables", [])),
            ]
        )
        docs.append(
            {
                "source_type": "dimension",
                "source_key": dimension["dimension_code"],
                "source_name": dimension["dimension_name"],
                "domain_key": dimension["domain_key"],
                "related_tables": dimension.get("related_tables", []),
                "related_metrics": [],
                "related_dimensions": [dimension["dimension_name"]],
                "priority_score": dimension["priority_score"],
                "search_text": search_text,
                "payload": dimension,
            }
        )

    for join in entities["joins"]:
        search_text = _join_search_lines(
            [
                join["join_code"],
                join.get("description", ""),
                join.get("join_condition", ""),
                join["left_table"],
                join["right_table"],
                _join_search_terms(join.get("keywords", [])),
            ]
        )
        docs.append(
            {
                "source_type": "join",
                "source_key": join["join_code"],
                "source_name": join.get("description") or join["join_code"],
                "domain_key": join["domain_key"],
                "related_tables": [join["left_table"], join["right_table"]],
                "related_metrics": [],
                "related_dimensions": [],
                "priority_score": join["priority_score"],
                "search_text": search_text,
                "payload": join,
            }
        )

    for synonym in entities["synonyms"]:
        search_text = _join_search_lines(
            [
                synonym["standard_name"],
                synonym["synonym_term"],
                synonym["target_type"],
                synonym["target_key"],
                _join_search_terms(synonym.get("related_tables", [])),
            ]
        )
        docs.append(
            {
                "source_type": "synonym",
                "source_key": str(synonym["id"]),
                "source_name": synonym["synonym_term"],
                "domain_key": None,
                "related_tables": synonym.get("related_tables", []),
                "related_metrics": [synonym["standard_name"]] if synonym["target_type"] == "metric" else [],
                "related_dimensions": [synonym["standard_name"]] if synonym["target_type"] == "dimension" else [],
                "priority_score": synonym["weight_score"],
                "search_text": search_text,
                "payload": synonym,
            }
        )

    for example in entities["examples"]:
        search_text = _join_search_lines(
            [
                example["question_text"],
                example.get("summary_text", ""),
                _join_search_terms(example.get("related_tables", [])),
                _join_search_terms(example.get("related_metrics", [])),
                _join_search_terms(example.get("related_dimensions", [])),
                example.get("sql_example", ""),
            ]
        )
        docs.append(
            {
                "source_type": "example",
                "source_key": example["example_key"],
                "source_name": example["question_text"][:80],
                "domain_key": example["domain_key"],
                "related_tables": example.get("related_tables", []),
                "related_metrics": example.get("related_metrics", []),
                "related_dimensions": example.get("related_dimensions", []),
                "priority_score": example["priority_score"],
                "search_text": search_text,
                "payload": example,
            }
        )

    active_keys = {(doc["source_type"], doc["source_key"]) for doc in docs}
    with conn.cursor() as cursor:
        for doc in docs:
            payload_json = json.dumps(doc["payload"], ensure_ascii=False, default=str)
            search_text = doc["search_text"]
            content_hash = _content_hash(search_text)
            cursor.execute(
                "SELECT `content_hash` FROM `semantic_search_doc` WHERE `source_type` = %s AND `source_key` = %s",
                (doc["source_type"], doc["source_key"]),
            )
            existing = cursor.fetchone()
            if existing and existing["content_hash"] == content_hash:
                cursor.execute(
                    """
                    UPDATE `semantic_search_doc`
                    SET `source_name` = %s,
                        `domain_key` = %s,
                        `related_tables_json` = %s,
                        `related_metrics_json` = %s,
                        `related_dimensions_json` = %s,
                        `priority_score` = %s,
                        `payload_json` = %s,
                        `is_active` = 1,
                        `updated_at` = NOW()
                    WHERE `source_type` = %s AND `source_key` = %s
                    """,
                    (
                        doc["source_name"],
                        doc["domain_key"],
                        _json_dumps(doc["related_tables"]),
                        _json_dumps(doc["related_metrics"]),
                        _json_dumps(doc["related_dimensions"]),
                        doc["priority_score"],
                        payload_json,
                        doc["source_type"],
                        doc["source_key"],
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO `semantic_search_doc` (
                        `source_type`, `source_key`, `source_name`, `domain_key`,
                        `related_tables_json`, `related_metrics_json`, `related_dimensions_json`,
                        `priority_score`, `search_text`, `payload_json`, `content_hash`,
                        `embedding_json`, `embedding_model`, `embedding_status`, `is_active`
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, NULL, 'pending', 1)
                    ON DUPLICATE KEY UPDATE
                        `source_name` = VALUES(`source_name`),
                        `domain_key` = VALUES(`domain_key`),
                        `related_tables_json` = VALUES(`related_tables_json`),
                        `related_metrics_json` = VALUES(`related_metrics_json`),
                        `related_dimensions_json` = VALUES(`related_dimensions_json`),
                        `priority_score` = VALUES(`priority_score`),
                        `search_text` = VALUES(`search_text`),
                        `payload_json` = VALUES(`payload_json`),
                        `content_hash` = VALUES(`content_hash`),
                        `embedding_json` = NULL,
                        `embedding_model` = NULL,
                        `embedding_status` = 'pending',
                        `is_active` = 1,
                        `updated_at` = NOW()
                    """,
                    (
                        doc["source_type"],
                        doc["source_key"],
                        doc["source_name"],
                        doc["domain_key"],
                        _json_dumps(doc["related_tables"]),
                        _json_dumps(doc["related_metrics"]),
                        _json_dumps(doc["related_dimensions"]),
                        doc["priority_score"],
                        search_text,
                        payload_json,
                        content_hash,
                    ),
                )

        cursor.execute("SELECT `source_type`, `source_key` FROM `semantic_search_doc`")
        stored_keys = {(row["source_type"], row["source_key"]) for row in cursor.fetchall()}
        stale_keys = stored_keys - active_keys
        if stale_keys:
            cursor.executemany(
                "UPDATE `semantic_search_doc` SET `is_active` = 0, `updated_at` = NOW() WHERE `source_type` = %s AND `source_key` = %s",
                list(stale_keys),
            )
    conn.commit()

    embedding_count = 0
    if refresh_embeddings:
        embedding_count = refresh_pending_embeddings(conn=conn)

    if owns_conn:
        conn.close()
    return {"docs": len(docs), "embeddings": embedding_count}


def refresh_pending_embeddings(conn: pymysql.connections.Connection | None = None, limit: int = 300) -> int:
    if not DASHSCOPE_API_KEY:
        return 0
    owns_conn = conn is None
    if owns_conn:
        conn = get_db_conn()
    assert conn is not None

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT `id`, `search_text`
            FROM `semantic_search_doc`
            WHERE `is_active` = 1
              AND (`embedding_status` = 'pending' OR `embedding_json` IS NULL OR `embedding_json` = '')
            ORDER BY `priority_score` DESC, `id` ASC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cursor.fetchall()

    if not rows:
        if owns_conn:
            conn.close()
        return 0

    batch_size = 1
    updated = 0
    for index in range(0, len(rows), batch_size):
        batch = rows[index:index + batch_size]
        embeddings = _embed_texts([row["search_text"] for row in batch])
        with conn.cursor() as cursor:
            for row, embedding in zip(batch, embeddings, strict=False):
                cursor.execute(
                    """
                    UPDATE `semantic_search_doc`
                    SET `embedding_json` = %s,
                        `embedding_model` = %s,
                        `embedding_status` = 'ready',
                        `updated_at` = NOW()
                    WHERE `id` = %s
                    """,
                    (json.dumps(embedding), _resolve_embedding_model_name(), row["id"]),
                )
                updated += 1
        conn.commit()

    if owns_conn:
        conn.close()
    return updated


def ensure_semantic_runtime(refresh_embeddings: bool = False) -> None:
    global SEMANTIC_RUNTIME_READY
    if SEMANTIC_RUNTIME_READY and not refresh_embeddings:
        return
    with get_db_conn() as conn:
        ensure_knowledge_runtime(conn)
        with conn.cursor() as cursor:
            for ddl in DDL_STATEMENTS:
                cursor.execute(ddl)
            _ensure_fulltext_index(cursor)
        conn.commit()
        _seed_defaults(conn)
        conn.commit()
        sync_semantic_schema(conn)
        rebuild_semantic_search(conn, refresh_embeddings=refresh_embeddings)
    SEMANTIC_RUNTIME_READY = True


def _load_search_docs(conn: pymysql.connections.Connection) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT *
            FROM `semantic_search_doc`
            WHERE `is_active` = 1
            ORDER BY `priority_score` DESC, `id` ASC
            """
        )
        docs = list(cursor.fetchall())
    for doc in docs:
        doc["related_tables"] = _json_loads(doc.pop("related_tables_json", None))
        doc["related_metrics"] = _json_loads(doc.pop("related_metrics_json", None))
        doc["related_dimensions"] = _json_loads(doc.pop("related_dimensions_json", None))
        payload_json = doc.pop("payload_json", None)
        try:
            doc["payload"] = json.loads(payload_json) if payload_json else {}
        except json.JSONDecodeError:
            doc["payload"] = {}
        try:
            doc["embedding"] = json.loads(doc["embedding_json"]) if doc.get("embedding_json") else []
        except json.JSONDecodeError:
            doc["embedding"] = []
    return docs


def _fulltext_search(conn: pymysql.connections.Connection, text: str, limit: int = SEMANTIC_FULLTEXT_TOPK) -> list[dict[str, Any]]:
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT `source_type`, `source_key`,
                       MATCH(`search_text`) AGAINST (%s IN NATURAL LANGUAGE MODE) AS `ft_score`
                FROM `semantic_search_doc`
                WHERE `is_active` = 1
                  AND MATCH(`search_text`) AGAINST (%s IN NATURAL LANGUAGE MODE)
                ORDER BY `ft_score` DESC, `priority_score` DESC
                LIMIT %s
                """,
                (text, text, limit),
            )
            return list(cursor.fetchall())
    except pymysql.MySQLError:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT `source_type`, `source_key`,
                       CASE WHEN `search_text` LIKE %s THEN 1 ELSE 0 END AS `ft_score`
                FROM `semantic_search_doc`
                WHERE `is_active` = 1
                  AND `search_text` LIKE %s
                ORDER BY `priority_score` DESC
                LIMIT %s
                """,
                (f"%{text}%", f"%{text}%", limit),
            )
            return list(cursor.fetchall())


def _vector_search(question_text: str, docs: list[dict[str, Any]], limit: int = SEMANTIC_VECTOR_TOPK) -> list[dict[str, Any]]:
    if not DASHSCOPE_API_KEY:
        return []
    embeddings = _embed_texts([question_text])
    if not embeddings:
        return []
    query_embedding = embeddings[0]
    scored = []
    for doc in docs:
        vector = doc.get("embedding") or []
        if not vector:
            continue
        score = _cosine_similarity(query_embedding, vector)
        if score > 0:
            scored.append(
                {
                    "source_type": doc["source_type"],
                    "source_key": doc["source_key"],
                    "vector_score": score,
                }
            )
    scored.sort(key=lambda item: item["vector_score"], reverse=True)
    return scored[:limit]


def _build_join_graph(join_rows: list[dict[str, Any]]) -> tuple[dict[str, set[str]], dict[frozenset[str], dict[str, Any]]]:
    graph: dict[str, set[str]] = defaultdict(set)
    lookup: dict[frozenset[str], dict[str, Any]] = {}
    for join in join_rows:
        left_table = join["left_table"]
        right_table = join["right_table"]
        graph[left_table].add(right_table)
        graph[right_table].add(left_table)
        lookup[frozenset((left_table, right_table))] = join
    return graph, lookup


def _shortest_path(graph: dict[str, set[str]], start_table: str, end_table: str) -> list[str]:
    if start_table == end_table:
        return [start_table]
    queue: deque[list[str]] = deque([[start_table]])
    visited = {start_table}
    while queue:
        path = queue.popleft()
        node = path[-1]
        for neighbor in graph.get(node, set()):
            if neighbor in visited:
                continue
            next_path = path + [neighbor]
            if neighbor == end_table:
                return next_path
            visited.add(neighbor)
            queue.append(next_path)
    return []


def _expand_tables(base_tables: set[str], join_rows: list[dict[str, Any]]) -> set[str]:
    if not base_tables:
        return {"order_master"}
    graph, _lookup = _build_join_graph(join_rows)
    expanded = set(base_tables)
    tables = list(base_tables)
    for index, left_table in enumerate(tables):
        for right_table in tables[index + 1:]:
            path = _shortest_path(graph, left_table, right_table)
            expanded.update(path)
    if "product_info" in expanded and "order_detail" not in expanded:
        expanded.add("order_detail")
    if "order_detail" in expanded and "order_master" not in expanded:
        expanded.add("order_master")
    if any(table in expanded for table in {"user_info", "store_info"}) and "order_master" not in expanded and "refund_master" not in expanded:
        expanded.add("order_master")
    if "refund_detail" in expanded:
        expanded.update({"refund_master", "order_detail"})
    return expanded


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        normalized = _safe_text(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _extract_column_refs(text: str) -> dict[str, list[str]]:
    refs: dict[str, list[str]] = defaultdict(list)
    for table_name, column_name in COLUMN_REF_PATTERN.findall(str(text or "")):
        if column_name not in refs[table_name]:
            refs[table_name].append(column_name)
    return dict(refs)


def _merge_column_refs(target: dict[str, list[str]], source: dict[str, list[str]]) -> None:
    for table_name, columns in source.items():
        bucket = target.setdefault(table_name, [])
        for column_name in columns:
            if column_name not in bucket:
                bucket.append(column_name)


def _append_columns(target: dict[str, list[str]], table_name: str, columns: list[str]) -> None:
    bucket = target.setdefault(table_name, [])
    for column_name in columns:
        if column_name not in bucket:
            bucket.append(column_name)


def _get_dimension_distinct_values(dimension_code: str) -> list[str]:
    values: list[str] = []
    for table_name, column_name in DIMENSION_VALUE_SOURCES.get(dimension_code, []):
        for value in get_distinct_dimension_values(table_name, column_name):
            normalized_value = _safe_text(value)
            if len(normalized_value) < 2 or normalized_value in values:
                continue
            values.append(normalized_value)
            if len(values) >= 120:
                return values
    return values


def _extract_dimension_value_matches(question_text: str, selected_dimension_rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    normalized_question = _normalize_for_match(question_text)
    matches: dict[str, list[str]] = {}
    for dimension_row in selected_dimension_rows:
        matched_values: list[str] = []
        for value in _get_dimension_distinct_values(dimension_row.get("dimension_code", "")):
            if _normalize_for_match(value) in normalized_question and value not in matched_values:
                matched_values.append(value)
            if len(matched_values) >= MAX_MATCHED_DIMENSION_VALUES:
                break
        if matched_values:
            matches[dimension_row["dimension_code"]] = matched_values
    return matches


def _is_explicit_group_dimension(question_text: str, dimension_row: dict[str, Any]) -> bool:
    normalized_question = _normalize_for_match(question_text)
    terms = [dimension_row.get("dimension_name", ""), *dimension_row.get("keywords", [])]
    for term in terms:
        normalized_term = _normalize_for_match(term)
        if not normalized_term:
            continue
        if any(
            marker in normalized_question
            for marker in (
                f"按{normalized_term}",
                f"各{normalized_term}",
                f"不同{normalized_term}",
                f"{normalized_term}分组",
                f"{normalized_term}拆分",
                f"{normalized_term}展开",
                f"按{normalized_term}排序",
                f"按{normalized_term}降序",
            )
        ):
            return True
    return False


def _build_relevant_column_refs(
    selected_metric_rows: list[dict[str, Any]],
    selected_dimension_rows: list[dict[str, Any]],
    selected_join_rows: list[dict[str, Any]],
    selected_example_rows: list[dict[str, Any]],
    extra_sql_text: str = "",
) -> dict[str, list[str]]:
    refs: dict[str, list[str]] = {}
    for metric_row in selected_metric_rows:
        _merge_column_refs(refs, _extract_column_refs(metric_row.get("default_expression", "")))
    for dimension_row in selected_dimension_rows:
        _merge_column_refs(refs, _extract_column_refs(dimension_row.get("source_expression", "")))
    for join_row in selected_join_rows:
        _merge_column_refs(refs, _extract_column_refs(join_row.get("join_condition", "")))
    for example_row in selected_example_rows:
        _merge_column_refs(refs, _extract_column_refs(example_row.get("sql_example", "")))
    if extra_sql_text:
        _merge_column_refs(refs, _extract_column_refs(extra_sql_text))

    metric_codes = {row.get("metric_code", "") for row in selected_metric_rows}
    dimension_codes = {row.get("dimension_code", "") for row in selected_dimension_rows}

    if metric_codes.intersection({"sales_amount", "order_count", "avg_order_value", "refund_rate", "discount_amount", "discount_rate", "pay_buyer_count", "items_per_order"}):
        _append_columns(refs, "order_master", ["order_id", "created_at", "order_status"])
    if metric_codes.intersection({"sales_amount", "avg_order_value", "discount_amount", "discount_rate", "pay_buyer_count", "items_per_order"}):
        _append_columns(refs, "order_master", ["buyer_id", "paid_amount", "gross_amount", "discount_amount", "item_count", "sales_channel", "channel_type", "platform", "payment_method"])
        _append_columns(refs, "order_detail", ["order_id", "line_paid_amount", "line_gross_amount", "line_discount_amount", "quantity", "brand_name"])
    if metric_codes.intersection({"sales_volume", "gross_merchandise_amount"}):
        _append_columns(refs, "order_detail", ["order_id", "quantity", "line_paid_amount", "line_gross_amount"])
    if metric_codes.intersection({"refund_amount", "refund_count", "refund_rate", "refund_item_count"}):
        _append_columns(refs, "refund_master", ["refund_id", "order_id", "refund_amount", "refund_item_count", "refund_status", "refund_type", "applied_at"])
    if metric_codes.intersection({"refund_amount", "refund_rate", "refund_item_count"}) and "refund_detail" in PROMPT_FIELD_HINTS:
        _append_columns(refs, "refund_detail", ["refund_id", "order_detail_id", "product_id", "refund_amount", "refund_quantity"])
    if "user_count" in metric_codes:
        _append_columns(refs, "user_info", ["user_id"])
    if "avg_selling_price" in metric_codes:
        _append_columns(refs, "order_detail", ["quantity", "line_paid_amount", "product_id"])

    dimension_hint_map = {
        "sales_channel": ("order_master", ["sales_channel"]),
        "platform": ("order_master", ["platform"]),
        "payment_method": ("order_master", ["payment_method"]),
        "channel_type": ("order_master", ["channel_type"]),
        "store_province": ("store_info", ["province"]),
        "store_city": ("store_info", ["city"]),
        "receiver_province": ("order_master", ["receiver_province"]),
        "receiver_city": ("order_master", ["receiver_city"]),
        "brand_name": ("order_detail", ["brand_name"]),
        "product_name": ("order_detail", ["product_name"]),
        "member_level": ("user_info", ["member_level"]),
        "city_tier": ("user_info", ["city_tier"]),
        "register_channel": ("user_info", ["register_channel"]),
        "customer_tag": ("user_info", ["customer_tag"]),
        "device_type": ("user_info", ["device_type"]),
        "sales_region": ("store_info", ["sales_region"]),
        "store_name": ("store_info", ["store_name"]),
        "store_type": ("store_info", ["store_type"]),
        "org_level_1": ("store_info", ["org_level_1"]),
        "category_l1": ("order_detail", ["category_l1"]),
        "category_l2": ("order_detail", ["category_l2"]),
        "target_group": ("product_info", ["target_group"]),
        "temperature_zone": ("product_info", ["temperature_zone"]),
        "refund_reason": ("refund_master", ["refund_reason"]),
        "refund_type": ("refund_master", ["refund_type"]),
    }
    for dimension_code in dimension_codes:
        table_and_fields = dimension_hint_map.get(dimension_code)
        if table_and_fields:
            _append_columns(refs, table_and_fields[0], table_and_fields[1])

    return {table_name: _dedupe_keep_order(columns) for table_name, columns in refs.items()}


def _build_compact_table_prompt_lines(
    selected_table_rows: list[dict[str, Any]],
    column_map: dict[str, list[dict[str, Any]]],
    relevant_column_refs: dict[str, list[str]],
    prompt_mode: str,
) -> list[str]:
    prompt_lines: list[str] = []
    field_limit = 8 if prompt_mode == "repair" else 6
    for table_row in selected_table_rows:
        table_name = table_row["table_name"]
        if prompt_mode == "repair":
            prompt_lines.append(
                f"- {table_name}（{table_row['business_name']}，{table_row['table_role']}）"
            )
        else:
            prompt_lines.append(
                f"- {table_name}（{table_row['business_name']}，{table_row['table_role']}）：{table_row.get('description', '')}"
            )
        allowed_columns = {column["column_name"] for column in column_map.get(table_name, [])}
        needed_fields = [
            column_name
            for column_name in relevant_column_refs.get(table_name, [])
            if column_name in allowed_columns
        ]
        if not needed_fields:
            needed_fields = [
                column_name
                for column_name in PROMPT_FIELD_HINTS.get(table_name, [])
                if column_name in allowed_columns
            ]
        prompt_lines.append(f"  必要字段：{'、'.join(needed_fields[:field_limit])}")
    return prompt_lines


def _build_semantic_prompt_text(
    *,
    question: str,
    selected_metric_rows: list[dict[str, Any]],
    selected_dimension_rows: list[dict[str, Any]],
    selected_table_rows: list[dict[str, Any]],
    selected_join_rows: list[dict[str, Any]],
    selected_example_rows: list[dict[str, Any]],
    column_map: dict[str, list[dict[str, Any]]],
    prompt_mode: str,
    extra_sql_text: str = "",
) -> str:
    matched_dimension_values = _extract_dimension_value_matches(question, selected_dimension_rows)
    group_dimensions = [
        row for row in selected_dimension_rows
        if _is_explicit_group_dimension(question, row)
    ]
    if not group_dimensions and is_context_dependent_question(question) and selected_dimension_rows:
        group_dimensions = list(selected_dimension_rows)
    filter_dimensions = [
        row for row in selected_dimension_rows
        if row["dimension_code"] not in {item["dimension_code"] for item in group_dimensions}
        and (
            row["dimension_code"] in matched_dimension_values
            or any(_normalize_for_match(keyword) in _normalize_for_match(question) for keyword in row.get("keywords", []))
        )
    ]
    relevant_column_refs = _build_relevant_column_refs(
        selected_metric_rows,
        selected_dimension_rows,
        selected_join_rows,
        selected_example_rows if prompt_mode == "query" else [],
        extra_sql_text=extra_sql_text,
    )

    prompt_lines: list[str] = ["候选业务语义层：", "候选业务指标:"]
    for metric_row in selected_metric_rows:
        if prompt_mode == "repair":
            prompt_lines.append(
                f"- {metric_row['metric_name']}：表达式 {metric_row.get('default_expression', '')}；相关表：{'、'.join(metric_row.get('related_tables', []))}"
            )
        else:
            prompt_lines.append(
                f"- {metric_row['metric_name']}：{metric_row.get('description', '')}；表达式：{metric_row.get('default_expression', '')}"
            )

    if group_dimensions:
        prompt_lines.append("候选分组维度:")
        for dimension_row in group_dimensions:
            prompt_lines.append(
                f"- {dimension_row['dimension_name']}：表达式 {dimension_row.get('source_expression', '')}"
            )

    if filter_dimensions:
        prompt_lines.append("候选过滤维度:")
        for dimension_row in filter_dimensions:
            matched_values = matched_dimension_values.get(dimension_row["dimension_code"], [])
            matched_text = f"；识别值：{'、'.join(matched_values)}" if matched_values else ""
            prompt_lines.append(
                f"- {dimension_row['dimension_name']}：表达式 {dimension_row.get('source_expression', '')}{matched_text}"
            )

    prompt_lines.append("候选业务表:")
    prompt_lines.extend(
        _build_compact_table_prompt_lines(
            selected_table_rows,
            column_map,
            relevant_column_refs,
            prompt_mode,
        )
    )

    prompt_lines.append("候选关联关系:")
    for join_row in selected_join_rows:
        prompt_lines.append(f"- {join_row['join_condition']}")

    if prompt_mode == "query" and selected_example_rows:
        prompt_lines.append("候选相似问法:")
        for example in selected_example_rows[:1]:
            prompt_lines.append(
                f"- 问法：{example.get('question_text', '')}；说明：{example.get('summary_text', '')}"
            )

    prompt_lines.append("如果候选信息不足以安全回答当前问题，必须先澄清，不允许臆造字段或关联关系。")
    return "\n".join(prompt_lines)


def retrieve_semantic_context(
    question: str,
    history_messages: list[dict[str, str]],
    max_tables: int = 4,
    carryover_context: dict[str, Any] | None = None,
    prompt_mode: str = "query",
    extra_sql_text: str = "",
) -> dict[str, Any]:
    ensure_semantic_runtime()
    with get_db_conn() as conn:
        entities = _load_semantic_entities(conn)
        docs = _load_search_docs(conn)
        if any(doc.get("embedding_status") == "pending" and not doc.get("embedding") for doc in docs):
            refresh_pending_embeddings(conn, limit=10)
            docs = _load_search_docs(conn)

        recent_user_text = ""
        if is_context_dependent_question(question):
            recent_user_text = " ".join(
                message.get("content", "")
                for message in history_messages[-6:]
                if message.get("role") == "user"
            )
        merged_question = f"{recent_user_text} {question}".strip()
        normalized_question = _normalize_for_match(merged_question)

        score_map: dict[tuple[str, str], float] = defaultdict(float)
        doc_map = {(doc["source_type"], doc["source_key"]): doc for doc in docs}

        for synonym in entities["synonyms"]:
            synonym_term = _normalize_for_match(synonym["synonym_term"])
            if synonym_term and synonym_term in normalized_question:
                target = (synonym["target_type"], synonym["target_key"])
                score_map[target] += synonym["weight_score"]
                for doc in docs:
                    if synonym["standard_name"] and synonym["standard_name"] in doc.get("source_name", ""):
                        score_map[(doc["source_type"], doc["source_key"])] += synonym["weight_score"] / 2

        for doc in docs:
            source_name = _normalize_for_match(doc.get("source_name", ""))
            if source_name and source_name in normalized_question:
                score_map[(doc["source_type"], doc["source_key"])] += 8

        fulltext_rows = _fulltext_search(conn, merged_question)
        shortlist_keys: set[tuple[str, str]] = set(score_map.keys())
        for row in fulltext_rows:
            score_map[(row["source_type"], row["source_key"])] += float(row.get("ft_score") or 0) * 6
            shortlist_keys.add((row["source_type"], row["source_key"]))

        vector_docs = [doc for doc in docs if (doc["source_type"], doc["source_key"]) in shortlist_keys] or docs
        for row in _vector_search(merged_question, vector_docs):
            score_map[(row["source_type"], row["source_key"])] += float(row.get("vector_score") or 0) * 10

        scored_docs = []
        for key, score in score_map.items():
            doc = doc_map.get(key)
            if not doc:
                continue
            final_score = score + float(doc.get("priority_score") or 0) / 10
            scored_docs.append((doc, final_score))

        score_floor = 4.0
        if not scored_docs:
            for doc in docs:
                if doc["source_type"] == "table" and doc["source_key"] == "order_master":
                    scored_docs.append((doc, float(doc.get("priority_score") or 0) / 10))
                    break

        scored_docs.sort(key=lambda item: item[1], reverse=True)
        if scored_docs:
            score_floor = max(4.0, scored_docs[0][1] * 0.35)
            recall_docs = [item for item in scored_docs if item[1] >= score_floor] or scored_docs[:6]
            scored_docs = rerank_semantic_docs(
                question,
                recall_docs,
                carryover_context=carryover_context,
                top_k=SEMANTIC_RERANK_TOPK,
                top_n=SEMANTIC_RERANK_FINAL_N,
            )

        metric_lookup = {item["metric_code"]: item for item in entities["metrics"]}
        dimension_lookup = {item["dimension_code"]: item for item in entities["dimensions"]}
        table_lookup = {item["table_name"]: item for item in entities["tables"]}
        join_rows = entities["joins"]

        selected_table_names: set[str] = set()
        selected_metric_names: list[str] = []
        selected_dimension_names: list[str] = []

        top_metric_docs = [doc for doc, _score in scored_docs if doc["source_type"] == "metric"][:3]
        top_dimension_docs = [doc for doc, _score in scored_docs if doc["source_type"] == "dimension"][:3]
        include_table_docs = not top_metric_docs and not top_dimension_docs
        top_table_docs = [doc for doc, _score in scored_docs if doc["source_type"] == "table"][:max_tables] if include_table_docs else []
        top_example_docs = [doc for doc, score in scored_docs if doc["source_type"] == "example" and score >= score_floor + 3][:1]

        selected_example_rows: list[dict[str, Any]] = []

        top_metric_codes = [doc["source_key"] for doc in top_metric_docs]
        top_dimension_codes = [doc["source_key"] for doc in top_dimension_docs]
        selected_metric_rows = [metric_lookup[code] for code in top_metric_codes if code in metric_lookup][:3]
        selected_dimension_rows = [dimension_lookup[code] for code in top_dimension_codes if code in dimension_lookup][:3]

        def prune_related_tables(table_names: list[str], context_key: str = '') -> list[str]:
            tables = list(table_names or [])
            if context_key in {'brand_name', 'product_name', 'category_l1', 'category_l2', 'brand_example'} and 'order_detail' in tables:
                tables = [table_name for table_name in tables if table_name != 'product_info']
            return tables

        def append_metric_row(metric_row: dict[str, Any]) -> None:
            if metric_row["metric_name"] not in selected_metric_names:
                selected_metric_names.append(metric_row["metric_name"])
            if not any(existing["metric_code"] == metric_row["metric_code"] for existing in selected_metric_rows):
                selected_metric_rows.append(metric_row)
            selected_table_names.update(metric_row.get("related_tables", []))

        def append_dimension_row(dimension_row: dict[str, Any]) -> None:
            if dimension_row["dimension_name"] not in selected_dimension_names:
                selected_dimension_names.append(dimension_row["dimension_name"])
            if not any(existing["dimension_code"] == dimension_row["dimension_code"] for existing in selected_dimension_rows):
                selected_dimension_rows.append(dimension_row)
            selected_table_names.update(prune_related_tables(dimension_row.get("related_tables", []), dimension_row["dimension_code"]))

        for doc in top_table_docs:
            selected_table_names.update(doc.get("related_tables", []))

        explicit_metric_rows = sorted(
            [
                metric
                for metric in entities["metrics"]
                if _keyword_match_score(question, metric.get("keywords", [])) > 0
            ],
            key=lambda row: (
                _keyword_match_score(question, row.get("keywords", [])),
                row.get("priority_score", 0),
            ),
            reverse=True,
        )
        explicit_dimension_rows = sorted(
            [
                dimension
                for dimension in entities["dimensions"]
                if _keyword_match_score(question, dimension.get("keywords", [])) > 0
            ],
            key=lambda row: (
                _keyword_match_score(question, row.get("keywords", [])),
                row.get("priority_score", 0),
            ),
            reverse=True,
        )

        if (
            ('大区' in normalized_question or '销售大区' in normalized_question)
            and '省份' in normalized_question
            and '收货省份' not in normalized_question
        ):
            explicit_dimension_rows = [
                dimension for dimension in explicit_dimension_rows
                if dimension.get("dimension_code") != "receiver_province"
            ]
            store_province_row = next(
                (dimension for dimension in entities["dimensions"] if dimension.get("dimension_code") == "store_province"),
                None,
            )
            if store_province_row and not any(
                item.get("dimension_code") == "store_province" for item in explicit_dimension_rows
            ):
                explicit_dimension_rows.append(store_province_row)

        for metric_row in explicit_metric_rows[:3]:
            append_metric_row(metric_row)
        for dimension_row in explicit_dimension_rows[:3]:
            append_dimension_row(dimension_row)

        if carryover_context:
            carryover_metric_names = [str(item).strip() for item in carryover_context.get("metrics", []) if str(item).strip()]
            carryover_dimension_names = [str(item).strip() for item in carryover_context.get("dimensions", []) if str(item).strip()]
            for metric_row in entities["metrics"]:
                if metric_row["metric_name"] in carryover_metric_names:
                    append_metric_row(metric_row)
            for dimension_row in entities["dimensions"]:
                if dimension_row["dimension_name"] in carryover_dimension_names:
                    append_dimension_row(dimension_row)
            if any(token in normalized_question for token in ["河南", "江苏", "浙江", "广东", "北京", "上海", "四川", "福建", "山东", "湖北", "陕西", "重庆"]):
                receiver_province_row = next(
                    (row for row in entities["dimensions"] if row["dimension_code"] == "receiver_province"),
                    None,
                )
                if receiver_province_row:
                    append_dimension_row(receiver_province_row)

        selected_metric_name_set = {row["metric_name"] for row in selected_metric_rows}
        selected_dimension_name_set = {row["dimension_name"] for row in selected_dimension_rows}
        selected_group_dimension_name_set = {
            row["dimension_name"]
            for row in selected_dimension_rows
            if _is_explicit_group_dimension(question, row)
        }

        for doc in top_example_docs:
            payload = doc["payload"]
            example_metrics = set(payload.get("related_metrics", []))
            example_dimensions = set(payload.get("related_dimensions", []))
            if selected_metric_name_set or selected_dimension_name_set:
                metric_overlap = example_metrics.intersection(selected_metric_name_set)
                dimension_overlap = example_dimensions.intersection(selected_dimension_name_set)
                if selected_group_dimension_name_set and example_dimensions and not example_dimensions.intersection(selected_group_dimension_name_set):
                    continue
                extra_dimensions = example_dimensions - selected_dimension_name_set
                if selected_dimension_name_set and extra_dimensions and not is_context_dependent_question(question):
                    continue
                if not (metric_overlap or dimension_overlap):
                    continue
            selected_example_rows.append(payload)
            example_context_key = 'brand_example' if '品牌' in example_dimensions else ''
            selected_table_names.update(prune_related_tables(payload.get("related_tables", []), example_context_key))

        for metric_row in list(selected_metric_rows):
            append_metric_row(metric_row)

        for dimension_row in list(selected_dimension_rows):
            append_dimension_row(dimension_row)

        selected_metric_rows = selected_metric_rows[:3]
        selected_dimension_rows = selected_dimension_rows[:3]

        selected_table_names = _expand_tables(selected_table_names, join_rows)
        ordered_table_names = sorted(
            selected_table_names,
            key=lambda item: table_lookup.get(item, {}).get("priority_score", 0),
            reverse=True,
        )[:max_tables]
        selected_table_rows = [table_lookup[name] for name in ordered_table_names if name in table_lookup]

        selected_join_rows = [
            join_row
            for join_row in join_rows
            if join_row["left_table"] in selected_table_names and join_row["right_table"] in selected_table_names
        ]

        column_map = entities["column_map"]
        if not selected_metric_rows:
            selected_metric_rows = entities["metrics"][:3]
        if not selected_table_rows:
            selected_table_rows = [table_lookup.get('order_master', DEFAULT_TABLES[0])]

        selected_group_dimension_rows = [
            row for row in selected_dimension_rows
            if _is_explicit_group_dimension(question, row)
        ]
        if not selected_group_dimension_rows and is_context_dependent_question(question) and selected_dimension_rows:
            selected_group_dimension_rows = list(selected_dimension_rows)
        selected_filter_dimension_rows = [
            row for row in selected_dimension_rows
            if row["dimension_code"] not in {item["dimension_code"] for item in selected_group_dimension_rows}
        ]

        knowledge_context = retrieve_knowledge_context(
            question,
            [row["table_name"] for row in selected_table_rows],
            [row["metric_name"] for row in selected_metric_rows],
            [row["dimension_name"] for row in selected_dimension_rows],
            top_n=KNOWLEDGE_CONTEXT_TOPN,
        )

        base_prompt_text = _build_semantic_prompt_text(
            question=question,
            selected_metric_rows=selected_metric_rows,
            selected_dimension_rows=selected_dimension_rows,
            selected_table_rows=selected_table_rows,
            selected_join_rows=selected_join_rows,
            selected_example_rows=selected_example_rows,
            column_map=column_map,
            prompt_mode="query",
        )
        prompt_text = base_prompt_text
        if knowledge_context.get("prompt_text"):
            prompt_text = f"{prompt_text}\n\n本地结构化知识层:\n{knowledge_context['prompt_text']}"
        base_repair_prompt_text = _build_semantic_prompt_text(
            question=question,
            selected_metric_rows=selected_metric_rows,
            selected_dimension_rows=selected_dimension_rows,
            selected_table_rows=selected_table_rows,
            selected_join_rows=selected_join_rows,
            selected_example_rows=[],
            column_map=column_map,
            prompt_mode="repair" if prompt_mode == "repair" else "query",
            extra_sql_text=extra_sql_text,
        )
        repair_prompt_text = base_repair_prompt_text
        if knowledge_context.get("prompt_text"):
            repair_prompt_text = f"{repair_prompt_text}\n\n本地结构化知识层:\n{knowledge_context['prompt_text']}"

        return {
            "candidate_tables": [row["table_name"] for row in selected_table_rows],
            "candidate_metrics": [row["metric_name"] for row in selected_metric_rows] or selected_metric_names,
            "candidate_dimensions": [row["dimension_name"] for row in selected_dimension_rows] or selected_dimension_names,
            "candidate_dimension_rules": [
                {
                    "dimension_code": row.get("dimension_code", ""),
                    "dimension_name": row.get("dimension_name", ""),
                    "source_expression": row.get("source_expression", ""),
                }
                for row in selected_dimension_rows
            ],
            "candidate_group_dimension_rules": [
                {
                    "dimension_code": row.get("dimension_code", ""),
                    "dimension_name": row.get("dimension_name", ""),
                    "source_expression": row.get("source_expression", ""),
                }
                for row in selected_group_dimension_rows
            ],
            "candidate_filter_dimension_rules": [
                {
                    "dimension_code": row.get("dimension_code", ""),
                    "dimension_name": row.get("dimension_name", ""),
                    "source_expression": row.get("source_expression", ""),
                }
                for row in selected_filter_dimension_rows
            ],
            "candidate_metric_rules": [
                {
                    "metric_code": row.get("metric_code", ""),
                    "metric_name": row.get("metric_name", ""),
                    "default_expression": row.get("default_expression", ""),
                }
                for row in selected_metric_rows
            ],
            "candidate_dimension_rules": [
                {
                    "dimension_code": row.get("dimension_code", ""),
                    "dimension_name": row.get("dimension_name", ""),
                    "source_expression": row.get("source_expression", ""),
                }
                for row in selected_dimension_rows
            ],
            "candidate_joins": selected_join_rows,
            "candidate_examples": [example.get("question_text", "") for example in selected_example_rows],
            "knowledge_context": knowledge_context,
            "base_prompt_text": base_prompt_text,
            "base_repair_prompt_text": base_repair_prompt_text,
            "prompt_text": prompt_text,
            "repair_prompt_text": repair_prompt_text,
        }


def list_admin_entity(entity: str) -> list[dict[str, Any]]:
    ensure_semantic_runtime()
    config = ADMIN_ENTITY_CONFIG.get(entity)
    if not config:
        raise ValueError("不支持的语义实体类型")
    fields = ", ".join(f"`{field}`" for field in config["fields"])
    sql = f"SELECT {fields} FROM `{config['table']}` ORDER BY {config['order_by']}"
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = list(cursor.fetchall())
    for row in rows:
        for field in config.get("json_fields", []):
            row[field] = ", ".join(str(item) for item in _json_loads(row.get(field)))
        for key, value in list(row.items()):
            if hasattr(value, "isoformat"):
                row[key] = str(value)
    return rows


def get_admin_bootstrap() -> dict[str, Any]:
    ensure_semantic_runtime()
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            counts = {}
            for entity, config in ADMIN_ENTITY_CONFIG.items():
                cursor.execute(f"SELECT COUNT(*) AS cnt FROM `{config['table']}`")
                counts[entity] = cursor.fetchone()["cnt"]
            cursor.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM `semantic_search_doc`
                WHERE `is_active` = 1
                  AND (`embedding_status` = 'pending' OR `embedding_json` IS NULL OR `embedding_json` = '')
                """
            )
            pending_embeddings = cursor.fetchone()["cnt"]
        data_quality = get_latest_data_quality_summary(conn)
    latest_rebuild_task = get_latest_task_by_type(TASK_TYPE_SEMANTIC_REBUILD) or {}
    latest_rebuild_result = latest_rebuild_task.get('result') if latest_rebuild_task else {}
    latest_rebuild = {
        'task_id': latest_rebuild_task.get('task_id', ''),
        'display_name': latest_rebuild_task.get('display_name', ''),
        'status': latest_rebuild_task.get('status', ''),
        'progress': latest_rebuild_task.get('progress', 0),
        'created_at': latest_rebuild_task.get('created_at', ''),
        'started_at': latest_rebuild_task.get('started_at', ''),
        'finished_at': latest_rebuild_task.get('finished_at', ''),
        'docs': (latest_rebuild_result.get('result') or {}).get('docs')
        if isinstance(latest_rebuild_result, dict) and isinstance(latest_rebuild_result.get('result'), dict)
        else latest_rebuild_result.get('docs') if isinstance(latest_rebuild_result, dict) else None,
        'embeddings': (latest_rebuild_result.get('result') or {}).get('embeddings')
        if isinstance(latest_rebuild_result, dict) and isinstance(latest_rebuild_result.get('result'), dict)
        else latest_rebuild_result.get('embeddings') if isinstance(latest_rebuild_result, dict) else None,
        'step': latest_rebuild_result.get('step', '') if isinstance(latest_rebuild_result, dict) else '',
        'error_message': latest_rebuild_task.get('error_message', ''),
    }
    payload = {
        "overview": {"counts": counts, "pending_embeddings": pending_embeddings},
        "latest_rebuild": latest_rebuild,
        "query_plan_quality": get_query_plan_quality_stats(limit=200),
        "data_quality": data_quality,
    }
    for entity in ADMIN_ENTITY_CONFIG:
        payload[entity] = list_admin_entity(entity)
    return payload


def upsert_admin_entity(entity: str, payload: dict[str, Any], *, rebuild: bool = True) -> None:
    ensure_semantic_runtime()
    config = ADMIN_ENTITY_CONFIG.get(entity)
    if not config or config.get("read_only"):
        raise ValueError("当前实体不支持维护")

    row = dict(payload)
    for field in config.get("json_fields", []):
        source_field = field[:-5] if field.endswith("_json") else field
        row[field] = _json_dumps(row.get(field, row.get(source_field)))
    if "priority_score" in row:
        row["priority_score"] = int(row.get("priority_score") or 0)
    if "weight_score" in row:
        row["weight_score"] = int(row.get("weight_score") or 0)
    if "is_active" in row:
        row["is_active"] = _bool_int(row.get("is_active"))

    key_field = config["key_field"]
    fields = [field for field in config["fields"] if field != "id" or row.get("id")]
    if config.get("auto_increment") and not row.get(key_field):
        insert_fields = [field for field in fields if field != key_field]
        placeholders = ", ".join(["%s"] * len(insert_fields))
        sql = f"INSERT INTO `{config['table']}` ({', '.join(f'`{field}`' for field in insert_fields)}) VALUES ({placeholders})"
        values = tuple(row.get(field) for field in insert_fields)
    else:
        non_key_fields = [field for field in fields if field != key_field]
        sql = (
            f"INSERT INTO `{config['table']}` ({', '.join(f'`{field}`' for field in fields)}) "
            f"VALUES ({', '.join(['%s'] * len(fields))}) "
            f"ON DUPLICATE KEY UPDATE {', '.join(f'`{field}` = VALUES(`{field}`)' for field in non_key_fields)}"
        )
        values = tuple(row.get(field) for field in fields)

    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, values)
        conn.commit()
        if rebuild:
            rebuild_semantic_search(conn, refresh_embeddings=False)
            conn.commit()


def delete_admin_entity(entity: str, payload: dict[str, Any]) -> None:
    ensure_semantic_runtime()
    config = ADMIN_ENTITY_CONFIG.get(entity)
    if not config or config.get("read_only"):
        raise ValueError("当前实体不支持删除")
    key_field = config["key_field"]
    key_value = payload.get(key_field)
    if key_value in (None, ""):
        raise ValueError("缺少主键，无法删除")
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM `{config['table']}` WHERE `{key_field}` = %s",
                (key_value,),
            )
        conn.commit()
        rebuild_semantic_search(conn, refresh_embeddings=False)
        conn.commit()


def rebuild_admin_search(refresh_embeddings: bool = False) -> dict[str, int]:
    ensure_semantic_runtime()
    with get_db_conn() as conn:
        sync_semantic_schema(conn)
        result = rebuild_semantic_search(conn, refresh_embeddings=refresh_embeddings)
        conn.commit()
        return result


def sync_builtin_semantic_knowledge(refresh_embeddings: bool = False) -> dict[str, int]:
    ensure_semantic_runtime()
    synonym_id_lookup: dict[tuple[str, str, str], int] = {}
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT `id`, `target_type`, `target_key`, `synonym_term` FROM `semantic_synonym`")
            for row in cursor.fetchall():
                synonym_id_lookup[
                    (
                        _safe_text(row.get("target_type")),
                        _safe_text(row.get("target_key")),
                        _safe_text(row.get("synonym_term")),
                    )
                ] = int(row.get("id"))

    builtin_entities = [
        ("domains", DEFAULT_DOMAINS),
        ("tables", DEFAULT_TABLES),
        ("metrics", DEFAULT_METRICS),
        ("dimensions", DEFAULT_DIMENSIONS),
        ("joins", DEFAULT_JOINS),
        ("synonyms", DEFAULT_SYNONYMS),
        ("examples", DEFAULT_EXAMPLES),
    ]
    for entity, rows in builtin_entities:
        for row in rows:
            payload = dict(row)
            if entity == "synonyms":
                synonym_id = synonym_id_lookup.get(
                    (
                        _safe_text(payload.get("target_type")),
                        _safe_text(payload.get("target_key")),
                        _safe_text(payload.get("synonym_term")),
                    )
                )
                if synonym_id:
                    payload["id"] = synonym_id
            upsert_admin_entity(entity, payload, rebuild=False)
    return rebuild_admin_search(refresh_embeddings=refresh_embeddings)


def get_semantic_maintenance_guide() -> dict[str, list[str]]:
    return {
        "steps": [
            "先在后台维护页修改业务域、业务表、指标、维度、关联关系、同义词或问法示例。",
            "如果业务表结构有变化，先点击“同步业务表结构”，把真实表字段备注同步到 semantic_column。",
            "系统启动和初始化造数时会自动执行一轮原始数据质量巡检，补齐省市编码、替换通用占位区县并修正常见脏值。",
            "修改完成后点击“一键刷新重建”，系统会同步重建检索索引并刷新有效文档的向量。",
            "维护指标时，related_tables 要填真实参与计算的表，default_expression 填标准口径表达式，description 填业务口径说明。",
            "维护维度时，source_expression 填默认分组字段或表达式，keywords 填常见自然语言别名。",
            "维护 join 时，join_condition 必须是可直接复制到 SQL 的真实关联条件。",
        ],
        "tables": [
            "semantic_domain：业务域定义。",
            "semantic_table：业务表语义定义，决定候选表召回。",
            "semantic_column：从 information_schema 同步的字段字典，建议只读维护。",
            "semantic_metric：指标定义和默认口径。",
            "semantic_dimension：维度定义和默认分组表达式。",
            "semantic_join：表与表的关联图。",
            "semantic_synonym：自然语言同义词映射。",
            "semantic_example：高质量问法示例。",
            "semantic_search_doc：全文索引和向量索引的物化文档。",
            "data_quality_run / data_quality_issue：原始数据质量巡检结果和问题明细。",
        ],
    }
