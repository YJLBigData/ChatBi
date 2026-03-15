from __future__ import annotations

import hashlib
import json
import math
import os
from collections import defaultdict, deque
from typing import Any

import pymysql
from dotenv import load_dotenv
from openai import OpenAI


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
SEMANTIC_VECTOR_TOPK = int(os.getenv("SEMANTIC_VECTOR_TOPK", "12"))
SEMANTIC_FULLTEXT_TOPK = int(os.getenv("SEMANTIC_FULLTEXT_TOPK", "12"))
SEMANTIC_RUNTIME_READY = False


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
]

DEFAULT_TABLES = [
    {
        "table_name": "order_master",
        "domain_key": "transaction",
        "business_name": "订单主表",
        "table_role": "事实表",
        "description": "记录订单主单级别的销售、支付、履约和收货信息，是订单金额、订单数、销售地区分析的主事实表。",
        "keywords": ["订单", "销售", "销售额", "销售金额", "GMV", "实付", "支付", "下单", "履约", "收货", "订单数", "客单价", "排名", "top", "前100", "地区销售"],
        "business_dimensions": ["销售渠道", "渠道类型", "订单状态", "支付方式", "收货省份", "收货城市", "下单日期", "完成日期"],
        "business_metrics": ["销售金额", "订单数", "退款金额", "客单价", "件单价"],
        "priority_score": 95,
        "is_active": 1,
    },
    {
        "table_name": "order_detail",
        "domain_key": "transaction",
        "business_name": "订单明细子表",
        "table_role": "事实表",
        "description": "记录订单行级商品明细，是商品销量、品牌销售、品类分析和SKU分析的核心事实表。",
        "keywords": ["商品", "产品", "sku", "明细", "品牌", "品类", "销量", "销售件数", "商品金额", "单品", "产品排名", "产品销售"],
        "business_dimensions": ["产品名称", "品牌", "一级品类", "二级品类", "销售渠道"],
        "business_metrics": ["销量", "商品金额", "销售金额", "件数"],
        "priority_score": 92,
        "is_active": 1,
    },
    {
        "table_name": "user_info",
        "domain_key": "user",
        "business_name": "用户信息维度表",
        "table_role": "维度表",
        "description": "提供用户属性和会员分层，用于性别、年龄、城市、注册渠道、会员等级等用户维度分析。",
        "keywords": ["用户", "会员", "人群", "性别", "年龄", "注册", "注册渠道", "标签", "城市等级", "母婴", "职业", "积分"],
        "business_dimensions": ["性别", "年龄", "常住省份", "常住城市", "会员等级", "注册渠道", "用户标签", "是否母婴人群"],
        "business_metrics": ["用户数", "会员销售金额", "新客销售金额"],
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
        "keywords": ["门店", "店铺", "大区", "销售大区", "组织", "区域", "华东", "华南", "华北", "华中", "西南", "西北", "省份", "城市", "河南", "江苏", "浙江", "广东", "湖北", "山东", "四川", "陕西", "北京", "上海", "渠道", "抖音", "京东", "天猫", "小程序", "线下", "社区团购", "O2O"],
        "business_dimensions": ["门店名称", "门店类型", "渠道名称", "渠道类型", "销售大区", "省份", "城市", "一级组织", "二级组织"],
        "business_metrics": ["门店销售金额", "门店订单数", "大区销售金额"],
        "priority_score": 91,
        "is_active": 1,
    },
    {
        "table_name": "refund_master",
        "domain_key": "refund",
        "business_name": "退款主表",
        "table_role": "事实表",
        "description": "记录退款申请、退款金额、退款状态和退款原因，用于售后和退款口径分析。",
        "keywords": ["退款", "退货", "售后", "退款金额", "退款单", "退款率", "售后金额", "售后单数", "退款原因"],
        "business_dimensions": ["退款状态", "退款类型", "退款原因", "退款申请日期"],
        "business_metrics": ["退款金额", "退款单数", "退款件数"],
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
        "description": "默认取退款主表 refund_amount 的汇总；分析退款商品时用退款明细表 refund_amount。",
        "default_expression": "SUM(refund_master.refund_amount) 或 SUM(refund_detail.refund_amount)",
        "default_filters": "未特别指定时默认统计退款成功和退款处理中记录。",
        "related_tables": ["refund_master", "refund_detail"],
        "keywords": ["退款金额", "退款额", "售后金额"],
        "priority_score": 94,
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
        "keywords": ["退款单数", "退款数", "售后单数"],
        "priority_score": 86,
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
]

DEFAULT_DIMENSIONS = [
    {"dimension_code": "sales_channel", "dimension_name": "销售渠道", "domain_key": "transaction", "description": "订单销售渠道，例如线下门店、天猫、京东、抖音。", "source_expression": "order_master.sales_channel", "related_tables": ["order_master", "order_detail"], "keywords": ["销售渠道", "渠道"], "priority_score": 90, "is_active": 1},
    {"dimension_code": "channel_type", "dimension_name": "渠道类型", "domain_key": "transaction", "description": "渠道所属类型，例如传统电商、兴趣电商、私域直营。", "source_expression": "order_master.channel_type 或 store_info.channel_type", "related_tables": ["order_master", "store_info"], "keywords": ["渠道类型"], "priority_score": 82, "is_active": 1},
    {"dimension_code": "sales_region", "dimension_name": "销售大区", "domain_key": "store", "description": "门店所在销售大区，例如华东大区、华南大区。", "source_expression": "store_info.sales_region", "related_tables": ["store_info", "order_master"], "keywords": ["销售大区", "大区", "华东", "华南", "华北", "华中", "西南", "西北"], "priority_score": 95, "is_active": 1},
    {"dimension_code": "store_name", "dimension_name": "门店名称", "domain_key": "store", "description": "门店或店铺名称。", "source_expression": "store_info.store_name", "related_tables": ["store_info", "order_master"], "keywords": ["门店", "店铺", "门店名称"], "priority_score": 88, "is_active": 1},
    {"dimension_code": "receiver_province", "dimension_name": "收货省份", "domain_key": "transaction", "description": "订单收货地址中的省份。", "source_expression": "order_master.receiver_province", "related_tables": ["order_master"], "keywords": ["省份", "收货省份", "地区", "河南", "江苏", "浙江", "广东"], "priority_score": 90, "is_active": 1},
    {"dimension_code": "receiver_city", "dimension_name": "收货城市", "domain_key": "transaction", "description": "订单收货地址中的城市。", "source_expression": "order_master.receiver_city", "related_tables": ["order_master"], "keywords": ["城市", "收货城市"], "priority_score": 82, "is_active": 1},
    {"dimension_code": "order_status", "dimension_name": "订单状态", "domain_key": "transaction", "description": "订单当前状态，仅可用中文状态值。", "source_expression": "order_master.order_status", "related_tables": ["order_master"], "keywords": ["订单状态", "已支付", "已完成", "已退款", "部分退款"], "priority_score": 84, "is_active": 1},
    {"dimension_code": "order_date", "dimension_name": "下单日期", "domain_key": "transaction", "description": "订单创建日期。", "source_expression": "DATE(order_master.created_at)", "related_tables": ["order_master"], "keywords": ["下单日期", "按天", "按日", "时间", "日期"], "priority_score": 87, "is_active": 1},
    {"dimension_code": "order_week", "dimension_name": "下单周", "domain_key": "transaction", "description": "订单创建所属周。", "source_expression": "YEARWEEK(order_master.created_at, 1)", "related_tables": ["order_master"], "keywords": ["按周", "周"], "priority_score": 76, "is_active": 1},
    {"dimension_code": "order_month", "dimension_name": "下单月", "domain_key": "transaction", "description": "订单创建所属月份。", "source_expression": "DATE_FORMAT(order_master.created_at, '%Y-%m')", "related_tables": ["order_master"], "keywords": ["按月", "月"], "priority_score": 78, "is_active": 1},
    {"dimension_code": "brand_name", "dimension_name": "品牌", "domain_key": "product", "description": "产品品牌名称。", "source_expression": "order_detail.brand_name 或 product_info.brand_name", "related_tables": ["order_detail", "product_info", "order_master"], "keywords": ["品牌", "特仑苏", "纯甄", "真果粒", "未来星", "冠益乳", "每日鲜语", "蒂兰圣雪", "蒙牛"], "priority_score": 96, "is_active": 1},
    {"dimension_code": "product_name", "dimension_name": "产品名称", "domain_key": "product", "description": "订单明细或产品维表中的产品名称。", "source_expression": "order_detail.product_name 或 product_info.product_name", "related_tables": ["order_detail", "product_info", "order_master"], "keywords": ["产品名称", "商品名称", "sku", "单品"], "priority_score": 89, "is_active": 1},
    {"dimension_code": "category_l1", "dimension_name": "一级品类", "domain_key": "product", "description": "产品一级品类。", "source_expression": "order_detail.category_l1 或 product_info.category_l1", "related_tables": ["order_detail", "product_info", "order_master"], "keywords": ["一级品类", "品类"], "priority_score": 86, "is_active": 1},
    {"dimension_code": "category_l2", "dimension_name": "二级品类", "domain_key": "product", "description": "产品二级品类。", "source_expression": "order_detail.category_l2 或 product_info.category_l2", "related_tables": ["order_detail", "product_info", "order_master"], "keywords": ["二级品类", "品类明细"], "priority_score": 84, "is_active": 1},
    {"dimension_code": "gender", "dimension_name": "性别", "domain_key": "user", "description": "用户性别。", "source_expression": "user_info.gender", "related_tables": ["user_info", "order_master"], "keywords": ["性别", "男", "女"], "priority_score": 88, "is_active": 1},
    {"dimension_code": "age", "dimension_name": "年龄", "domain_key": "user", "description": "用户年龄。", "source_expression": "user_info.age", "related_tables": ["user_info", "order_master"], "keywords": ["年龄", "18到25", "25到30"], "priority_score": 82, "is_active": 1},
    {"dimension_code": "member_level", "dimension_name": "会员等级", "domain_key": "user", "description": "用户会员等级。", "source_expression": "user_info.member_level", "related_tables": ["user_info", "order_master"], "keywords": ["会员等级", "金卡", "黑金", "新客"], "priority_score": 86, "is_active": 1},
    {"dimension_code": "refund_reason", "dimension_name": "退款原因", "domain_key": "refund", "description": "退款或售后的原因分类。", "source_expression": "refund_master.refund_reason 或 refund_detail.refund_reason", "related_tables": ["refund_master", "refund_detail"], "keywords": ["退款原因", "售后原因", "包装破损", "配送超时"], "priority_score": 80, "is_active": 1},
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
]

DEFAULT_SYNONYMS = [
    {"target_type": "metric", "target_key": "sales_amount", "standard_name": "销售金额", "synonym_term": "GMV", "related_tables": ["order_master", "order_detail"], "weight_score": 18, "is_active": 1},
    {"target_type": "metric", "target_key": "sales_amount", "standard_name": "销售金额", "synonym_term": "成交额", "related_tables": ["order_master", "order_detail"], "weight_score": 15, "is_active": 1},
    {"target_type": "metric", "target_key": "sales_amount", "standard_name": "销售金额", "synonym_term": "订单金额", "related_tables": ["order_master", "order_detail"], "weight_score": 14, "is_active": 1},
    {"target_type": "metric", "target_key": "order_count", "standard_name": "订单数", "synonym_term": "单量", "related_tables": ["order_master"], "weight_score": 14, "is_active": 1},
    {"target_type": "metric", "target_key": "sales_volume", "standard_name": "销量", "synonym_term": "件数", "related_tables": ["order_detail", "order_master"], "weight_score": 14, "is_active": 1},
    {"target_type": "metric", "target_key": "refund_amount", "standard_name": "退款金额", "synonym_term": "售后金额", "related_tables": ["refund_master", "refund_detail"], "weight_score": 14, "is_active": 1},
    {"target_type": "dimension", "target_key": "sales_region", "standard_name": "销售大区", "synonym_term": "大区", "related_tables": ["store_info", "order_master"], "weight_score": 12, "is_active": 1},
    {"target_type": "dimension", "target_key": "sales_region", "standard_name": "销售大区", "synonym_term": "区域", "related_tables": ["store_info", "order_master"], "weight_score": 10, "is_active": 1},
    {"target_type": "dimension", "target_key": "receiver_province", "standard_name": "收货省份", "synonym_term": "地区", "related_tables": ["order_master"], "weight_score": 8, "is_active": 1},
    {"target_type": "dimension", "target_key": "store_name", "standard_name": "门店名称", "synonym_term": "店铺", "related_tables": ["store_info", "order_master"], "weight_score": 10, "is_active": 1},
    {"target_type": "dimension", "target_key": "brand_name", "standard_name": "品牌", "synonym_term": "牌子", "related_tables": ["order_detail", "product_info", "order_master"], "weight_score": 8, "is_active": 1},
    {"target_type": "table", "target_key": "refund_master", "standard_name": "退款主表", "synonym_term": "售后主表", "related_tables": ["refund_master"], "weight_score": 8, "is_active": 1},
    {"target_type": "table", "target_key": "order_detail", "standard_name": "订单明细子表", "synonym_term": "订单子表", "related_tables": ["order_detail"], "weight_score": 10, "is_active": 1},
    {"target_type": "table", "target_key": "product_info", "standard_name": "产品信息维度表", "synonym_term": "商品表", "related_tables": ["product_info"], "weight_score": 8, "is_active": 1},
    {"target_type": "dimension", "target_key": "sales_channel", "standard_name": "销售渠道", "synonym_term": "平台", "related_tables": ["order_master", "order_detail"], "weight_score": 8, "is_active": 1},
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


def _bool_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    text = str(value or "").strip().lower()
    return 1 if text in {"1", "true", "yes", "y", "on"} else 0


def _normalize_for_match(text: str) -> str:
    return _safe_text(text).lower()


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


def _get_embedding_client() -> OpenAI | None:
    if not DASHSCOPE_API_KEY:
        return None
    return OpenAI(api_key=DASHSCOPE_API_KEY, base_url=DASHSCOPE_BASE_URL)


def _embed_texts(texts: list[str]) -> list[list[float]]:
    client = _get_embedding_client()
    if client is None or not texts:
        return []
    try:
        response = client.embeddings.create(model=DASHSCOPE_EMBEDDING_MODEL, input=texts)
    except Exception:  # noqa: BLE001
        return []
    return [item.embedding for item in response.data]


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
                  'store_info', 'refund_master', 'refund_detail'
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
                  'store_info', 'refund_master', 'refund_detail'
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
        search_text = "\n".join(
            [
                table["table_name"],
                table["business_name"],
                table.get("table_role", ""),
                table.get("description", ""),
                table.get("table_comment", ""),
                " ".join(table.get("keywords", [])),
                " ".join(table.get("business_dimensions", [])),
                " ".join(table.get("business_metrics", [])),
                " ".join(key_fields),
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
        search_text = "\n".join(
            [
                metric["metric_code"],
                metric["metric_name"],
                metric.get("definition_name", ""),
                metric.get("description", ""),
                metric.get("default_expression", ""),
                metric.get("default_filters", ""),
                " ".join(metric.get("keywords", [])),
                " ".join(metric.get("related_tables", [])),
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
        search_text = "\n".join(
            [
                dimension["dimension_code"],
                dimension["dimension_name"],
                dimension.get("description", ""),
                dimension.get("source_expression", ""),
                " ".join(dimension.get("keywords", [])),
                " ".join(dimension.get("related_tables", [])),
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
        search_text = "\n".join(
            [
                join["join_code"],
                join.get("description", ""),
                join.get("join_condition", ""),
                join["left_table"],
                join["right_table"],
                " ".join(join.get("keywords", [])),
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
        search_text = "\n".join(
            [
                synonym["standard_name"],
                synonym["synonym_term"],
                synonym["target_type"],
                synonym["target_key"],
                " ".join(synonym.get("related_tables", [])),
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
        search_text = "\n".join(
            [
                example["question_text"],
                example.get("summary_text", ""),
                " ".join(example.get("related_tables", [])),
                " ".join(example.get("related_metrics", [])),
                " ".join(example.get("related_dimensions", [])),
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
                    (json.dumps(embedding), DASHSCOPE_EMBEDDING_MODEL, row["id"]),
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


def retrieve_semantic_context(question: str, history_messages: list[dict[str, str]], max_tables: int = 5) -> dict[str, Any]:
    ensure_semantic_runtime()
    with get_db_conn() as conn:
        entities = _load_semantic_entities(conn)
        docs = _load_search_docs(conn)
        if any(doc.get("embedding_status") == "pending" and not doc.get("embedding") for doc in docs):
            refresh_pending_embeddings(conn, limit=10)
            docs = _load_search_docs(conn)

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

        if not scored_docs:
            for doc in docs:
                if doc["source_type"] == "table" and doc["source_key"] == "order_master":
                    scored_docs.append((doc, float(doc.get("priority_score") or 0) / 10))
                    break

        scored_docs.sort(key=lambda item: item[1], reverse=True)
        if scored_docs:
            score_floor = max(4.0, scored_docs[0][1] * 0.35)
            scored_docs = [item for item in scored_docs if item[1] >= score_floor] or scored_docs[:6]

        metric_lookup = {item["metric_code"]: item for item in entities["metrics"]}
        dimension_lookup = {item["dimension_code"]: item for item in entities["dimensions"]}
        table_lookup = {item["table_name"]: item for item in entities["tables"]}
        join_rows = entities["joins"]

        selected_table_names: set[str] = set()
        selected_metric_names: list[str] = []
        selected_dimension_names: list[str] = []

        top_table_docs = [doc for doc, _score in scored_docs if doc["source_type"] == "table"][:max_tables]
        top_metric_docs = [doc for doc, _score in scored_docs if doc["source_type"] == "metric"][:4]
        top_dimension_docs = [doc for doc, _score in scored_docs if doc["source_type"] == "dimension"][:4]
        top_example_docs = [doc for doc, _score in scored_docs if doc["source_type"] == "example"][:2]

        selected_example_rows: list[dict[str, Any]] = [doc["payload"] for doc in top_example_docs]

        for doc in top_table_docs + top_metric_docs + top_dimension_docs + top_example_docs:
            selected_table_names.update(doc.get("related_tables", []))
            for metric_name in doc.get("related_metrics", []):
                if metric_name and metric_name not in selected_metric_names:
                    selected_metric_names.append(metric_name)
            for dimension_name in doc.get("related_dimensions", []):
                if dimension_name and dimension_name not in selected_dimension_names:
                    selected_dimension_names.append(dimension_name)

        top_metric_codes = [doc["source_key"] for doc in top_metric_docs]
        top_dimension_codes = [doc["source_key"] for doc in top_dimension_docs]
        selected_metric_rows = [metric_lookup[code] for code in top_metric_codes if code in metric_lookup][:4]
        selected_dimension_rows = [dimension_lookup[code] for code in top_dimension_codes if code in dimension_lookup][:4]

        for metric_row in selected_metric_rows:
            if metric_row["metric_name"] not in selected_metric_names:
                selected_metric_names.append(metric_row["metric_name"])
            selected_table_names.update(metric_row.get("related_tables", []))

        for dimension_row in selected_dimension_rows:
            if dimension_row["dimension_name"] not in selected_dimension_names:
                selected_dimension_names.append(dimension_row["dimension_name"])
            selected_table_names.update(dimension_row.get("related_tables", []))

        selected_table_names = _expand_tables(selected_table_names, join_rows)
        ordered_table_names = sorted(
            selected_table_names,
            key=lambda item: table_lookup.get(item, {}).get("priority_score", 0),
            reverse=True,
        )[: max_tables + 2]
        selected_table_rows = [table_lookup[name] for name in ordered_table_names if name in table_lookup]

        selected_join_rows = [
            join_row
            for join_row in join_rows
            if join_row["left_table"] in selected_table_names and join_row["right_table"] in selected_table_names
        ]

        column_map = entities["column_map"]
        prompt_lines: list[str] = ["候选业务语义层："]
        prompt_lines.append("候选业务指标:")
        for metric_row in selected_metric_rows or entities["metrics"][:3]:
            prompt_lines.append(
                f"- {metric_row['metric_name']}：{metric_row.get('description', '')}；默认表达式：{metric_row.get('default_expression', '')}；相关表：{'、'.join(metric_row.get('related_tables', []))}"
            )

        prompt_lines.append("候选业务维度:")
        for dimension_row in selected_dimension_rows or entities["dimensions"][:4]:
            prompt_lines.append(
                f"- {dimension_row['dimension_name']}：{dimension_row.get('description', '')}；默认表达式：{dimension_row.get('source_expression', '')}；相关表：{'、'.join(dimension_row.get('related_tables', []))}"
            )

        prompt_lines.append("候选业务表:")
        for table_row in selected_table_rows or [table_lookup.get('order_master', DEFAULT_TABLES[0])]:
            key_fields = []
            for column in column_map.get(table_row["table_name"], [])[:12]:
                label = column.get("business_name") or column.get("column_comment") or column["column_name"]
                key_fields.append(f"{column['column_name']}({label})")
            prompt_lines.append(
                f"- {table_row['table_name']}（{table_row['business_name']}，{table_row['table_role']}）：{table_row.get('description', '')}"
            )
            prompt_lines.append(f"  常见维度：{'、'.join(table_row.get('business_dimensions', []))}")
            prompt_lines.append(f"  常见指标：{'、'.join(table_row.get('business_metrics', []))}")
            prompt_lines.append(f"  关键业务字段：{'、'.join(key_fields)}")

        prompt_lines.append("候选关联关系:")
        for join_row in selected_join_rows:
            prompt_lines.append(
                f"- {join_row['join_condition']}（{join_row.get('description', '')}）"
            )

        if selected_example_rows:
            prompt_lines.append("候选相似问法:")
            for example in selected_example_rows:
                prompt_lines.append(
                    f"- 问法：{example.get('question_text', '')}；说明：{example.get('summary_text', '')}；涉及表：{'、'.join(example.get('related_tables', []))}"
                )

        prompt_lines.append("如果候选表、候选维度和候选指标不足以安全回答当前问题，必须先澄清，不允许臆造字段或关联关系。")

        return {
            "candidate_tables": [row["table_name"] for row in selected_table_rows],
            "candidate_metrics": [row["metric_name"] for row in selected_metric_rows] or selected_metric_names,
            "candidate_dimensions": [row["dimension_name"] for row in selected_dimension_rows] or selected_dimension_names,
            "candidate_joins": selected_join_rows,
            "candidate_examples": [example.get("question_text", "") for example in selected_example_rows],
            "prompt_text": "\n".join(prompt_lines),
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
                "SELECT COUNT(*) AS cnt FROM `semantic_search_doc` WHERE `embedding_status` = 'pending' OR `embedding_json` IS NULL OR `embedding_json` = ''"
            )
            pending_embeddings = cursor.fetchone()["cnt"]
    payload = {"overview": {"counts": counts, "pending_embeddings": pending_embeddings}}
    for entity in ADMIN_ENTITY_CONFIG:
        payload[entity] = list_admin_entity(entity)
    return payload


def upsert_admin_entity(entity: str, payload: dict[str, Any]) -> None:
    ensure_semantic_runtime()
    config = ADMIN_ENTITY_CONFIG.get(entity)
    if not config or config.get("read_only"):
        raise ValueError("当前实体不支持维护")

    row = dict(payload)
    for field in config.get("json_fields", []):
        row[field] = _json_dumps(row.get(field))
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


def get_semantic_maintenance_guide() -> dict[str, list[str]]:
    return {
        "steps": [
            "先在后台维护页修改业务域、业务表、指标、维度、关联关系、同义词或问法示例。",
            "如果业务表结构有变化，先点击“同步业务表结构”，把真实表字段备注同步到 semantic_column。",
            "修改完成后点击“重建检索索引”；如果希望立即刷新向量召回，再点击“刷新向量索引”。",
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
        ],
    }
