KNOWLEDGE_METRIC_DICT_DDL = """
CREATE TABLE IF NOT EXISTS `knowledge_metric_dict` (
    `metric_key` VARCHAR(64) NOT NULL COMMENT '指标编码',
    `metric_name` VARCHAR(128) NOT NULL COMMENT '指标名称',
    `business_definition` LONGTEXT NULL COMMENT '业务定义',
    `calculation_rule` LONGTEXT NULL COMMENT '计算规则',
    `security_level` VARCHAR(8) NOT NULL DEFAULT 'S1' COMMENT '安全等级',
    `keywords_json` LONGTEXT NULL COMMENT '关键词JSON',
    `related_tables_json` LONGTEXT NULL COMMENT '相关表JSON',
    `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`metric_key`),
    KEY `idx_knowledge_metric_active` (`is_active`, `security_level`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='结构化知识层-指标字典';
"""

KNOWLEDGE_DIMENSION_DICT_DDL = """
CREATE TABLE IF NOT EXISTS `knowledge_dimension_dict` (
    `dimension_key` VARCHAR(64) NOT NULL COMMENT '维度编码',
    `dimension_name` VARCHAR(128) NOT NULL COMMENT '维度名称',
    `business_definition` LONGTEXT NULL COMMENT '业务定义',
    `business_scope` LONGTEXT NULL COMMENT '业务范围说明',
    `security_level` VARCHAR(8) NOT NULL DEFAULT 'S1' COMMENT '安全等级',
    `keywords_json` LONGTEXT NULL COMMENT '关键词JSON',
    `related_tables_json` LONGTEXT NULL COMMENT '相关表JSON',
    `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`dimension_key`),
    KEY `idx_knowledge_dimension_active` (`is_active`, `security_level`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='结构化知识层-维度字典';
"""

KNOWLEDGE_SYNONYM_DICT_DDL = """
CREATE TABLE IF NOT EXISTS `knowledge_synonym_dict` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `target_type` VARCHAR(32) NOT NULL COMMENT '目标类型',
    `target_key` VARCHAR(64) NOT NULL COMMENT '目标编码',
    `standard_term` VARCHAR(128) NOT NULL COMMENT '标准术语',
    `synonym_term` VARCHAR(128) NOT NULL COMMENT '同义词',
    `security_level` VARCHAR(8) NOT NULL DEFAULT 'S1' COMMENT '安全等级',
    `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_knowledge_synonym` (`target_type`, `target_key`, `synonym_term`),
    KEY `idx_knowledge_synonym_active` (`is_active`, `security_level`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='结构化知识层-同义词词典';
"""

KNOWLEDGE_JOIN_GRAPH_DDL = """
CREATE TABLE IF NOT EXISTS `knowledge_join_graph` (
    `join_key` VARCHAR(64) NOT NULL COMMENT '关系编码',
    `left_table` VARCHAR(64) NOT NULL COMMENT '左表',
    `right_table` VARCHAR(64) NOT NULL COMMENT '右表',
    `join_condition` LONGTEXT NOT NULL COMMENT '关联条件',
    `business_meaning` LONGTEXT NULL COMMENT '业务含义',
    `security_level` VARCHAR(8) NOT NULL DEFAULT 'S1' COMMENT '安全等级',
    `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`join_key`),
    KEY `idx_knowledge_join_active` (`is_active`, `security_level`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='结构化知识层-表关系图谱';
"""

KNOWLEDGE_FIELD_GLOSSARY_DDL = """
CREATE TABLE IF NOT EXISTS `knowledge_field_glossary` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `table_name` VARCHAR(64) NOT NULL COMMENT '表名',
    `column_name` VARCHAR(64) NOT NULL COMMENT '字段名',
    `business_name` VARCHAR(128) NOT NULL COMMENT '业务名称',
    `business_meaning` LONGTEXT NULL COMMENT '业务释义',
    `security_level` VARCHAR(8) NOT NULL DEFAULT 'S1' COMMENT '安全等级',
    `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_knowledge_field` (`table_name`, `column_name`),
    KEY `idx_knowledge_field_active` (`is_active`, `security_level`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='结构化知识层-字段业务释义';
"""

KNOWLEDGE_SQL_EXAMPLE_DDL = """
CREATE TABLE IF NOT EXISTS `knowledge_sql_example` (
    `example_key` VARCHAR(64) NOT NULL COMMENT '样例编码',
    `title` VARCHAR(255) NOT NULL COMMENT '样例标题',
    `question_text` LONGTEXT NOT NULL COMMENT '问法',
    `sql_text` LONGTEXT NOT NULL COMMENT '标准SQL',
    `quality_score` INT NOT NULL DEFAULT 80 COMMENT '质量分',
    `security_level` VARCHAR(8) NOT NULL DEFAULT 'S1' COMMENT '安全等级',
    `related_tables_json` LONGTEXT NULL COMMENT '相关表JSON',
    `related_metrics_json` LONGTEXT NULL COMMENT '相关指标JSON',
    `related_dimensions_json` LONGTEXT NULL COMMENT '相关维度JSON',
    `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`example_key`),
    KEY `idx_knowledge_example_active` (`is_active`, `security_level`, `quality_score`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='结构化知识层-标准SQL样例';
"""
