CHAT_SESSION_DDL = """
CREATE TABLE IF NOT EXISTS `chat_session` (
    `conversation_id` VARCHAR(80) NOT NULL COMMENT '会话ID',
    `title` VARCHAR(255) NULL COMMENT '会话标题',
    `latest_result_json` LONGTEXT NULL COMMENT '最近一次查询结果快照',
    `context_summary` LONGTEXT NULL COMMENT '滚动上下文摘要',
    `summary_message_count` INT NOT NULL DEFAULT 0 COMMENT '已压缩消息数',
    `last_compacted_message_id` BIGINT NULL COMMENT '最近一次压缩到的消息ID',
    `context_stats_json` LONGTEXT NULL COMMENT '上下文统计快照',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`conversation_id`),
    KEY `idx_chat_session_updated_at` (`updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ChatBI会话主表';
"""

CHAT_MESSAGE_DDL = """
CREATE TABLE IF NOT EXISTS `chat_message` (
    `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '消息ID',
    `conversation_id` VARCHAR(80) NOT NULL COMMENT '会话ID',
    `role` VARCHAR(20) NOT NULL COMMENT '角色',
    `content` LONGTEXT NOT NULL COMMENT '模型上下文内容',
    `display_content` LONGTEXT NULL COMMENT '页面展示内容',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`),
    KEY `idx_chat_message_conversation` (`conversation_id`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ChatBI会话消息表';
"""

ASYNC_TASK_DDL = """
CREATE TABLE IF NOT EXISTS `async_task` (
    `task_id` VARCHAR(80) NOT NULL COMMENT '任务ID',
    `task_type` VARCHAR(32) NOT NULL COMMENT '任务类型',
    `conversation_id` VARCHAR(80) NULL COMMENT '会话ID',
    `client_id` VARCHAR(80) NULL COMMENT '客户端ID',
    `display_name` VARCHAR(255) NOT NULL COMMENT '任务展示名称',
    `status` VARCHAR(32) NOT NULL DEFAULT 'pending' COMMENT '任务状态',
    `progress` INT NOT NULL DEFAULT 0 COMMENT '任务进度',
    `attempt_count` INT NOT NULL DEFAULT 0 COMMENT '执行次数',
    `worker_id` VARCHAR(120) NULL COMMENT '当前工作进程',
    `claim_token` VARCHAR(64) NULL COMMENT '抢占令牌',
    `lease_expires_at` DATETIME NULL COMMENT '租约过期时间',
    `payload_json` LONGTEXT NULL COMMENT '任务入参JSON',
    `result_json` LONGTEXT NULL COMMENT '任务结果JSON',
    `error_message` LONGTEXT NULL COMMENT '失败原因',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `started_at` DATETIME NULL COMMENT '开始时间',
    `finished_at` DATETIME NULL COMMENT '完成时间',
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`task_id`),
    KEY `idx_async_task_status` (`status`, `created_at`),
    KEY `idx_async_task_claim_token` (`claim_token`),
    KEY `idx_async_task_lease` (`status`, `lease_expires_at`),
    KEY `idx_async_task_client` (`client_id`, `created_at`),
    KEY `idx_async_task_conversation` (`conversation_id`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ChatBI异步任务表';
"""

LLM_INVOCATION_LOG_DDL = """
CREATE TABLE IF NOT EXISTS `llm_invocation_log` (
    `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '日志ID',
    `conversation_id` VARCHAR(80) NULL COMMENT '会话ID',
    `client_id` VARCHAR(80) NULL COMMENT '客户端ID',
    `request_id` VARCHAR(64) NULL COMMENT '请求链路ID',
    `round_no` INT NULL COMMENT '会话轮次',
    `stage` VARCHAR(64) NOT NULL COMMENT '调用阶段',
    `llm_provider` VARCHAR(32) NOT NULL COMMENT '模型引擎',
    `model_name` VARCHAR(128) NOT NULL COMMENT '模型名称',
    `request_json` LONGTEXT NOT NULL COMMENT '发送给模型的JSON',
    `response_json` LONGTEXT NULL COMMENT '模型返回结果',
    `error_message` LONGTEXT NULL COMMENT '错误信息',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`),
    KEY `idx_llm_log_conversation` (`conversation_id`, `created_at`),
    KEY `idx_llm_log_round` (`conversation_id`, `round_no`, `created_at`),
    KEY `idx_llm_log_client` (`client_id`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ChatBI模型调用日志';
"""

CHAT_SESSION_MIGRATIONS = {
    'title': "ALTER TABLE `chat_session` ADD COLUMN `title` VARCHAR(255) NULL COMMENT '会话标题' AFTER `conversation_id`",
    'latest_result_json': "ALTER TABLE `chat_session` ADD COLUMN `latest_result_json` LONGTEXT NULL COMMENT '最近一次查询结果快照' AFTER `title`",
    'context_summary': "ALTER TABLE `chat_session` ADD COLUMN `context_summary` LONGTEXT NULL COMMENT '滚动上下文摘要' AFTER `latest_result_json`",
    'summary_message_count': "ALTER TABLE `chat_session` ADD COLUMN `summary_message_count` INT NOT NULL DEFAULT 0 COMMENT '已压缩消息数' AFTER `context_summary`",
    'last_compacted_message_id': "ALTER TABLE `chat_session` ADD COLUMN `last_compacted_message_id` BIGINT NULL COMMENT '最近一次压缩到的消息ID' AFTER `summary_message_count`",
    'context_stats_json': "ALTER TABLE `chat_session` ADD COLUMN `context_stats_json` LONGTEXT NULL COMMENT '上下文统计快照' AFTER `last_compacted_message_id`",
}

CHAT_MESSAGE_MIGRATIONS = {
    'display_content': "ALTER TABLE `chat_message` ADD COLUMN `display_content` LONGTEXT NULL COMMENT '页面展示内容' AFTER `content`",
}

ASYNC_TASK_MIGRATIONS = {
    'attempt_count': "ALTER TABLE `async_task` ADD COLUMN `attempt_count` INT NOT NULL DEFAULT 0 COMMENT '执行次数' AFTER `progress`",
    'worker_id': "ALTER TABLE `async_task` ADD COLUMN `worker_id` VARCHAR(120) NULL COMMENT '当前工作进程' AFTER `attempt_count`",
    'claim_token': "ALTER TABLE `async_task` ADD COLUMN `claim_token` VARCHAR(64) NULL COMMENT '抢占令牌' AFTER `worker_id`",
    'lease_expires_at': "ALTER TABLE `async_task` ADD COLUMN `lease_expires_at` DATETIME NULL COMMENT '租约过期时间' AFTER `claim_token`",
}

LLM_INVOCATION_LOG_MIGRATIONS = {
    'request_id': "ALTER TABLE `llm_invocation_log` ADD COLUMN `request_id` VARCHAR(64) NULL COMMENT '请求链路ID' AFTER `client_id`",
    'round_no': "ALTER TABLE `llm_invocation_log` ADD COLUMN `round_no` INT NULL COMMENT '会话轮次' AFTER `request_id`",
}
