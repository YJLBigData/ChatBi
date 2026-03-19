# ChatBI 架构说明

## 1. PPT 一页展示超简版

```text
用户提问
  ↓
前端交互层
  含义：自然语言提问、图表切换、下载和报告入口
  做了什么：输入问题、连续追问、展示列表/柱图/饼图、查看执行日志

  ↓
接入编排层
  含义：系统请求总入口
  做了什么：Flask API 接收请求，编排查询、导出、报告、后台维护

  ↓
会话与上下文层
  含义：管理连续对话和上下文记忆
  做了什么：会话持久化、结果快照保存、滚动摘要 + 最近窗口压缩

  ↓
语义层
  含义：先把问题映射到相关业务语义，而不是直接查全库
  做了什么：
  - 维护 semantic_domain / table / column / metric / dimension / join / synonym / example
  - 从 information_schema 同步真实字段
  - 构建 semantic_search_doc 检索文档
  - 用规则召回 + 同义词召回 + MySQL FULLTEXT + embedding 召回收敛候选语义
  当前技术：
  - embedding 模型：DashScope text-embedding-v4
  - 向量存储：MySQL semantic_search_doc.embedding_json
  - 相似度计算：Python 余弦相似度
  - 当前不是 pgvector，后续可升级为 pgvector / OpenSearch

  ↓
Prompt 组装层
  含义：把候选语义压缩成模型输入
  做了什么：只发送本轮相关的指标、维度、业务表、必要字段、必要 join、必要示例

  ↓
大模型查询规划层
  含义：理解问题并生成查询计划
  做了什么：调用阿里百炼 qwen3-max / DeepSeek，输出指标定义、指标描述、维度、指标、SQL、时间范围

  ↓
SQL 治理与执行层
  含义：保证 SQL 安全、兼容 MySQL、可稳定执行
  做了什么：
  - 只允许 SELECT / WITH
  - 白名单表校验
  - LIMIT 自动补全
  - MySQL 方言归一化
  - SQL 执行失败时自动进入 SQL 修复链路

  ↓
数据执行层
  含义：查询真实业务数据
  做了什么：查询订单、商品、用户、门店、退款等业务表，返回结果集

  ↓
结果输出层
  含义：把 SQL 结果变成业务可读内容
  做了什么：输出表格、图表、执行日志、SQL、指标定义、指标描述

  ↓
导出与报告层
  含义：把分析结果沉淀成文件和管理层材料
  做了什么：导出 CSV、导出图表 Word、生成商业分析报告 Word、维护模板与历史

  ↓
异步任务层
  含义：处理耗时任务
  做了什么：独立 worker 消费 async_task，处理报告生成和语义一键刷新重建

  ↓
日志与监控层
  含义：问题排查和质量监控
  做了什么：记录 Web/Worker 日志、LLM 调用日志、语义刷新结果、模型漏返字段统计
```

## 2. 一句话总结

这套系统不是“模型直接查数据库”，而是：

`用户问题 -> 会话上下文 -> 语义层收敛候选对象 -> Prompt 压缩 -> 大模型生成 SQL -> SQL 治理/修复 -> MySQL 查询 -> 图表/导出/报告输出`

## 3. 技术分层说明

### 3.1 前端交互层

- 含义：业务人员使用系统的入口。
- 做了什么：
  - 自然语言提问
  - 连续追问
  - 列表 / 柱图 / 饼图切换
  - 下载明细、下载图表、生成报告
  - 查看执行日志和任务状态

### 3.2 接入编排层

- 含义：后端请求总入口。
- 做了什么：
  - Flask 路由接收请求
  - 编排查询、导出、报告、后台管理
  - 统一返回 JSON / 文件流

### 3.3 会话与上下文层

- 含义：管理多轮对话上下文。
- 做了什么：
  - chat_session / chat_message 持久化
  - 最近结果快照保存
  - 独立问题 / 追问识别
  - 上下文压缩：滚动摘要 + 最近窗口

### 3.4 语义层

- 含义：把自然语言问题先转换成业务语义候选集。
- 核心表：
  - semantic_domain：业务域
  - semantic_table：业务表语义
  - semantic_column：字段字典
  - semantic_metric：指标口径
  - semantic_dimension：维度口径
  - semantic_join：关联关系
  - semantic_synonym：同义词
  - semantic_example：问法示例
  - semantic_search_doc：检索物化文档
- 做了什么：
  - 从真实库同步字段结构
  - 管理指标、维度、表关系、同义词、示例
  - 构建检索文档
  - 收敛候选指标、候选维度、候选表、候选 join、候选示例
- 当前技术点：
  - 检索：规则 + 同义词 + MySQL FULLTEXT + embedding
  - embedding 模型：DashScope `text-embedding-v4`
  - 向量存储：MySQL `semantic_search_doc.embedding_json`
  - 相似度：Python 余弦相似度
  - 当前不是 pgvector

### 3.5 Prompt 组装层

- 含义：把候选语义压缩成模型真正看到的内容。
- 做了什么：
  - 只发送本轮相关的指标、维度、表、必要字段、必要关联关系
  - 控制 prompt 长度，减少全量 schema 噪音

### 3.6 大模型查询规划层

- 含义：让模型理解问题并输出查询计划。
- 做了什么：
  - 调用阿里百炼 / DeepSeek
  - 输出 action、指标定义、指标描述、维度、指标、SQL、时间范围
  - 信息不足时返回 clarify

### 3.7 SQL 治理与执行层

- 含义：保证模型输出 SQL 安全、稳定、兼容 MySQL。
- 做了什么：
  - 只允许 SELECT / WITH
  - 白名单表限制
  - LIMIT 自动补全
  - MySQL 方言修正
  - 地区字面值标准化
  - SQL 失败后自动走 SQL 修复链路

### 3.8 数据执行层

- 含义：查询真实业务数据。
- 当前业务表：
  - order_master
  - order_detail
  - user_info
  - product_info
  - store_info
  - refund_master
  - refund_detail

### 3.9 导出与报告层

- 含义：把结果沉淀成文件和汇报材料。
- 做了什么：
  - CSV 导出
  - 图表 Word 导出
  - 商业分析报告 Word 生成
  - 模板上传、模板解析、报告历史回看

### 3.10 异步任务层

- 含义：处理耗时任务，避免阻塞前端。
- 做了什么：
  - async_task 任务表
  - 独立 worker.py 消费
  - 处理报告生成、语义一键刷新重建
  - 心跳、抢占、状态回写

### 3.11 日志与监控层

- 含义：定位问题并监控质量。
- 做了什么：
  - web.log / web.error.log
  - worker.log / worker.error.log
  - llm_invocation_log
  - 最近一次语义刷新结果
  - query_plan 漏返字段统计
  - 日志总量超 1GB 自动裁剪

## 4. 汇报时建议突出 4 个重点

1. 先用语义层约束业务语义，再让模型写 SQL，降低大模型直接查库的错误率。  
2. SQL 不是直接执行，前面有治理，失败后还有自动修复链路。  
3. 查询、图表、下载、报告已经形成闭环。  
4. 指标、维度、同义词、联表关系都可以在后台持续维护，不是一次性写死。  
