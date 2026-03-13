# ChatBI 本地测试

一个本地可运行的 ChatBI 样例，当前包含 7 张可联查业务表：
- `order_master`：订单主表
- `order_detail`：订单明细子表
- `user_info`：用户信息维度表
- `product_info`：产品信息维度表
- `store_info`：门店信息维度表
- `refund_master`：退款主表
- `refund_detail`：退款明细子表

核心能力：
- 自动建表并生成 10 万级以上模拟数据
- 支持连续对话、澄清追问和日期范围再次提问
- 会话持久化到 MySQL，服务重启后仍可继续追问
- 上下文采用 `滚动摘要 + 最近窗口` 压缩策略，并在页面右下角展示压缩量与剩余额度
- 使用数据库驱动的语义层和候选表召回，不再把全量 schema 直接塞给模型
- 候选召回采用 规则 + 全文索引 + 向量 embedding 混合召回
- 提供本地语义层后台维护页，可维护业务域、业务表、指标、维度、关联关系、同义词和问法示例
- 支持在 `阿里百炼 / DeepSeek` 之间切换模型引擎，并调用对应模型自动生成 SQL
- 执行 SQL 并返回指标定义、指标描述、维度、指标、结果数据
- 结果支持列表 / 柱图 / 饼图切换展示
- 支持下载当前查询明细数据 CSV，并可导出图表快照 Word
- 支持生成管理层商业分析报告 Word，默认提供专业报告模板，并支持上传自定义 `.docx` 模板样例解析样式

## 1. 环境准备

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

如果你使用的是 MySQL 8 默认认证插件，`cryptography` 是必须依赖，已经写入 `requirements.txt`。

## 2. 配置

```bash
cp .env.example .env
```

至少确认以下配置：
- `MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DATABASE`
- `DASHSCOPE_API_KEY`
- `DASHSCOPE_MODEL`
- `DEEPSEEK_API_KEY`
- `DEEPSEEK_MODEL`
- `DEFAULT_LLM_PROVIDER`
- `DASHSCOPE_EMBEDDING_MODEL`
- `MAX_HISTORY_MESSAGES`
- `MAX_UI_HISTORY_MESSAGES`
- `MAX_CONTEXT_SOURCE_MESSAGES`
- `MAX_CONTEXT_RECENT_MESSAGES`
- `CONTEXT_COMPRESSION_TRIGGER_MESSAGES`
- `CONTEXT_COMPRESSION_TRIGGER_TOKENS`
- `MAX_CONTEXT_SUMMARY_LINES`
- `LLM_REQUEST_TIMEOUT_SECONDS`
- `QUERY_TIMEOUT_MS`
- `REPORT_PREVIEW_MAX_ROWS`
- `SEMANTIC_FULLTEXT_TOPK`
- `SEMANTIC_VECTOR_TOPK`

## 3. 初始化数据库和测试数据

```bash
python init_db.py --rows 120000 --user-rows 40000 --batch-size 2000
```

默认会重建以下业务表：
- `user_info`
- `store_info`
- `product_info`
- `order_master`
- `order_detail`
- `refund_master`
- `refund_detail`

会话持久化表会在应用启动时自动创建：
- `chat_session`
- `chat_message`

语义层元数据表也会自动创建并初始化：
- `semantic_domain`
- `semantic_table`
- `semantic_column`
- `semantic_metric`
- `semantic_dimension`
- `semantic_join`
- `semantic_synonym`
- `semantic_example`
- `semantic_search_doc`

报告模板表也会自动创建并初始化：
- `report_template`

默认报告模板样例文件：
- `report_templates/default_management_report_template.docx`

## 4. 启动服务

```bash
python app.py
```

浏览器访问：
- http://127.0.0.1:8000
- 语义层后台：http://127.0.0.1:8000/admin/semantic

## 5. 使用说明

示例问题：
- `按销售大区统计近30天销售金额和订单数，按销售金额降序`
- `统计河南门店近30天销售金额排名前100`
- `按品牌统计近30天销量和销售金额，按销售金额降序`
- `统计近30天退款金额和退款单数`
- `按会员等级统计近30天销售金额`
- `查询订单排名前100的数据`

页面能力：
- `Enter` 直接发送，`Shift + Enter` 换行
- 页面可切换 `阿里百炼 / DeepSeek` 引擎
- 点击 `语义层维护` 会在新标签页打开，不影响当前对话和结果保留
- 如果结果适合图表，可切换到柱图或饼图
- 如果问题包含日期 / 月份 / 周范围，结果返回后可以直接调整范围并再次提问
- 页面刷新后会自动恢复最近一次会话和结果快照
- 页面右下角会显示上下文压缩量和当前模型剩余额度，鼠标悬停可查看详情
- 支持下载当前结果的明细 CSV，浏览器会优先弹出保存位置选择
- 支持下载图表快照 Word，自动嵌入当前图表和结果表
- 支持生成商业分析报告 Word，自动嵌入看板快照、关键发现、专业分析、策略建议和行动计划
- 支持上传自定义 `.docx` 报告模板样例，系统会自动解析标题、正文、列表和表格样式

导出与报告建议：
- 先执行查询，再使用 `下载明细数据`
- 如果当前结果支持图表，可使用 `下载图表Word`
- 如需管理层汇报材料，可使用 `生成商业报告`
- 如需自定义版式，先上传 `.docx` 模板样例，再生成报告

## 6. 语义层维护

后台维护页支持维护以下对象：
- `semantic_domain`：业务域
- `semantic_table`：业务表语义
- `semantic_metric`：指标口径
- `semantic_dimension`：维度口径
- `semantic_join`：联表关系
- `semantic_synonym`：自然语言同义词
- `semantic_example`：问法示例

维护建议：
- 业务表结构变化后，先点击“同步业务表结构”
- 修改完语义对象后，点击“重建检索索引”
- 如果希望新配置立即参与向量召回，再点击“刷新向量索引”
- `semantic_column` 和 `semantic_search_doc` 主要是系统生成结果，通常只读

## 安全说明

后端默认只允许：
- `SELECT` / `WITH` 查询
- 仅查询 7 张白名单业务表
- 单条 SQL
- 默认限制返回行数为 `MAX_RESULT_ROWS`，默认 `200`
