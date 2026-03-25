# ChatBI Full Test Report - 2026-03-25

- total: 27
- passed: 27
- failed: 0

## Results

- [PASS] local_classify: `"{\"label\":\"ok\"}"`
- [PASS] local_clarify: `"请问您具体指的是订单数量还是订单金额呢？"`
- [PASS] security_s0: `{"security_level": "S0", "security_reasons": ["默认公开聚合分析"]}`
- [PASS] api_query_bailian: `{"status": 200, "reply_type": "result", "row_count": 1, "actual_provider": "bailian"}`
- [PASS] api_query_local_bailian: `{"status": 200, "reply_type": "result", "row_count": 7, "actual_provider": "bailian"}`
- [PASS] api_query_local: `{"status": 200, "reply_type": "result", "row_count": 1, "actual_provider": "local"}`
- [PASS] api_query_clarify: `{"status": 200, "reply_type": "clarify", "row_count": null, "actual_provider": "bailian"}`
- [PASS] followup_chain: `{"row_counts": [9, 10, 9]}`
- [PASS] inventory_query: `{"status": 200, "row_count": 54, "metrics": ["可售库存", "在途库存", "库存金额"]}`
- [PASS] conversation_logs: `{"status": 200, "log_count": 3}`
- [PASS] semantic_bootstrap: `{"status": 200}`
- [PASS] report_bootstrap: `{"status": 200}`
- [PASS] report_templates: `{"status": 200}`
- [PASS] report_template_sample_txt: `{"status": 200}`
- [PASS] export_base_query: `{"status": 200, "row_count": 9}`
- [PASS] export_data_csv: `{"status": 200, "size": 264}`
- [PASS] export_chart_word: `{"status": 200, "media_count": 1, "drawing_count": 2, "skip_text": false}`
- [PASS] report_generate_async: `{"task_id": "task_d83af9f9a9264584ab", "status": "succeeded", "media_count": 1, "drawing_count": 2, "skip_text": false}`
- [PASS] semantic_rebuild_async: `{"task_id": "task_75b772358d5a419595", "status": "succeeded"}`
- [PASS] tasks_list: `{"status": 200, "count": 1}`
- [PASS] eval_harness_bailian_case: `{"passed": true, "reply_type": "result", "actual_metrics": ["订单数"], "provider": "bailian"}`
- [PASS] eval_harness_local_bailian_case: `{"passed": true, "reply_type": "result", "actual_metrics": ["订单数"], "provider": "local_bailian"}`
- [PASS] db_order_master_count: `{"cnt": 120000}`
- [PASS] db_inventory_stock_count: `{"cnt": 4320}`
- [PASS] db_semantic_doc_count: `{"cnt": 144}`
- [PASS] db_report_template_count: `{"cnt": 4}`
- [PASS] llm_log_latest: `{"stage": "report_generate", "llm_provider": "bailian", "model_name": "qwen3-max"}`

## Post Fix Verification

- 已补测 `security_s0`，结果：`S0`。
- 已补测 `eval_harness_bailian_case`，结果：通过。
- 已补测 `eval_harness_local_bailian_case`，结果：通过。
