import os
from io import BytesIO
from typing import Any

from flask import Flask, jsonify, render_template, request, send_file

from chatbi.config import LLM_PROVIDER_OPTIONS, TASK_TYPE_REPORT_GENERATE, TASK_TYPE_SEMANTIC_REBUILD
from chatbi.repository.chat_repository import normalize_conversation_id
from chatbi.repository.task_repository import list_llm_invocation_logs
from chatbi.service.conversation_service import get_conversation_view, get_latest_result_or_raise
from chatbi.service.llm_service import DEFAULT_PROVIDER, normalize_llm_provider
from chatbi.service.query_service import handle_user_query
from chatbi.service.report_service import (
    build_report_task_display_name,
    execute_report_generation_task,
    export_chart_word_file,
    export_data_file,
)
from chatbi.service.runtime_service import ensure_runtime_ready
from chatbi.service.task_service import get_task_view, list_task_views, submit_task
from reporting import (
    DEFAULT_TEMPLATE_ID,
    delete_report_template,
    get_report_history_detail,
    get_report_history_file,
    get_report_template,
    list_report_history,
    list_report_templates,
    save_uploaded_template,
    set_default_report_template,
)
from semantic_layer import (
    delete_admin_entity,
    get_admin_bootstrap,
    get_semantic_maintenance_guide,
    rebuild_admin_search,
    sync_semantic_schema,
    upsert_admin_entity,
)
from chatbi.repository.db import get_db_conn

app = Flask(__name__)


def normalize_chart_images(raw_items: Any) -> list[dict[str, str]]:
    if not isinstance(raw_items, list):
        return []
    normalized: list[dict[str, str]] = []
    for item in raw_items:
        if len(normalized) >= 6:
            break
        if not isinstance(item, dict):
            continue
        png_data_url = str(item.get('png_data_url', '')).strip()
        if not png_data_url.startswith('data:image/png;base64,'):
            continue
        normalized.append(
            {
                'title': str(item.get('title', '图表快照')).strip() or '图表快照',
                'caption': str(item.get('caption', '')).strip(),
                'png_data_url': png_data_url,
            }
        )
    return normalized


def ensure_existing_file(file_path: str) -> str:
    normalized_path = str(file_path or '').strip()
    if not normalized_path or not os.path.exists(normalized_path):
        raise ValueError('文件不存在或已被清理，请重新生成')
    return normalized_path


@app.get('/')
def index() -> str:
    return render_template(
        'index.html',
        default_llm_provider=DEFAULT_PROVIDER,
        llm_provider_options=LLM_PROVIDER_OPTIONS,
    )


@app.get('/admin/semantic')
def semantic_admin() -> str:
    return render_template('semantic_admin.html')


@app.get('/admin/report')
def report_admin() -> str:
    return render_template('report_admin.html')


@app.get('/api/admin/semantic/bootstrap')
def semantic_admin_bootstrap_api():
    try:
        ensure_runtime_ready()
        payload = get_admin_bootstrap()
        payload['maintenance_guide'] = get_semantic_maintenance_guide()
        return jsonify(payload)
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.post('/api/admin/semantic/<entity>/save')
def semantic_admin_save_api(entity: str):
    payload = request.get_json(silent=True) or {}
    try:
        ensure_runtime_ready()
        upsert_admin_entity(entity, payload)
        return jsonify({'ok': True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.post('/api/admin/semantic/<entity>/delete')
def semantic_admin_delete_api(entity: str):
    payload = request.get_json(silent=True) or {}
    try:
        ensure_runtime_ready()
        delete_admin_entity(entity, payload)
        return jsonify({'ok': True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.post('/api/admin/semantic/sync-schema')
def semantic_admin_sync_schema_api():
    try:
        ensure_runtime_ready()
        sync_semantic_schema()
        result = rebuild_admin_search(refresh_embeddings=False)
        return jsonify({'ok': True, 'result': result})
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.post('/api/admin/semantic/rebuild')
def semantic_admin_rebuild_api():
    payload = request.get_json(silent=True) or {}
    refresh_embeddings = bool(payload.get('refresh_embeddings'))
    async_mode = bool(payload.get('async', refresh_embeddings))
    client_id = str(payload.get('client_id', '')).strip()
    try:
        ensure_runtime_ready()
        if async_mode:
            task = submit_task(
                TASK_TYPE_SEMANTIC_REBUILD,
                '语义索引重建任务',
                {'refresh_embeddings': refresh_embeddings},
                client_id=client_id,
            )
            return jsonify({'ok': True, 'async': True, 'task': task})
        result = rebuild_admin_search(refresh_embeddings=refresh_embeddings)
        return jsonify({'ok': True, 'result': result})
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.get('/api/admin/report/bootstrap')
def report_admin_bootstrap_api():
    try:
        ensure_runtime_ready()
        with get_db_conn() as conn:
            templates = list_report_templates(conn)
            history = list_report_history(conn)
        return jsonify(
            {
                'templates': templates,
                'default_template_id': next(
                    (item['template_id'] for item in templates if item.get('is_default')),
                    DEFAULT_TEMPLATE_ID,
                ),
                'history': history,
                'guide': [
                    '预置模板仅允许下载和设为默认，不允许删除。',
                    '自定义模板支持上传、设为默认、删除。',
                    '每次点击“生成商业报告”都会自动入库，并将 Word 文件落盘到 report_outputs 目录。',
                    '历史回看可查看报告标题、原始问题、指标口径、模板、引擎、生成时间，并支持重新下载。',
                ],
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.get('/api/admin/report/history')
def report_history_api():
    try:
        ensure_runtime_ready()
        with get_db_conn() as conn:
            history = list_report_history(conn)
        return jsonify({'history': history})
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.get('/api/admin/report/history/<report_id>')
def report_history_detail_api(report_id: str):
    try:
        ensure_runtime_ready()
        with get_db_conn() as conn:
            detail = get_report_history_detail(conn, report_id)
        return jsonify({'detail': detail})
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.get('/api/admin/report/history/<report_id>/download')
def report_history_download_api(report_id: str):
    try:
        ensure_runtime_ready()
        with get_db_conn() as conn:
            detail = get_report_history_file(conn, report_id)
        return send_file(
            ensure_existing_file(detail['file_path']),
            as_attachment=True,
            download_name=detail['file_name'],
            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.post('/api/admin/report/templates/default')
def report_template_set_default_api():
    payload = request.get_json(silent=True) or {}
    template_id = str(payload.get('template_id', '')).strip()
    if not template_id:
        return jsonify({'error': '请先选择要设为默认的模板'}), 400
    try:
        ensure_runtime_ready()
        with get_db_conn() as conn:
            template_info = set_default_report_template(conn, template_id)
            conn.commit()
            templates = list_report_templates(conn)
        return jsonify({'ok': True, 'template': template_info, 'templates': templates})
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.post('/api/admin/report/templates/<template_id>/delete')
def report_template_delete_api(template_id: str):
    try:
        ensure_runtime_ready()
        with get_db_conn() as conn:
            delete_report_template(conn, template_id)
            conn.commit()
            templates = list_report_templates(conn)
        return jsonify({'ok': True, 'templates': templates})
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.get('/api/report/templates')
def report_templates_api():
    try:
        ensure_runtime_ready()
        with get_db_conn() as conn:
            templates = list_report_templates(conn)
        return jsonify(
            {
                'templates': templates,
                'default_template_id': next(
                    (item['template_id'] for item in templates if item.get('is_default')),
                    DEFAULT_TEMPLATE_ID,
                ),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.post('/api/report/templates/upload')
def report_template_upload_api():
    file_storage = request.files.get('file')
    if not file_storage:
        return jsonify({'error': '请先选择要上传的 .docx 报告模板'}), 400
    try:
        ensure_runtime_ready()
        with get_db_conn() as conn:
            template_info = save_uploaded_template(conn, file_storage)
            conn.commit()
            templates = list_report_templates(conn)
        return jsonify({'ok': True, 'template': template_info, 'templates': templates})
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.get('/api/report/templates/<template_id>/sample')
def report_template_sample_api(template_id: str):
    try:
        ensure_runtime_ready()
        with get_db_conn() as conn:
            template_row = get_report_template(conn, template_id)
        return send_file(
            template_row['file_path'],
            as_attachment=True,
            download_name=template_row['file_name'],
            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.post('/api/export/data')
def export_data_api():
    payload = request.get_json(silent=True) or {}
    conversation_id = normalize_conversation_id(payload.get('conversation_id'))
    try:
        ensure_runtime_ready()
        csv_bytes, download_name = export_data_file(conversation_id)
        return send_file(BytesIO(csv_bytes), as_attachment=True, download_name=download_name, mimetype='text/csv')
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.post('/api/export/chart-word')
def export_chart_word_api():
    payload = request.get_json(silent=True) or {}
    conversation_id = normalize_conversation_id(payload.get('conversation_id'))
    chart_images = normalize_chart_images(payload.get('chart_images'))
    try:
        ensure_runtime_ready()
        document_bytes, download_name = export_chart_word_file(conversation_id, payload.get('template_id'), chart_images)
        return send_file(
            BytesIO(document_bytes),
            as_attachment=True,
            download_name=download_name,
            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.post('/api/report/generate')
def generate_report_api():
    payload = request.get_json(silent=True) or {}
    conversation_id = normalize_conversation_id(payload.get('conversation_id'))
    llm_provider = normalize_llm_provider(payload.get('llm_provider')) or DEFAULT_PROVIDER
    chart_images = normalize_chart_images(payload.get('chart_images'))
    client_id = str(payload.get('client_id', '')).strip()
    async_mode = bool(payload.get('async', True))
    try:
        ensure_runtime_ready()
        latest_result = get_latest_result_or_raise(conversation_id)
        if async_mode:
            task = submit_task(
                TASK_TYPE_REPORT_GENERATE,
                build_report_task_display_name(latest_result),
                {
                    'conversation_id': conversation_id,
                    'template_id': payload.get('template_id'),
                    'llm_provider': llm_provider,
                    'chart_images': chart_images,
                    'client_id': client_id,
                },
                conversation_id=conversation_id,
                client_id=client_id,
            )
            return jsonify({'ok': True, 'async': True, 'task': task})

        result = execute_report_generation_task(
            {
                'conversation_id': conversation_id,
                'template_id': payload.get('template_id'),
                'llm_provider': llm_provider,
                'chart_images': chart_images,
                'client_id': client_id,
            }
        )
        return send_file(
            result['file_path'],
            as_attachment=True,
            download_name=result['download_name'],
            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.get('/api/tasks')
def tasks_api():
    client_id = str(request.args.get('client_id', '')).strip()
    conversation_id = str(request.args.get('conversation_id', '')).strip()
    if not client_id and not conversation_id:
        return jsonify({'error': '请至少提供 client_id 或 conversation_id'}), 400
    try:
        ensure_runtime_ready()
        tasks = list_task_views(client_id=client_id, conversation_id=conversation_id)
        return jsonify({'tasks': tasks})
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.get('/api/tasks/<task_id>')
def task_detail_api(task_id: str):
    try:
        ensure_runtime_ready()
        task = get_task_view(task_id)
        if not task:
            return jsonify({'error': '未找到任务'}), 404
        return jsonify({'task': task})
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.get('/api/tasks/<task_id>/download')
def task_download_api(task_id: str):
    try:
        ensure_runtime_ready()
        task = get_task_view(task_id)
        if not task:
            return jsonify({'error': '未找到任务'}), 404
        result = task.get('result') or {}
        file_path = result.get('file_path')
        file_name = result.get('download_name') or task.get('display_name') or 'task_result.docx'
        if task.get('status') != 'succeeded' or not file_path:
            return jsonify({'error': '任务尚未完成或没有可下载文件'}), 400
        return send_file(
            ensure_existing_file(file_path),
            as_attachment=True,
            download_name=file_name,
            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.get('/api/conversation/<conversation_id>')
def conversation_api(conversation_id: str):
    try:
        ensure_runtime_ready()
        return jsonify(get_conversation_view(conversation_id))
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.get('/api/conversation/<conversation_id>/logs')
def conversation_logs_api(conversation_id: str):
    try:
        ensure_runtime_ready()
        logs = list_llm_invocation_logs(conversation_id)
        return jsonify({'conversation_id': normalize_conversation_id(conversation_id), 'logs': logs})
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


@app.post('/api/query')
def query_api():
    payload = request.get_json(silent=True) or {}
    question = str(payload.get('question', '')).strip()
    conversation_id = normalize_conversation_id(payload.get('conversation_id'))
    llm_provider = normalize_llm_provider(payload.get('llm_provider')) or DEFAULT_PROVIDER
    client_id = str(payload.get('client_id', '')).strip()
    if not question:
        return jsonify({'error': '请输入查询问题或选择新的时间范围后再次提问'}), 400
    try:
        ensure_runtime_ready()
        result = handle_user_query(
            question=question,
            conversation_id=conversation_id,
            llm_provider=llm_provider,
            client_id=client_id,
        )
        return jsonify(result)
    except Exception as exc:  # noqa: BLE001
        return jsonify({'error': str(exc)}), 500


if __name__ == '__main__':
    ensure_runtime_ready()
    port = int(os.getenv('PORT', '8000'))
    app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)
