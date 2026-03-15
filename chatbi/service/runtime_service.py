from chatbi.config import RUNTIME_BOOTSTRAP_LOCK_NAME
from chatbi.repository.db import ensure_table_columns, get_db_conn
from chatbi.schema.runtime_schema import (
    ASYNC_TASK_DDL,
    ASYNC_TASK_MIGRATIONS,
    CHAT_MESSAGE_DDL,
    CHAT_MESSAGE_MIGRATIONS,
    CHAT_SESSION_DDL,
    CHAT_SESSION_MIGRATIONS,
    LLM_INVOCATION_LOG_DDL,
    LLM_INVOCATION_LOG_MIGRATIONS,
)
from reporting import ensure_reporting_runtime
from semantic_layer import ensure_semantic_runtime

RUNTIME_READY = False


def acquire_runtime_lock(conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute('SELECT GET_LOCK(%s, 30) AS `lock_status`', (RUNTIME_BOOTSTRAP_LOCK_NAME,))
        row = cursor.fetchone() or {}
    if int(row.get('lock_status') or 0) != 1:
        raise ValueError('启动运行时初始化失败：未能获取系统引导锁')


def release_runtime_lock(conn) -> None:
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT RELEASE_LOCK(%s)', (RUNTIME_BOOTSTRAP_LOCK_NAME,))
    except Exception:  # noqa: BLE001
        pass


def ensure_runtime_ready() -> None:
    global RUNTIME_READY
    if RUNTIME_READY:
        return
    with get_db_conn() as conn:
        acquire_runtime_lock(conn)
        try:
            with conn.cursor() as cursor:
                cursor.execute(CHAT_SESSION_DDL)
                cursor.execute(CHAT_MESSAGE_DDL)
                cursor.execute(ASYNC_TASK_DDL)
                cursor.execute(LLM_INVOCATION_LOG_DDL)
                ensure_table_columns(cursor, 'chat_session', CHAT_SESSION_MIGRATIONS)
                ensure_table_columns(cursor, 'chat_message', CHAT_MESSAGE_MIGRATIONS)
                ensure_table_columns(cursor, 'async_task', ASYNC_TASK_MIGRATIONS)
                ensure_table_columns(cursor, 'llm_invocation_log', LLM_INVOCATION_LOG_MIGRATIONS)
                ensure_reporting_runtime(conn)
            conn.commit()
            ensure_semantic_runtime(refresh_embeddings=False)
        finally:
            release_runtime_lock(conn)
    RUNTIME_READY = True
