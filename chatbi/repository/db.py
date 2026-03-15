import threading

import pymysql

from chatbi.config import DB_CONFIG


_DATABASE_READY = False
_DATABASE_LOCK = threading.Lock()


def ensure_database_exists() -> None:
    global _DATABASE_READY
    if _DATABASE_READY:
        return

    with _DATABASE_LOCK:
        if _DATABASE_READY:
            return

        base_config = {
            'host': DB_CONFIG['host'],
            'port': DB_CONFIG['port'],
            'user': DB_CONFIG['user'],
            'password': DB_CONFIG['password'],
            'charset': DB_CONFIG['charset'],
            'autocommit': True,
        }
        database_name = DB_CONFIG['database']
        with pymysql.connect(**base_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{database_name}` "
                    "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
        _DATABASE_READY = True


def get_db_conn() -> pymysql.connections.Connection:
    try:
        ensure_database_exists()
        return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, autocommit=False, **DB_CONFIG)
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        if 'cryptography' in message.lower():
            raise ValueError(
                '当前 MySQL 认证方式需要 cryptography 依赖。requirements.txt 已加入该依赖，请执行 pip install -r requirements.txt'
            ) from exc
        raise


def ensure_table_columns(
    cursor: pymysql.cursors.DictCursor,
    table_name: str,
    expected_migrations: dict[str, str],
) -> None:
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    existing_columns = {
        row.get('Field') or row.get('field')
        for row in cursor.fetchall()
        if (row.get('Field') or row.get('field'))
    }
    for column_name, ddl in expected_migrations.items():
        if column_name not in existing_columns:
            cursor.execute(ddl)
