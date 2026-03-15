import pymysql

from chatbi.config import DB_CONFIG


def get_db_conn() -> pymysql.connections.Connection:
    try:
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
