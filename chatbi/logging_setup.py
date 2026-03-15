import logging
import os
import threading
from logging.handlers import RotatingFileHandler
from pathlib import Path

from chatbi.config import LOG_DIR, LOG_FILE_BACKUP_COUNT, LOG_FILE_MAX_BYTES, LOG_TOTAL_MAX_BYTES


_CONFIG_LOCK = threading.Lock()
_CONFIGURED_SERVICES: set[str] = set()


def _prune_log_directory(log_dir: Path, max_total_bytes: int) -> None:
    if max_total_bytes <= 0 or not log_dir.exists():
        return
    files = [path for path in log_dir.glob('*.log*') if path.is_file()]
    total_size = sum(path.stat().st_size for path in files)
    if total_size <= max_total_bytes:
        return

    active_files: list[Path] = []
    rotated_files: list[Path] = []
    for path in files:
        suffixes = ''.join(path.suffixes)
        if any(char.isdigit() for char in suffixes):
            rotated_files.append(path)
        else:
            active_files.append(path)

    deletion_order = sorted(rotated_files, key=lambda item: item.stat().st_mtime) + sorted(
        active_files,
        key=lambda item: item.stat().st_mtime,
    )

    for path in deletion_order:
        if total_size <= max_total_bytes:
            break
        try:
            size = path.stat().st_size
            path.unlink(missing_ok=True)
            total_size -= size
        except Exception:
            continue


class PruningRotatingFileHandler(RotatingFileHandler):
    _emit_count = 0

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        type(self)._emit_count += 1
        if type(self)._emit_count % 50 == 0:
            _prune_log_directory(Path(LOG_DIR), LOG_TOTAL_MAX_BYTES)


def configure_logging(service_name: str) -> logging.Logger:
    normalized_name = str(service_name or 'app').strip().lower() or 'app'
    with _CONFIG_LOCK:
        if normalized_name in _CONFIGURED_SERVICES:
            return logging.getLogger(normalized_name)

        log_dir = Path(LOG_DIR)
        log_dir.mkdir(parents=True, exist_ok=True)
        _prune_log_directory(log_dir, LOG_TOTAL_MAX_BYTES)

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.handlers = []

        formatter = logging.Formatter(
            fmt='%(asctime)s [%(levelname)s] %(process)d %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        app_handler = PruningRotatingFileHandler(
            log_dir / f'{normalized_name}.log',
            maxBytes=LOG_FILE_MAX_BYTES,
            backupCount=LOG_FILE_BACKUP_COUNT,
            encoding='utf-8',
        )
        app_handler.setLevel(logging.INFO)
        app_handler.setFormatter(formatter)
        logger.addHandler(app_handler)

        error_handler = PruningRotatingFileHandler(
            log_dir / f'{normalized_name}.error.log',
            maxBytes=LOG_FILE_MAX_BYTES,
            backupCount=max(2, LOG_FILE_BACKUP_COUNT // 2),
            encoding='utf-8',
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)

        logging.captureWarnings(True)
        service_logger = logging.getLogger(normalized_name)
        service_logger.info(
            '日志系统已初始化: service=%s pid=%s log_dir=%s max_file_bytes=%s max_total_bytes=%s',
            normalized_name,
            os.getpid(),
            str(log_dir),
            LOG_FILE_MAX_BYTES,
            LOG_TOTAL_MAX_BYTES,
        )
        _CONFIGURED_SERVICES.add(normalized_name)
        return service_logger
