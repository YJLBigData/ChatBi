from chatbi.logging_setup import configure_logging
from chatbi.service.runtime_service import ensure_runtime_ready
from chatbi.service.task_service import run_task_worker_forever

logger = configure_logging('worker')


if __name__ == '__main__':
    logger.info('worker 进程启动')
    ensure_runtime_ready()
    run_task_worker_forever()
