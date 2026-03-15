from chatbi.service.runtime_service import ensure_runtime_ready
from chatbi.service.task_service import run_task_worker_forever


if __name__ == '__main__':
    ensure_runtime_ready()
    run_task_worker_forever()
