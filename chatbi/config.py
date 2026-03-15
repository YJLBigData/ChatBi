import os
from datetime import date

from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', '127.0.0.1'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'toor'),
    'database': os.getenv('MYSQL_DATABASE', 'test'),
    'charset': 'utf8mb4',
}

DASHSCOPE_API_KEY = os.getenv('DASHSCOPE_API_KEY', '')
DASHSCOPE_BASE_URL = os.getenv('DASHSCOPE_BASE_URL', 'https://dashscope.aliyuncs.com/compatible-mode/v1')
DASHSCOPE_MODEL = os.getenv('DASHSCOPE_MODEL', 'qwen3-max')
DASHSCOPE_EMBEDDING_MODEL = os.getenv('DASHSCOPE_EMBEDDING_MODEL', 'text-embedding-v4')
DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY', '')
DEEPSEEK_BASE_URL = os.getenv('DEEPSEEK_BASE_URL', 'https://api.deepseek.com')
DEEPSEEK_MODEL = os.getenv('DEEPSEEK_MODEL', 'deepseek-reasoner')
DEFAULT_LLM_PROVIDER = os.getenv('DEFAULT_LLM_PROVIDER', 'bailian')

MAX_RESULT_ROWS = int(os.getenv('MAX_RESULT_ROWS', '200'))
MAX_HISTORY_MESSAGES = int(os.getenv('MAX_HISTORY_MESSAGES', '20'))
MAX_UI_HISTORY_MESSAGES = int(os.getenv('MAX_UI_HISTORY_MESSAGES', '100'))
MAX_CONTEXT_SOURCE_MESSAGES = int(os.getenv('MAX_CONTEXT_SOURCE_MESSAGES', '160'))
MAX_CONTEXT_RECENT_MESSAGES = int(os.getenv('MAX_CONTEXT_RECENT_MESSAGES', '16'))
CONTEXT_COMPRESSION_TRIGGER_MESSAGES = int(os.getenv('CONTEXT_COMPRESSION_TRIGGER_MESSAGES', '24'))
CONTEXT_COMPRESSION_TRIGGER_TOKENS = int(os.getenv('CONTEXT_COMPRESSION_TRIGGER_TOKENS', '15000'))
MAX_CONTEXT_SUMMARY_LINES = int(os.getenv('MAX_CONTEXT_SUMMARY_LINES', '10'))
QUERY_TIMEOUT_MS = int(os.getenv('QUERY_TIMEOUT_MS', '15000'))
MAX_CONVERSATION_ID_LENGTH = int(os.getenv('MAX_CONVERSATION_ID_LENGTH', '80'))
MAX_CLIENT_ID_LENGTH = int(os.getenv('MAX_CLIENT_ID_LENGTH', '80'))
LLM_REQUEST_TIMEOUT_SECONDS = int(os.getenv('LLM_REQUEST_TIMEOUT_SECONDS', '90'))
REPORT_PREVIEW_MAX_ROWS = int(os.getenv('REPORT_PREVIEW_MAX_ROWS', '12'))
SEMANTIC_VECTOR_TOPK = int(os.getenv('SEMANTIC_VECTOR_TOPK', '12'))
SEMANTIC_FULLTEXT_TOPK = int(os.getenv('SEMANTIC_FULLTEXT_TOPK', '12'))
TASK_WORKER_COUNT = int(os.getenv('TASK_WORKER_COUNT', '2'))
TASK_POLL_LIMIT = int(os.getenv('TASK_POLL_LIMIT', '30'))

ALLOWED_BASE_TABLES = {
    'order_master',
    'order_detail',
    'user_info',
    'product_info',
    'store_info',
    'refund_master',
    'refund_detail',
}

TODAY_STR = date.today().isoformat()
CONTEXT_STRATEGY_LABEL = '滚动摘要 + 最近窗口'
RUNTIME_BOOTSTRAP_LOCK_NAME = 'chatbi_runtime_bootstrap'

LLM_PROVIDER_CONFIGS = {
    'bailian': {
        'label': '阿里百炼',
        'api_key': DASHSCOPE_API_KEY,
        'base_url': DASHSCOPE_BASE_URL,
        'model': DASHSCOPE_MODEL,
        'max_input_tokens': 258048,
    },
    'deepseek': {
        'label': 'DeepSeek',
        'api_key': DEEPSEEK_API_KEY,
        'base_url': DEEPSEEK_BASE_URL,
        'model': DEEPSEEK_MODEL,
        'max_input_tokens': 128000,
    },
}

LLM_PROVIDER_ALIASES = {
    'bailian': 'bailian',
    'aliyun': 'bailian',
    'dashscope': 'bailian',
    'qwen': 'bailian',
    'deepseek': 'deepseek',
    'ds': 'deepseek',
}

LLM_PROVIDER_OPTIONS = [
    {'value': 'bailian', 'label': '阿里百炼', 'model': DASHSCOPE_MODEL},
    {'value': 'deepseek', 'label': 'DeepSeek', 'model': DEEPSEEK_MODEL},
]

TASK_TYPE_REPORT_GENERATE = 'report_generate'
TASK_TYPE_SEMANTIC_REBUILD = 'semantic_rebuild'
TASK_STATUS_PENDING = 'pending'
TASK_STATUS_RUNNING = 'running'
TASK_STATUS_SUCCEEDED = 'succeeded'
TASK_STATUS_FAILED = 'failed'
