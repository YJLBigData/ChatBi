import re


FOLLOWUP_PATTERNS = [
    r'^(继续|接着|再|再来|然后|另外|同时|顺便)',
    r'^(只保留|保留|只看|仅看|筛选|限定|过滤)',
    r'^(按|并按|并且按).*(继续|拆分|细分|展开)',
    r'^(把|将).*(也拉出来|也加上|也带上|一起看|一起拉出来)',
    r'^(改成|换成|改为|切成|切换到)',
    r'^(去掉|不要|去除)',
    r'^(增加|加上|补上)',
    r'^(时间范围改成|日期改成|范围改成|改时间范围)',
    r'^(用户量|用户数|买家数|支付买家|支付用户|退货量|退款量|退款单数|退款订单数|销量|GMV|销售金额|订单数).*(是|指)',
    r'^(这个|这里的|其中的).*(是|指)',
    r'^(近|最近)\d+(天|周|个月|月|年)$',
    r'^(本月|本周|本季度|本年|今天|昨天|上周|上月)$',
    r'^\d{4}-\d{2}-\d{2}\s*(到|至|-)\s*\d{4}-\d{2}-\d{2}$',
]

INDEPENDENT_QUESTION_KEYWORDS = [
    '统计',
    '查询',
    '分析',
    '看一下',
    '查看',
    '近',
    '本月',
    '本周',
    '今天',
    '昨天',
    '品牌',
    '销售金额',
    '销量',
    '退款金额',
    '订单数',
]


def compact_whitespace(text: str) -> str:
    return re.sub(r'\s+', ' ', str(text or '')).strip()


def is_context_dependent_question(question: str) -> bool:
    text = compact_whitespace(question)
    if not text:
        return False
    if any(re.search(pattern, text, re.IGNORECASE) for pattern in FOLLOWUP_PATTERNS):
        return True
    if len(text) <= 18 and not any(keyword in text for keyword in INDEPENDENT_QUESTION_KEYWORDS):
        return True
    return False


def sanitize_history_content(role: str, content: str) -> str:
    text = str(content or '').strip()
    if role != 'assistant' or not text:
        return text
    text = re.sub(r'候选表:\s*.*?(?=(SQL:|$))', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'SQL:\s*.*$', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'\s+', ' ', text).strip()
    return text
