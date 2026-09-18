import hashlib

TOPIC_BY_EVENT_TYPE = {
    "view": "ecommerce.view",
    "cart": "ecommerce.cart",
    "purchase": "ecommerce.purchase",
}

ALLOWED_TOPICS = tuple(TOPIC_BY_EVENT_TYPE.values())
EVENT_TYPE_BY_TOPIC = {topic: event_type for event_type, topic in TOPIC_BY_EVENT_TYPE.items()}
EVENT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S %Z"
DLQ_TOPIC = "ecommerce.events.dlq"

EVENT_FIELDS = (
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
)

REQUIRED_FIELDS = (
    "event_id",
    "event_type",
    "event_time",
    "user_id",
    "user_session",
    "product_id",
)


def compute_event_id(fields: dict) -> str:
    """event_id = EVENT_FIELDS 9개 값을 고정 순서로 연결한 SHA-256.

    누락되거나 None인 값만 원본 CSV의 빈 문자열과 동일하게 취급한다(kafka_producer가
    csv.DictReader로 읽을 때 빈 칸이 ""로 오는 것과 맞추기 위함). `or ""`를 쓰면 price=0처럼
    falsy지만 유효한 값까지 빈 문자열로 뭉개져 계약을 깨므로 `is None`으로만 판정한다.
    호출자는 검증·정규화가 끝난 값을 넘겨야 한다(dict/list 같은 원시 타입이 아니라 str|None).
    """
    parts = []
    for name in EVENT_FIELDS:
        value = fields.get(name)
        parts.append("" if value is None else str(value))
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
