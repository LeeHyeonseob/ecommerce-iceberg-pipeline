TOPIC_BY_EVENT_TYPE = {
    "view": "ecommerce.view",
    "cart": "ecommerce.cart",
    "purchase": "ecommerce.purchase",
}

ALLOWED_TOPICS = tuple(TOPIC_BY_EVENT_TYPE.values())
EVENT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S %Z"

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
