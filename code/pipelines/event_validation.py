import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import NamedTuple

from event_contract import EVENT_TIME_FORMAT, REQUIRED_FIELDS


class ValidationResult(NamedTuple):
    is_valid: bool
    reason_code: str | None
    failure_detail: str | None
    event_time: str | None
    event_type: str | None
    product_id: str | None
    category_id: str | None
    category_code: str | None
    brand: str | None
    price: str | None
    user_id: str | None
    user_session: str | None
    event_id: str | None


def validate_event(payload: str, expected_event_type: str) -> ValidationResult:
    """입력 계약을 검증하고 모든 실패를 결과값으로 반환한다."""
    try:
        return _validate_event(payload, expected_event_type)
    except Exception as exc:
        return _invalid("VALIDATION_INTERNAL_ERROR", str(exc))


def _validate_event(payload: str, expected_event_type: str) -> ValidationResult:
    try:
        obj = json.loads(payload)
    except (json.JSONDecodeError, TypeError) as exc:
        return _invalid("MALFORMED_JSON", str(exc))
    if not isinstance(obj, dict):
        return _invalid("MALFORMED_JSON", "payload is not a JSON object")

    fields = {
        name: _coerce_str(obj.get(name))
        for name in (*REQUIRED_FIELDS, "category_id", "category_code", "brand")
    }
    price = obj.get("price")

    for field in REQUIRED_FIELDS:
        value = fields[field]
        if value is None or value.strip() == "":
            return _invalid("MISSING_REQUIRED_FIELD", field)

    event_type = fields["event_type"]
    if event_type != expected_event_type:
        return _invalid(
            "EVENT_TYPE_MISMATCH",
            f"got={event_type} expected={expected_event_type}",
        )

    event_time = fields["event_time"]
    try:
        datetime.strptime(event_time, EVENT_TIME_FORMAT)
    except (ValueError, TypeError) as exc:
        return _invalid("INVALID_EVENT_TIME", str(exc))

    try:
        amount = Decimal(str(price))
        if not (amount.is_finite() and amount >= 0):
            return _invalid("INVALID_PRICE", f"price={price}")
    except InvalidOperation:
        return _invalid("INVALID_PRICE", f"price={price}")

    return ValidationResult(
        True,
        None,
        None,
        event_time,
        event_type,
        fields["product_id"],
        fields["category_id"],
        fields["category_code"],
        fields["brand"],
        str(price),
        fields["user_id"],
        fields["user_session"],
        fields["event_id"],
    )


def _coerce_str(value) -> str | None:
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return None
    return str(value)


def _invalid(reason_code: str, failure_detail: str) -> ValidationResult:
    return ValidationResult(
        False,
        reason_code,
        failure_detail,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
