"""PyFlink 런타임과 독립적인 입력 계약 회귀 테스트."""
from pathlib import Path
import sys
import unittest
from unittest import mock

CODE_DIR = Path(__file__).resolve().parents[1] / "code"
sys.path.insert(0, str(CODE_DIR))
from pipelines.common import event_validation as validation  # noqa: E402


class ValidateEventTest(unittest.TestCase):
    def setUp(self):
        self.expected_event_type = "view"

    def _reason(self, payload):
        result = validation.validate_event(payload, self.expected_event_type)
        return result.is_valid, result.reason_code

    def test_valid_event(self):
        payload = (
            '{"event_time": "2019-12-01 15:00:00 UTC", "event_type": "view", '
            '"product_id": "1001", "category_id": "2001", '
            '"category_code": "electronics.smartphone", "brand": "apple", '
            '"price": "1099.00", "user_id": "u001", "user_session": "s001", '
            '"event_id": "e001"}'
        )
        is_valid, reason = self._reason(payload)
        self.assertTrue(is_valid)
        self.assertIsNone(reason)

    def test_null_category_and_brand_allowed(self):
        payload = (
            '{"event_time": "2019-12-01 15:00:00 UTC", "event_type": "view", '
            '"product_id": "1001", "category_id": "2001", "category_code": null, '
            '"brand": null, "price": "10.0", "user_id": "u001", '
            '"user_session": "s001", "event_id": "e001"}'
        )
        is_valid, _ = self._reason(payload)
        self.assertTrue(is_valid)

    def test_price_as_json_number_allowed(self):
        """producer가 실제로는 price를 문자열로 보내지만, JSON number로 와도
        허용해야 한다(계약의 "숫자"는 JSON 타입이 아니라 파싱 가능한 값)."""
        payload = (
            '{"event_time": "2019-12-01 15:00:00 UTC", "event_type": "view", '
            '"product_id": "1001", "category_id": "2001", "category_code": "x", '
            '"brand": "y", "price": 34.9, "user_id": "u001", '
            '"user_session": "s001", "event_id": "e001"}'
        )
        is_valid, _ = self._reason(payload)
        self.assertTrue(is_valid)

    def test_numeric_id_fields_coerced_not_crashed(self):
        """숫자형 ID도 STRING Row 슬롯에 안전하게 들어가야 한다."""
        payload = (
            '{"event_time": "2019-12-01 15:00:00 UTC", "event_type": "view", '
            '"product_id": 1001, "category_id": "2001", "category_code": "x", '
            '"brand": "y", "price": "10.0", "user_id": "u001", '
            '"user_session": "s001", "event_id": 12345}'
        )
        result = validation.validate_event(payload, self.expected_event_type)
        self.assertTrue(result.is_valid)
        product_id, event_id = result.product_id, result.event_id
        self.assertEqual(product_id, "1001")
        self.assertEqual(event_id, "12345")
        self.assertIsInstance(product_id, str)
        self.assertIsInstance(event_id, str)

    def test_malformed_json(self):
        _, reason = self._reason("not json at all")
        self.assertEqual(reason, "MALFORMED_JSON")

    def test_truncated_json(self):
        _, reason = self._reason('{"event_time": "2019-12-01 15:00:00')
        self.assertEqual(reason, "MALFORMED_JSON")

    def test_json_array_not_object(self):
        _, reason = self._reason("[1, 2, 3]")
        self.assertEqual(reason, "MALFORMED_JSON")

    def test_missing_required_field_empty_string(self):
        payload = (
            '{"event_time": "2019-12-01 15:00:00 UTC", "event_type": "view", '
            '"product_id": "1001", "category_id": "2001", "category_code": "x", '
            '"brand": "y", "price": "10.0", "user_id": "", '
            '"user_session": "s001", "event_id": "e001"}'
        )
        _, reason = self._reason(payload)
        self.assertEqual(reason, "MISSING_REQUIRED_FIELD")

    def test_missing_required_field_whitespace_only(self):
        payload = (
            '{"event_time": "2019-12-01 15:00:00 UTC", "event_type": "view", '
            '"product_id": "   ", "category_id": "2001", "category_code": "x", '
            '"brand": "y", "price": "10.0", "user_id": "u001", '
            '"user_session": "s001", "event_id": "e001"}'
        )
        _, reason = self._reason(payload)
        self.assertEqual(reason, "MISSING_REQUIRED_FIELD")

    def test_missing_required_field_wrong_type(self):
        """배열처럼 의미가 달라지는 타입은 누락으로 취급한다."""
        payload = (
            '{"event_time": "2019-12-01 15:00:00 UTC", "event_type": "view", '
            '"product_id": "1001", "category_id": "2001", "category_code": "x", '
            '"brand": "y", "price": "10.0", "user_id": ["a", "b"], '
            '"user_session": "s001", "event_id": "e001"}'
        )
        result = validation.validate_event(payload, self.expected_event_type)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.reason_code, "MISSING_REQUIRED_FIELD")
        self.assertEqual(result.failure_detail, "user_id")

    def test_event_type_mismatch(self):
        payload = (
            '{"event_time": "2019-12-01 15:00:00 UTC", "event_type": "cart", '
            '"product_id": "1001", "category_id": "2001", "category_code": "x", '
            '"brand": "y", "price": "10.0", "user_id": "u001", '
            '"user_session": "s001", "event_id": "e001"}'
        )
        _, reason = self._reason(payload)
        self.assertEqual(reason, "EVENT_TYPE_MISMATCH")

    def test_invalid_event_time(self):
        payload = (
            '{"event_time": "NOT-A-TIME", "event_type": "view", '
            '"product_id": "1001", "category_id": "2001", "category_code": "x", '
            '"brand": "y", "price": "10.0", "user_id": "u001", '
            '"user_session": "s001", "event_id": "e001"}'
        )
        _, reason = self._reason(payload)
        self.assertEqual(reason, "INVALID_EVENT_TIME")

    def test_negative_price(self):
        payload = (
            '{"event_time": "2019-12-01 15:00:00 UTC", "event_type": "view", '
            '"product_id": "1001", "category_id": "2001", "category_code": "x", '
            '"brand": "y", "price": "-5.0", "user_id": "u001", '
            '"user_session": "s001", "event_id": "e001"}'
        )
        _, reason = self._reason(payload)
        self.assertEqual(reason, "INVALID_PRICE")

    def test_nan_price(self):
        payload = (
            '{"event_time": "2019-12-01 15:00:00 UTC", "event_type": "view", '
            '"product_id": "1001", "category_id": "2001", "category_code": "x", '
            '"brand": "y", "price": "NaN", "user_id": "u001", '
            '"user_session": "s001", "event_id": "e001"}'
        )
        _, reason = self._reason(payload)
        self.assertEqual(reason, "INVALID_PRICE")

    def test_non_numeric_price(self):
        payload = (
            '{"event_time": "2019-12-01 15:00:00 UTC", "event_type": "view", '
            '"product_id": "1001", "category_id": "2001", "category_code": "x", '
            '"brand": "y", "price": "abc", "user_id": "u001", '
            '"user_session": "s001", "event_id": "e001"}'
        )
        _, reason = self._reason(payload)
        self.assertEqual(reason, "INVALID_PRICE")

    def test_zero_price_is_valid_boundary(self):
        payload = (
            '{"event_time": "2019-12-01 15:00:00 UTC", "event_type": "view", '
            '"product_id": "1001", "category_id": "2001", "category_code": "x", '
            '"brand": "y", "price": "0", "user_id": "u001", '
            '"user_session": "s001", "event_id": "e001"}'
        )
        is_valid, _ = self._reason(payload)
        self.assertTrue(is_valid)

    def test_never_raises_on_unexpected_exception(self):
        """예상하지 못한 예외도 VALIDATION_INTERNAL_ERROR로 변환한다."""
        with mock.patch.object(validation.json, "loads", side_effect=RuntimeError("boom")):
            result = validation.validate_event('{"anything": "here"}', self.expected_event_type)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.reason_code, "VALIDATION_INTERNAL_ERROR")


if __name__ == "__main__":
    unittest.main()
