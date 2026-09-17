"""PyFlink 심볼을 스텁으로 대체한 입력 계약 회귀 테스트.

Row 인코딩과 StatementSet 분기는 실제 Flink 통합 검증으로 별도 확인한다.
"""
import sys
import types
import unittest
from unittest import mock


def _install_pyflink_stubs() -> None:
    if "pyflink" in sys.modules:
        return

    class _Row(tuple):
        def __new__(cls, *args):
            return super().__new__(cls, args)

    class _ScalarFunction:
        pass

    def _udf(func, result_type=None):
        return func

    class _DataTypes:
        def __getattr__(self, _name):
            return lambda *a, **k: None

    pyflink = types.ModuleType("pyflink")
    pyflink.table = types.ModuleType("pyflink.table")
    pyflink.table.DataTypes = _DataTypes()
    pyflink.table.Row = _Row
    pyflink.table.StreamTableEnvironment = object
    pyflink.table.udf = types.ModuleType("pyflink.table.udf")
    pyflink.table.udf.ScalarFunction = _ScalarFunction
    pyflink.table.udf.udf = _udf
    pyflink.datastream = types.ModuleType("pyflink.datastream")
    pyflink.datastream.StreamExecutionEnvironment = object

    sys.modules["pyflink"] = pyflink
    sys.modules["pyflink.table"] = pyflink.table
    sys.modules["pyflink.table.udf"] = pyflink.table.udf
    sys.modules["pyflink.datastream"] = pyflink.datastream


_install_pyflink_stubs()

sys.path.insert(0, "code/pipelines")
import raw_zone_consumer as m  # noqa: E402


class ParseAndValidateTest(unittest.TestCase):
    def setUp(self):
        self.validate = m.ParseAndValidate("view")

    def _reason(self, payload):
        row = self.validate.eval(payload)
        return row[0], row[1]  # is_valid, reason_code

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
        row = self.validate.eval(payload)
        self.assertTrue(row[0])
        product_id, event_id = row[5], row[12]
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
        row = self.validate.eval(payload)
        self.assertFalse(row[0])
        self.assertEqual(row[1], "MISSING_REQUIRED_FIELD")
        self.assertEqual(row[2], "user_id")

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
        with mock.patch("json.loads", side_effect=RuntimeError("boom")):
            row = self.validate.eval('{"anything": "here"}')
        self.assertFalse(row[0])
        self.assertEqual(row[1], "VALIDATION_INTERNAL_ERROR")


if __name__ == "__main__":
    unittest.main()
