"""dlq_replayer의 순수 로직 회귀 테스트. 실제 Kafka 연결은 mock으로 대체한다.

kafka-python-ng가 로컬에 없어도 돌아가도록 kafka 모듈을 스텁으로 주입한다
(test_raw_zone_consumer.py의 pyflink 스텁과 같은 패턴).
"""
import json
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

CODE_DIR = Path(__file__).resolve().parents[1] / "code"
sys.path.insert(0, str(CODE_DIR))


def _install_dotenv_stub():
    if "dotenv" in sys.modules:
        return
    dotenv_mod = types.ModuleType("dotenv")
    dotenv_mod.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = dotenv_mod


def _install_kafka_stub():
    if "kafka" in sys.modules:
        return

    kafka_mod = types.ModuleType("kafka")

    class _KafkaConsumer:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("실제 kafka-python-ng로 교체돼야 호출 가능")

    class _KafkaProducer:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("실제 kafka-python-ng로 교체돼야 호출 가능")

    class _TopicPartition:
        def __init__(self, topic, partition):
            self.topic = topic
            self.partition = partition

    kafka_mod.KafkaConsumer = _KafkaConsumer
    kafka_mod.KafkaProducer = _KafkaProducer
    kafka_mod.TopicPartition = _TopicPartition
    sys.modules["kafka"] = kafka_mod


_install_dotenv_stub()
_install_kafka_stub()

from pipelines.ingestion import dlq_replayer as replayer  # noqa: E402


def make_record(
    payload: str,
    original_topic: str = "ecommerce.view",
    reason_code: str = "INVALID_PRICE",
    dlq_offset: int = 1,
) -> replayer.DlqRecord:
    return replayer.DlqRecord(
        dlq_offset=dlq_offset,
        original_payload=payload,
        original_topic=original_topic,
        original_kafka_partition=0,
        original_kafka_offset=42,
        reason_code=reason_code,
        failure_detail="price=-5.0",
    )


VALID_VIEW_PAYLOAD = json.dumps(
    {
        "event_time": "2019-12-01 15:00:00 UTC",
        "event_type": "view",
        "product_id": "1001",
        "category_id": "2001",
        "category_code": "electronics.smartphone",
        "brand": "apple",
        "price": "-5.0",
        "user_id": "u001",
        "user_session": "s001",
        "event_id": "stale-event-id",
    }
)


class BuildCandidateFieldsTest(unittest.TestCase):
    def test_malformed_original_returns_none(self):
        record = make_record("not json at all")
        self.assertIsNone(replayer.build_candidate_fields(record, {}))

    def test_json_array_returns_none(self):
        record = make_record("[1, 2, 3]")
        self.assertIsNone(replayer.build_candidate_fields(record, {}))

    def test_correction_overlays_only_given_fields(self):
        record = make_record(VALID_VIEW_PAYLOAD)
        fields = replayer.build_candidate_fields(record, {"price": "12.50"})
        self.assertEqual(fields["price"], "12.50")
        self.assertEqual(fields["product_id"], "1001")  # 나머지는 원본 그대로


class ProcessRecordTest(unittest.TestCase):
    def test_correction_fixes_invalid_price_and_recomputes_event_id(self):
        record = make_record(VALID_VIEW_PAYLOAD, reason_code="INVALID_PRICE")
        outcome, candidate_json, key = replayer.process_record(record, {"price": "12.50"})
        self.assertEqual(outcome.status, "valid")
        self.assertEqual(outcome.original_event_id, "stale-event-id")
        self.assertNotEqual(outcome.recomputed_event_id, "stale-event-id")
        self.assertEqual(key, "u001")
        self.assertIn('"price": "12.50"', candidate_json)

    def test_still_invalid_without_correction_is_rejected(self):
        record = make_record(VALID_VIEW_PAYLOAD, reason_code="INVALID_PRICE")
        outcome, candidate_json, key = replayer.process_record(record, {})
        self.assertEqual(outcome.status, "rejected")
        self.assertIn("INVALID_PRICE", outcome.detail)
        self.assertIsNone(candidate_json)
        self.assertIsNone(key)

    def test_raw_revalidation_recomputes_event_id_deterministically(self):
        """보정 없이도 event_id는 항상 재계산한다(규칙 변경으로 통과하는 경우 포함)."""
        payload = json.loads(VALID_VIEW_PAYLOAD)
        payload["price"] = "10.0"  # 이제는 유효한 값
        record = make_record(json.dumps(payload), reason_code="INVALID_PRICE")
        outcome1, json1, _ = replayer.process_record(record, {})
        outcome2, json2, _ = replayer.process_record(record, {})
        self.assertEqual(outcome1.status, "valid")
        self.assertEqual(outcome1.recomputed_event_id, outcome2.recomputed_event_id)
        self.assertEqual(json1, json2)  # 같은 입력이면 재실행해도 완전히 동일 - 중복 실행에 안전

    def test_event_type_mismatch_against_target_topic_rejected(self):
        payload = json.loads(VALID_VIEW_PAYLOAD)
        payload["event_type"] = "cart"
        record = make_record(json.dumps(payload), original_topic="ecommerce.view")
        outcome, candidate_json, key = replayer.process_record(record, {})
        self.assertEqual(outcome.status, "rejected")
        self.assertIn("EVENT_TYPE_MISMATCH", outcome.detail)

    def test_malformed_original_rejected_even_with_correction(self):
        record = make_record("not json at all")
        outcome, candidate_json, key = replayer.process_record(record, {"price": "12.50"})
        self.assertEqual(outcome.status, "rejected")
        self.assertIsNone(candidate_json)

    def test_zero_price_survives_normalization_not_collapsed_to_empty(self):
        """price가 JSON number 0이어도(falsy) event_id 계산에서 빈 문자열로 뭉개지면 안 된다."""
        payload = json.loads(VALID_VIEW_PAYLOAD)
        payload["price"] = 0  # 문자열 "0"이 아니라 falsy한 JSON number
        record = make_record(json.dumps(payload), reason_code="INVALID_PRICE")
        outcome, candidate_json, _ = replayer.process_record(record, {})
        self.assertEqual(outcome.status, "valid")
        self.assertIn('"price": "0"', candidate_json)
        expected_id = replayer.compute_event_id(
            {
                "event_time": "2019-12-01 15:00:00 UTC",
                "event_type": "view",
                "product_id": "1001",
                "category_id": "2001",
                "category_code": "electronics.smartphone",
                "brand": "apple",
                "price": "0",
                "user_id": "u001",
                "user_session": "s001",
            }
        )
        self.assertEqual(outcome.recomputed_event_id, expected_id)

    def test_event_id_hashes_normalized_value_not_raw_type(self):
        """category_code가 배열로 오면 검증기는 None으로 정규화한다 - event_id도 그 정규화된
        값(None -> "")을 기준으로 계산돼야지, 배열의 문자열 표현을 기준으로 계산되면 안 된다."""
        payload = json.loads(VALID_VIEW_PAYLOAD)
        payload["price"] = "10.0"
        payload["category_code"] = ["a", "b"]  # dict/list는 _coerce_str이 None으로 취급
        record = make_record(json.dumps(payload), reason_code="INVALID_PRICE")
        outcome, candidate_json, _ = replayer.process_record(record, {})
        self.assertEqual(outcome.status, "valid")
        self.assertIn('"category_code": null', candidate_json)
        expected_id = replayer.compute_event_id(
            {
                "event_time": "2019-12-01 15:00:00 UTC",
                "event_type": "view",
                "product_id": "1001",
                "category_id": "2001",
                "category_code": None,
                "brand": "apple",
                "price": "10.0",
                "user_id": "u001",
                "user_session": "s001",
            }
        )
        self.assertEqual(outcome.recomputed_event_id, expected_id)


class ComputeEventIdTest(unittest.TestCase):
    def test_falsy_but_present_values_are_not_blanked(self):
        base = {
            "event_time": "t",
            "event_type": "view",
            "product_id": "p",
            "category_id": "c",
            "category_code": "cc",
            "brand": "b",
            "price": "0",
            "user_id": "u",
            "user_session": "s",
        }
        zero_price_str = replayer.compute_event_id(base)
        empty_price = replayer.compute_event_id({**base, "price": ""})
        self.assertNotEqual(zero_price_str, empty_price, "price='0'과 price=''는 다른 event_id여야 함")

    def test_none_treated_as_empty_string(self):
        base = {"event_time": "t", "event_type": "view", "product_id": "p", "category_id": "c",
                "category_code": None, "brand": "b", "price": "1.0", "user_id": "u", "user_session": "s"}
        via_none = replayer.compute_event_id(base)
        via_missing = replayer.compute_event_id({k: v for k, v in base.items() if k != "category_code"})
        self.assertEqual(via_none, via_missing)


class LoadCorrectionsTest(unittest.TestCase):
    def test_parses_jsonl_keyed_by_dlq_offset(self):
        path = self._write_jsonl(
            [
                '{"dlq_partition": 0, "dlq_offset": 5, "corrections": {"price": "1.0"}}',
                '{"dlq_partition": 0, "dlq_offset": 9, "corrections": {"event_time": "2019-10-01 00:00:00 UTC"}}',
            ]
        )
        corrections = replayer.load_corrections(path, dlq_partition=0)
        self.assertEqual(corrections[5], {"price": "1.0"})
        self.assertEqual(corrections[9], {"event_time": "2019-10-01 00:00:00 UTC"})

    def test_none_path_returns_empty(self):
        self.assertEqual(replayer.load_corrections(None, dlq_partition=0), {})

    def test_missing_dlq_offset_raises(self):
        path = self._write_jsonl(['{"dlq_partition": 0, "corrections": {"price": "1.0"}}'])
        with self.assertRaises(ValueError):
            replayer.load_corrections(path, dlq_partition=0)

    def test_missing_dlq_partition_raises(self):
        path = self._write_jsonl(['{"dlq_offset": 5, "corrections": {"price": "1.0"}}'])
        with self.assertRaises(ValueError):
            replayer.load_corrections(path, dlq_partition=0)

    def test_mismatched_dlq_partition_raises(self):
        """다른 파티션용 보정 파일을 잘못 넘기면 조용히 통과시키지 말고 거부해야 한다."""
        path = self._write_jsonl(['{"dlq_partition": 1, "dlq_offset": 5, "corrections": {"price": "1.0"}}'])
        with self.assertRaises(ValueError):
            replayer.load_corrections(path, dlq_partition=0)

    def test_duplicate_dlq_offset_raises(self):
        path = self._write_jsonl(
            [
                '{"dlq_partition": 0, "dlq_offset": 5, "corrections": {"price": "1.0"}}',
                '{"dlq_partition": 0, "dlq_offset": 5, "corrections": {"price": "2.0"}}',
            ]
        )
        with self.assertRaises(ValueError):
            replayer.load_corrections(path, dlq_partition=0)

    def test_invalid_json_line_raises(self):
        path = self._write_jsonl(["not json"])
        with self.assertRaises(ValueError):
            replayer.load_corrections(path, dlq_partition=0)

    def _write_jsonl(self, lines: list[str]) -> str:
        import tempfile

        f = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False, encoding="utf-8")
        f.write("\n".join(lines))
        f.close()
        self.addCleanup(lambda: __import__("os").unlink(f.name))
        return f.name


class _FakeMessage:
    def __init__(self, offset, value):
        self.offset = offset
        self.value = value


class _FakeConsumer:
    """beginning/end offset과 실제 소비 위치를 흉내내는 최소 가짜 컨슈머.

    consumer_timeout_ms로 끊기는 실제 상황(요청한 to_offset까지 메시지가 없어서 조용히
    끝나는 것)을 messages 딕셔너리에 일부러 구멍을 내서 재현한다.
    """

    def __init__(self, messages: dict, beginning: int, end: int):
        self._messages = messages
        self._beginning = beginning
        self._end = end
        self._position = None
        self._tp = None

    def assign(self, partitions):
        self._tp = partitions[0]

    def beginning_offsets(self, partitions):
        return {self._tp: self._beginning}

    def end_offsets(self, partitions):
        return {self._tp: self._end}

    def seek(self, tp, offset):
        self._position = offset

    def position(self, tp):
        return self._position

    def __iter__(self):
        for offset in sorted(o for o in self._messages if o >= self._position):
            self._position = offset + 1
            yield _FakeMessage(offset, self._messages[offset])

    def close(self):
        pass


def _dlq_json(original_topic="ecommerce.view", reason_code="INVALID_PRICE") -> str:
    return json.dumps(
        {
            "original_payload": VALID_VIEW_PAYLOAD,
            "original_topic": original_topic,
            "kafka_partition": 0,
            "kafka_offset": 42,
            "reason_code": reason_code,
            "failure_detail": "x",
        }
    )


class FetchDlqRecordsTest(unittest.TestCase):
    def test_reads_full_requested_range(self):
        messages = {i: _dlq_json() for i in range(5)}
        fake = _FakeConsumer(messages, beginning=0, end=5)
        with mock.patch.object(replayer, "KafkaConsumer", return_value=fake):
            records = replayer.fetch_dlq_records("localhost:9092", "dlq", 0, 0, 3)
        self.assertEqual([r.dlq_offset for r in records], [0, 1, 2])

    def test_raises_when_to_offset_beyond_actual_end(self):
        fake = _FakeConsumer({0: _dlq_json(), 1: _dlq_json()}, beginning=0, end=2)
        with mock.patch.object(replayer, "KafkaConsumer", return_value=fake):
            with self.assertRaises(replayer.DlqRangeError):
                replayer.fetch_dlq_records("localhost:9092", "dlq", 0, 0, 5)

    def test_raises_when_from_offset_before_beginning(self):
        fake = _FakeConsumer({5: _dlq_json()}, beginning=5, end=6)
        with mock.patch.object(replayer, "KafkaConsumer", return_value=fake):
            with self.assertRaises(replayer.DlqRangeError):
                replayer.fetch_dlq_records("localhost:9092", "dlq", 0, 0, 6)

    def test_raises_when_range_has_gap_not_actually_consumed(self):
        """end_offsets()는 5까지 있다고 하지만 실제로는 offset 3 이후가 안 온 상황을 재현한다 -
        consumer_timeout_ms 안에 후속 메시지가 없어 순회가 조용히 끝나는 실제 상황과 동치."""
        fake = _FakeConsumer({0: _dlq_json(), 1: _dlq_json(), 2: _dlq_json()}, beginning=0, end=5)
        with mock.patch.object(replayer, "KafkaConsumer", return_value=fake):
            with self.assertRaises(replayer.DlqRangeError):
                replayer.fetch_dlq_records("localhost:9092", "dlq", 0, 0, 5)


class ReplayOrchestrationTest(unittest.TestCase):
    """fetch_dlq_records/producer는 mock으로 대체해 replay()의 흐름만 검증한다."""

    def _fake_args(self, execute: bool, reason_code=None):
        return types.SimpleNamespace(
            bootstrap_servers="localhost:9092",
            dlq_topic="ecommerce.events.dlq",
            dlq_partition=0,
            from_offset=0,
            to_offset=10,
            original_topic="ecommerce.view",
            reason_code=reason_code,
            corrections=None,
            execute=execute,
        )

    def test_dry_run_does_not_construct_producer(self):
        valid_record = make_record(
            json.dumps({**json.loads(VALID_VIEW_PAYLOAD), "price": "10.0"}), reason_code="INVALID_PRICE"
        )
        with mock.patch.object(replayer, "fetch_dlq_records", return_value=[valid_record]):
            with mock.patch.object(replayer, "KafkaProducer") as mock_producer_cls:
                report = replayer.replay(self._fake_args(execute=False))
        mock_producer_cls.assert_not_called()
        self.assertEqual(report["counts"]["attempted"], 1)
        self.assertEqual(report["records"][0]["status"], "dry_run")

    def test_execute_sends_only_valid_records(self):
        valid_record = make_record(
            json.dumps({**json.loads(VALID_VIEW_PAYLOAD), "price": "10.0"}), reason_code="INVALID_PRICE", dlq_offset=1
        )
        invalid_record = make_record(VALID_VIEW_PAYLOAD, reason_code="INVALID_PRICE", dlq_offset=2)

        mock_future = mock.Mock()
        mock_future.get.return_value = None
        mock_producer = mock.Mock()
        mock_producer.send.return_value = mock_future

        with mock.patch.object(replayer, "fetch_dlq_records", return_value=[valid_record, invalid_record]):
            with mock.patch.object(replayer, "KafkaProducer", return_value=mock_producer):
                report = replayer.replay(self._fake_args(execute=True))

        self.assertEqual(mock_producer.send.call_count, 1)
        self.assertEqual(report["counts"]["replayed"], 1)
        self.assertEqual(report["counts"]["rejected"], 1)

    def test_send_failure_not_counted_as_success(self):
        valid_record = make_record(
            json.dumps({**json.loads(VALID_VIEW_PAYLOAD), "price": "10.0"}), reason_code="INVALID_PRICE"
        )
        mock_producer = mock.Mock()
        mock_producer.send.side_effect = RuntimeError("broker down")

        with mock.patch.object(replayer, "fetch_dlq_records", return_value=[valid_record]):
            with mock.patch.object(replayer, "KafkaProducer", return_value=mock_producer):
                report = replayer.replay(self._fake_args(execute=True))

        self.assertEqual(report["counts"]["replayed"], 0)
        self.assertEqual(report["counts"]["failed"], 1)

    def test_original_topic_filter_skips_other_topics(self):
        other_topic_record = make_record(VALID_VIEW_PAYLOAD, original_topic="ecommerce.cart")
        with mock.patch.object(replayer, "fetch_dlq_records", return_value=[other_topic_record]):
            report = replayer.replay(self._fake_args(execute=False))
        self.assertEqual(report["counts"]["attempted"], 0)
        self.assertEqual(report["counts"]["skipped"], 1)

    def test_reason_code_filter(self):
        other_reason = make_record(VALID_VIEW_PAYLOAD, reason_code="MISSING_REQUIRED_FIELD")
        with mock.patch.object(replayer, "fetch_dlq_records", return_value=[other_reason]):
            report = replayer.replay(self._fake_args(execute=False, reason_code="INVALID_PRICE"))
        self.assertEqual(report["counts"]["attempted"], 0)
        self.assertEqual(report["counts"]["skipped"], 1)


if __name__ == "__main__":
    unittest.main()
