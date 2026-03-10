import logging
import threading
import time
from collections import defaultdict

from core.kfg import KafkaCommunicationGateway
from core.settings import settings
from pydantic import BaseSettings
from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Integer,
    MetaData,
    String,
    create_engine,
    event,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger(__name__)

_RECONNECT_INITIAL_DELAY = 1.0
_RECONNECT_MAX_DELAY = 60.0
_RECONNECT_BACKOFF_FACTOR = 2.0
_STATUS_ROW_ID = 1


class TopicStats:
    """Stores the aggregated report and runtime status derived from Kafka messages."""

    def __init__(
        self,
        settings: BaseSettings,
        batch_size: int | None = None,
        batch_timeout: float | None = None,
    ):
        self.settings = settings
        self.batch_size = batch_size if batch_size is not None else settings.batch_size
        self.batch_timeout = (
            batch_timeout if batch_timeout is not None else settings.batch_timeout
        )

        engine_kwargs: dict = {"pool_pre_ping": True}
        if settings.database.startswith("sqlite"):
            engine_kwargs["connect_args"] = {"check_same_thread": False}
        else:
            engine_kwargs["pool_size"] = 10
            engine_kwargs["max_overflow"] = 20

        self.engine = create_engine(settings.database, **engine_kwargs)
        if settings.database.startswith("sqlite"):
            event.listen(self.engine, "connect", self._configure_sqlite)

        self.metadata = MetaData()
        self.session = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.Base = declarative_base(metadata=self.metadata)
        self.running = False
        self.thread = None
        self._runtime_lock = threading.Lock()
        self._batch_lock = threading.Lock()
        self._current_batch_size = 0
        self._validation_pending_messages = None
        self._validation_pending_by_partition: dict[str, int] = {}
        self._message_batch: list[dict] = []

        class Report(self.Base):
            __tablename__ = "report"
            validator = Column(String, primary_key=True)
            rule = Column(String, primary_key=True)
            feature = Column(String, primary_key=True)
            valid = Column(Integer, default=0)
            fail = Column(Integer, default=0)

        class ConsumerOffset(self.Base):
            __tablename__ = "consumer_offsets"
            topic = Column(String, primary_key=True)
            partition = Column(Integer, primary_key=True)
            next_offset = Column(Integer, nullable=False)
            updated_at = Column(Float, nullable=False)

        class ReporterStatus(self.Base):
            __tablename__ = "reporter_status"
            id = Column(Integer, primary_key=True, default=_STATUS_ROW_ID)
            state = Column(String, nullable=False, default="starting")
            kafka_connected = Column(Boolean, nullable=False, default=False)
            last_poll_at = Column(Float, nullable=True)
            last_message_at = Column(Float, nullable=True)
            last_flush_at = Column(Float, nullable=True)
            last_error = Column(String, nullable=True)
            messages_processed_total = Column(Integer, nullable=False, default=0)
            current_batch_size = Column(Integer, nullable=False, default=0)
            reconnect_count = Column(Integer, nullable=False, default=0)

        self.Report = Report
        self.ConsumerOffset = ConsumerOffset
        self.ReporterStatus = ReporterStatus
        self.Base.metadata.create_all(bind=self.engine)
        self._ensure_status_row()

    @staticmethod
    def _configure_sqlite(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.close()

    def _ensure_status_row(self):
        try:
            with Session(self.engine) as db:
                status_row = db.get(self.ReporterStatus, _STATUS_ROW_ID)
                if status_row is None:
                    db.add(self.ReporterStatus(id=_STATUS_ROW_ID))
                    db.commit()
        except SQLAlchemyError as exc:
            logger.error(f"Database error creating reporter status row: {exc}")

    def start(self):
        """Start the background thread for computing stats."""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._compute_stats_loop, daemon=True)
            self.thread.start()
            logger.info("TopicStats background thread started")

    def stop(self):
        """Stop the background thread gracefully."""
        if self.running:
            self.running = False
            if self.thread:
                self.thread.join(timeout=5)
            logger.info("TopicStats background thread stopped")

    def _set_runtime_batch_size(self, batch_size: int):
        with self._runtime_lock:
            self._current_batch_size = batch_size

    def _append_to_batch(self, messages: list[dict]):
        with self._batch_lock:
            self._message_batch.extend(messages)
            batch_size = len(self._message_batch)
        self._set_runtime_batch_size(batch_size)

    def _batch_size(self) -> int:
        with self._batch_lock:
            return len(self._message_batch)

    def _drain_batch(self) -> list[dict]:
        with self._batch_lock:
            drained_messages = self._message_batch
            self._message_batch = []
        self._set_runtime_batch_size(0)
        return drained_messages

    def _clear_buffered_batch(self):
        with self._batch_lock:
            self._message_batch = []
        self._set_runtime_batch_size(0)

    def _set_runtime_lag(
        self, pending_messages: int | None, pending_by_partition: dict[str, int]
    ):
        with self._runtime_lock:
            self._validation_pending_messages = pending_messages
            self._validation_pending_by_partition = pending_by_partition

    def _load_offsets_for_topic(
        self, db: Session, topic: str
    ) -> dict[tuple[str, int], int]:
        rows = (
            db.query(self.ConsumerOffset)
            .filter(self.ConsumerOffset.topic == topic)
            .all()
        )
        return {(row.topic, row.partition): row.next_offset for row in rows}

    def _get_persisted_offsets_for_topic(self, topic: str) -> dict[tuple[str, int], int]:
        try:
            with Session(self.engine) as db:
                return self._load_offsets_for_topic(db, topic)
        except SQLAlchemyError as exc:
            logger.error(f"Database error loading persisted offsets for topic {topic}: {exc}")
            return {}

    def _refresh_validation_lag(self, kfg_valid: KafkaCommunicationGateway | None):
        if kfg_valid is None:
            self._set_runtime_lag(None, {})
            return

        try:
            end_offsets = kfg_valid.get_end_offsets()
            if not end_offsets:
                self._set_runtime_lag(0, {})
                return

            with Session(self.engine) as db:
                stored_offsets = self._load_offsets_for_topic(
                    db, self.settings.validation_topic
                )

            pending_by_partition: dict[str, int] = {}
            total_pending = 0
            for (topic, partition), end_offset in end_offsets.items():
                next_offset = stored_offsets.get((topic, partition), 0)
                pending = max(end_offset - next_offset, 0)
                pending_by_partition[str(partition)] = pending
                total_pending += pending

            self._set_runtime_lag(total_pending, pending_by_partition)
        except Exception as exc:
            logger.warning(f"Unable to refresh validation lag: {exc}")
            self._set_runtime_lag(None, {})

    def _update_status_row(self, db: Session, **changes):
        status_row = db.get(self.ReporterStatus, _STATUS_ROW_ID)
        if status_row is None:
            status_row = self.ReporterStatus(id=_STATUS_ROW_ID)
            db.add(status_row)
            db.flush()

        increment_keys = {"messages_processed_total", "reconnect_count"}
        for field, value in changes.items():
            if field in increment_keys:
                current_value = getattr(status_row, field) or 0
                setattr(status_row, field, current_value + value)
            else:
                setattr(status_row, field, value)
        return status_row

    def _record_status(self, **changes):
        try:
            with Session(self.engine) as db:
                self._update_status_row(db, **changes)
                db.commit()
        except SQLAlchemyError as exc:
            logger.error(f"Database error updating reporter status: {exc}")

    def _message_timestamp_seconds(self, message: dict) -> float | None:
        timestamp = message.get("timestamp")
        if timestamp is None:
            return None
        return float(timestamp) / 1000.0

    def _load_offsets(
        self, db: Session, batch_offsets: dict[tuple[str, int], int]
    ) -> dict[tuple[str, int], int]:
        offsets: dict[tuple[str, int], int] = {}
        for topic, partition in batch_offsets.keys():
            row = db.get(self.ConsumerOffset, {"topic": topic, "partition": partition})
            if row is not None:
                offsets[(topic, partition)] = row.next_offset
        return offsets

    def _bulk_upsert_reports(self, db: Session, updates: dict):
        if not updates:
            return

        dialect_name = self.engine.dialect.name
        for (validator, rule, feature), counts in updates.items():
            values = {
                "validator": validator,
                "rule": rule,
                "feature": feature,
                "valid": counts["valid"],
                "fail": counts["fail"],
            }

            if dialect_name == "postgresql":
                stmt = pg_insert(self.Report).values(**values)
            elif dialect_name == "sqlite":
                stmt = sqlite_insert(self.Report).values(**values)
            else:
                existing = (
                    db.query(self.Report)
                    .filter_by(
                        validator=validator,
                        rule=rule,
                        feature=feature,
                    )
                    .first()
                )
                if existing:
                    existing.valid += counts["valid"]
                    existing.fail += counts["fail"]
                else:
                    db.add(self.Report(**values))
                continue

            stmt = stmt.on_conflict_do_update(
                index_elements=["validator", "rule", "feature"],
                set_={
                    "valid": self.Report.valid + stmt.excluded.valid,
                    "fail": self.Report.fail + stmt.excluded.fail,
                },
            )
            db.execute(stmt)

    def _bulk_upsert_offsets(
        self, db: Session, offsets: dict[tuple[str, int], int], timestamp: float
    ):
        if not offsets:
            return

        dialect_name = self.engine.dialect.name
        for (topic, partition), next_offset in offsets.items():
            values = {
                "topic": topic,
                "partition": partition,
                "next_offset": next_offset,
                "updated_at": timestamp,
            }

            if dialect_name == "postgresql":
                stmt = pg_insert(self.ConsumerOffset).values(**values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["topic", "partition"],
                    set_={
                        "next_offset": stmt.excluded.next_offset,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
                db.execute(stmt)
            elif dialect_name == "sqlite":
                stmt = sqlite_insert(self.ConsumerOffset).values(**values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["topic", "partition"],
                    set_={
                        "next_offset": stmt.excluded.next_offset,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
                db.execute(stmt)
            else:
                existing = db.get(
                    self.ConsumerOffset, {"topic": topic, "partition": partition}
                )
                if existing:
                    existing.next_offset = next_offset
                    existing.updated_at = timestamp
                else:
                    db.add(self.ConsumerOffset(**values))

    def _persist_batch(
        self, messages: list[dict]
    ) -> tuple[dict[tuple[str, int], int] | None, int]:
        if not messages:
            return {}, 0

        batch_offsets: dict[tuple[str, int], int] = {}
        for message in messages:
            key = (message["topic"], message["partition"])
            next_offset = message["offset"] + 1
            batch_offsets[key] = max(batch_offsets.get(key, next_offset), next_offset)

        updates = defaultdict(lambda: {"valid": 0, "fail": 0})
        fresh_messages = 0
        max_message_time = None
        now = time.time()

        try:
            with Session(self.engine) as db:
                stored_offsets = self._load_offsets(db, batch_offsets)

                for message in messages:
                    key = (message["topic"], message["partition"])
                    stored_next_offset = stored_offsets.get(key)
                    if (
                        stored_next_offset is not None
                        and message["offset"] < stored_next_offset
                    ):
                        continue

                    validator = message["value"]["validatorID"]
                    for validation in message["value"]["validations"]:
                        update_key = (
                            validator,
                            validation["type"],
                            validation["feature"],
                        )
                        if validation["result"]:
                            updates[update_key]["valid"] += 1
                        else:
                            updates[update_key]["fail"] += 1

                    message_time = self._message_timestamp_seconds(message)
                    if message_time is not None:
                        max_message_time = max(
                            max_message_time or message_time, message_time
                        )
                    fresh_messages += 1

                self._bulk_upsert_reports(db, updates)

                persisted_offsets = {}
                for key, batch_next_offset in batch_offsets.items():
                    persisted_offsets[key] = max(
                        batch_next_offset, stored_offsets.get(key, batch_next_offset)
                    )

                self._bulk_upsert_offsets(db, persisted_offsets, now)
                status_changes = {
                    "kafka_connected": True,
                    "last_poll_at": now,
                    "last_flush_at": now,
                    "last_error": None,
                    "current_batch_size": 0,
                    "messages_processed_total": fresh_messages,
                    "state": "healthy",
                }
                if max_message_time is not None:
                    status_changes["last_message_at"] = max_message_time
                self._update_status_row(db, **status_changes)
                db.commit()
                return persisted_offsets, fresh_messages
        except KeyError as exc:
            logger.error(f"Missing key in message batch: {exc}")
        except SQLAlchemyError as exc:
            logger.error(f"Database error persisting message batch: {exc}")
        except Exception as exc:
            logger.error(f"Unexpected error persisting message batch: {exc}")

        return None, 0

    def get_report(self, validator: str | None = None):
        """Return the aggregated validation counts, optionally filtered by validator."""
        try:
            with Session(self.engine) as db:
                query = db.query(self.Report)
                if validator is not None:
                    query = query.filter(self.Report.validator == validator)
                items = query.all()

                return [
                    {
                        "validator": report.validator,
                        "rule": report.rule,
                        "feature": report.feature,
                        "VALID": report.valid,
                        "FAIL": report.fail,
                    }
                    for report in items
                ]
        except SQLAlchemyError as exc:
            logger.error(f"Database error getting report: {exc}")
            return []

    def clear_report(self):
        """Delete all records from the report table and discard any buffered batch."""
        try:
            self._clear_buffered_batch()
            with Session(self.engine) as db:
                num_deleted = db.query(self.Report).delete()
                db.commit()
                logger.info(f"Cleared {num_deleted} records from the report table.")
                return num_deleted
        except SQLAlchemyError as exc:
            logger.error(f"Database error while clearing report table: {exc}")
            return 0

    def _derive_state(self, status_row, now: float) -> str:
        if status_row is None:
            return "starting"
        if not status_row.kafka_connected:
            return "error"
        if not status_row.last_flush_at:
            return "starting"

        age = now - status_row.last_flush_at
        if age <= self.settings.healthy_after_seconds:
            return "healthy"
        if age < self.settings.stale_after_seconds:
            return "catching_up"
        return "stale"

    def get_status(self):
        """Return the current runtime status used by the API status endpoint."""
        now = time.time()
        try:
            with Session(self.engine) as db:
                status_row = db.get(self.ReporterStatus, _STATUS_ROW_ID)
                state = self._derive_state(status_row, now)

                with self._runtime_lock:
                    current_batch_size = self._current_batch_size
                    validation_pending_messages = self._validation_pending_messages
                    validation_pending_by_partition = dict(
                        self._validation_pending_by_partition
                    )

                return {
                    "state": state,
                    "kafka_connected": (
                        bool(status_row.kafka_connected) if status_row else False
                    ),
                    "consumer_group": self.settings.kafka_consumer_group_id,
                    "last_poll_at": status_row.last_poll_at if status_row else None,
                    "last_message_at": (
                        status_row.last_message_at if status_row else None
                    ),
                    "last_flush_at": status_row.last_flush_at if status_row else None,
                    "seconds_since_last_flush": (
                        (now - status_row.last_flush_at)
                        if status_row and status_row.last_flush_at
                        else None
                    ),
                    "messages_processed_total": (
                        status_row.messages_processed_total if status_row else 0
                    ),
                    "current_batch_size": current_batch_size,
                    "validation_pending_messages": validation_pending_messages,
                    "validation_pending_by_partition": validation_pending_by_partition,
                    "reconnect_count": status_row.reconnect_count if status_row else 0,
                    "last_error": status_row.last_error if status_row else None,
                }
        except SQLAlchemyError as exc:
            logger.error(f"Database error getting reporter status: {exc}")
            return {
                "state": "error",
                "kafka_connected": False,
                "consumer_group": self.settings.kafka_consumer_group_id,
                "last_poll_at": None,
                "last_message_at": None,
                "last_flush_at": None,
                "seconds_since_last_flush": None,
                "messages_processed_total": 0,
                "current_batch_size": 0,
                "validation_pending_messages": None,
                "validation_pending_by_partition": {},
                "reconnect_count": 0,
                "last_error": str(exc),
            }

    def _compute_stats_loop(self):
        """Internal method that runs in a background thread to process Kafka messages."""
        kfg_valid = None
        last_flush_time = time.time()
        backoff = _RECONNECT_INITIAL_DELAY
        last_status_heartbeat = 0.0

        while self.running:
            if kfg_valid is None:
                try:
                    kfg_valid = KafkaCommunicationGateway(
                        self.settings.kafka_consumer_group_id,
                        self.settings.validation_topic,
                        self.settings.broker,
                        self.settings.security_protocol,
                        self.settings.mechanism,
                        self.settings.sasl_username,
                        self.settings.sasl_password,
                        self.settings.kafka_server_certificate_location,
                        auto_offset_reset=self.settings.kafka_auto_offset_reset,
                        poll_timeout_ms=self.settings.kafka_poll_timeout_ms,
                        max_poll_records=self.settings.kafka_max_poll_records,
                    )
                    persisted_offsets = self._get_persisted_offsets_for_topic(
                        self.settings.validation_topic
                    )
                    kfg_valid.initialize_offsets(persisted_offsets)
                    self._record_status(
                        kafka_connected=True,
                        last_error=None,
                        state="healthy",
                    )
                    self._refresh_validation_lag(kfg_valid)
                    backoff = _RECONNECT_INITIAL_DELAY
                    last_status_heartbeat = time.time()
                    logger.info("Connected to Kafka broker.")
                except Exception as exc:
                    if kfg_valid is not None:
                        try:
                            kfg_valid.close()
                        except Exception as close_exc:
                            logger.warning(
                                f"Failed to close Kafka consumer cleanly: {close_exc}"
                            )
                        kfg_valid = None
                    self._set_runtime_lag(None, {})
                    self._record_status(
                        kafka_connected=False,
                        last_error=str(exc),
                        reconnect_count=1,
                        state="error",
                    )
                    logger.error(
                        f"Kafka connection failed: {exc}. Retrying in {backoff:.1f}s."
                    )
                    time.sleep(backoff)
                    backoff = min(
                        backoff * _RECONNECT_BACKOFF_FACTOR, _RECONNECT_MAX_DELAY
                    )
                    continue

            try:
                messages = kfg_valid.receive()
                now = time.time()

                if messages:
                    self._append_to_batch(messages)
                    self._refresh_validation_lag(kfg_valid)

                buffered_batch_size = self._batch_size()
                if buffered_batch_size and (
                    buffered_batch_size >= self.batch_size
                    or (now - last_flush_time) >= self.batch_timeout
                ):
                    batch_to_flush = self._drain_batch()
                    offsets_to_commit, fresh_messages = self._persist_batch(batch_to_flush)
                    if offsets_to_commit is None:
                        self._append_to_batch(batch_to_flush)
                        raise RuntimeError("Failed to persist Kafka message batch")
                    if offsets_to_commit:
                        kfg_valid.commit_offsets(offsets_to_commit)
                        logger.debug(
                            "Persisted batch with %s Kafka messages (%s new) across %s partitions",
                            len(batch_to_flush),
                            fresh_messages,
                            len(offsets_to_commit),
                        )
                    last_flush_time = now
                    last_status_heartbeat = now
                    self._refresh_validation_lag(kfg_valid)
                elif (
                    not messages
                    and (now - last_status_heartbeat)
                    >= self.settings.healthy_after_seconds
                ):
                    self._record_status(
                        kafka_connected=True,
                        last_poll_at=now,
                        current_batch_size=buffered_batch_size,
                    )
                    self._refresh_validation_lag(kfg_valid)
                    last_status_heartbeat = now
            except Exception as exc:
                self._set_runtime_lag(None, {})
                self._record_status(
                    kafka_connected=False,
                    last_error=str(exc),
                    reconnect_count=1,
                    state="error",
                )
                logger.error(f"Kafka receive error: {exc}. Will reconnect.")
                if kfg_valid is not None:
                    try:
                        kfg_valid.close()
                    except Exception as close_exc:
                        logger.warning(
                            f"Failed to close Kafka consumer cleanly: {close_exc}"
                        )
                kfg_valid = None

        if self._batch_size() and kfg_valid is not None:
            batch_to_flush = self._drain_batch()
            offsets_to_commit, fresh_messages = self._persist_batch(batch_to_flush)
            if offsets_to_commit:
                kfg_valid.commit_offsets(offsets_to_commit)
                logger.info(
                    "Persisted final batch with %s Kafka messages (%s new) across %s partitions",
                    len(batch_to_flush),
                    fresh_messages,
                    len(offsets_to_commit),
                )
            elif offsets_to_commit is None:
                self._append_to_batch(batch_to_flush)
            self._refresh_validation_lag(kfg_valid)

        self._set_runtime_batch_size(0)
        self._set_runtime_lag(None, {})
        if kfg_valid is not None:
            try:
                kfg_valid.close()
            except Exception as exc:
                logger.warning(f"Failed to close Kafka consumer during shutdown: {exc}")
        logger.info("Stats computation loop ended.")

    def shutdown(self):
        """Graceful shutdown - alias for stop() for backward compatibility."""
        self.stop()


topic_stats = TopicStats(settings)


def compute_stats():
    """Start computing statistics from Kafka messages."""
    topic_stats.start()
