from pydantic import BaseSettings
from core.settings import settings
from core.kfg import KafkaCommunicationGateway

import os
import threading
import time
from collections import defaultdict

from sqlalchemy import create_engine, Column, Integer, String, MetaData, tuple_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert as pg_insert

import logging
logger = logging.getLogger(__name__)

class TopicStats():
    """It stores the statistics computed on the messages read by Kafka.
    When a new message available, the backend computes the GUI 
    statistics and the results are stored inside this class.
    """

    def __init__(self, settings: BaseSettings, batch_size: int = 100, batch_timeout: float = 1.0):
        """Initialization of all statistic variables.
        
        Args:
            settings: Application settings
            batch_size: Number of messages to batch before DB update (default: 100)
            batch_timeout: Max seconds to wait before flushing batch (default: 1.0)
        """
        self.engine = create_engine(
            settings.database,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )
        self.metadata = MetaData()
        self.session = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.Base = declarative_base(metadata=self.metadata)
        self.running = False
        self.thread = None
        
        # Batch processing settings
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout

        class Report(self.Base):
            __tablename__ = "report"
            validator = Column(String, primary_key=True)
            rule = Column(String, primary_key=True)
            feature = Column(String, primary_key=True)
            valid = Column(Integer, default=0)
            fail = Column(Integer, default=0)
        
            def as_dict(self):
                return {column.name: getattr(self, column.name) for column in self.__table__.columns}
            
        self.Report = Report
        self.Base.metadata.create_all(bind=self.engine)

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

    def _batch_update(self, messages: list):
        """Update statistics for a batch of messages.
        
        Args:
            messages: List of Kafka messages to process
        """
        if not messages:
            return
        
        # Aggregate updates in memory first
        updates = defaultdict(lambda: {'valid': 0, 'fail': 0})
        
        try:
            for message in messages:
                validator = message['value']["validatorID"]
                
                for v in message['value']["validations"]:
                    rule, feature, result = v["type"], v["feature"], v["result"]
                    key = (validator, rule, feature)
                    
                    if result:
                        updates[key]['valid'] += 1
                    else:
                        updates[key]['fail'] += 1
            
            # Perform bulk upsert
            self._bulk_upsert(updates)
            
        except KeyError as e:
            logger.error(f"Missing key in message batch: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in batch update: {e}")

    def _bulk_upsert(self, updates: dict):
        """Perform bulk upsert operation using database-specific optimization.
        
        Args:
            updates: Dictionary mapping (validator, rule, feature) -> {'valid': int, 'fail': int}
        """
        if not updates:
            return
        
        try:
            with Session(self.engine) as db:
                # Check if using PostgreSQL for UPSERT, otherwise use UPDATE/INSERT
                dialect_name = self.engine.dialect.name
                
                if dialect_name == 'postgresql':
                    # Use PostgreSQL's ON CONFLICT for efficient upsert
                    for (validator, rule, feature), counts in updates.items():
                        stmt = pg_insert(self.Report).values(
                            validator=validator,
                            rule=rule,
                            feature=feature,
                            valid=counts['valid'],
                            fail=counts['fail']
                        )
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['validator', 'rule', 'feature'],
                            set_={
                                'valid': self.Report.valid + stmt.excluded.valid,
                                'fail': self.Report.fail + stmt.excluded.fail
                            }
                        )
                        db.execute(stmt)
                else:
                    # Fallback for other databases: fetch existing records and update
                    keys = list(updates.keys())
                    existing = {
                        (r.validator, r.rule, r.feature): r
                        for r in db.query(self.Report).filter(
                            tuple_(self.Report.validator, self.Report.rule, self.Report.feature).in_(keys)
                        ).all()
                    }
                    
                    for (validator, rule, feature), counts in updates.items():
                        key = (validator, rule, feature)
                        if key in existing:
                            existing[key].valid += counts['valid']
                            existing[key].fail += counts['fail']
                        else:
                            report = self.Report(
                                validator=validator,
                                rule=rule,
                                feature=feature,
                                valid=counts['valid'],
                                fail=counts['fail']
                            )
                            db.add(report)
                
                db.commit()
                logger.debug(f"Batch updated {len(updates)} report entries")
                
        except SQLAlchemyError as e:
            logger.error(f"Database error in bulk upsert: {e}")
            db.rollback()

    def get_report(self):
        """It returns the information about all the simulation requests read by the backend.

        Returns:
            list: A list of dictionaries, each containing report statistics.
        """
        try:
            with Session(self.engine) as db:
                items = db.query(self.Report).all()
                
                return [
                    {
                        "validator": report.validator,
                        "rule": report.rule,
                        "feature": report.feature,
                        "VALID": report.valid,
                        "FAIL": report.fail
                    }
                    for report in items
                ]
                
        except SQLAlchemyError as e:
            logger.error(f"Database error getting report: {e}")
            return {}

    def _compute_stats_loop(self):
        """Internal method that runs in background thread to process Kafka messages."""
        try:
            kfg_valid = KafkaCommunicationGateway(
                os.getenv("HOSTNAME"), 
                settings.validation_topic,
                settings.broker,
                settings.security_protocol,
                settings.mechanism,
                settings.sasl_username,
                settings.sasl_password,
                settings.kafka_server_certificate_location)
        except Exception as e:
            logger.error(f"Failed to initialize Kafka gateway: {e}")
            self.running = False
            return
        
        logger.info("Starting stats computation loop with batch processing...")
        
        message_batch = []
        last_flush_time = time.time()
        
        while self.running:
            try:
                messages = kfg_valid.receive()
                
                if messages:
                    message_batch.extend(messages)
                    
                    # Flush batch if size threshold reached
                    if len(message_batch) >= self.batch_size:
                        self._batch_update(message_batch)
                        message_batch = []
                        last_flush_time = time.time()
                
                # Flush batch if timeout reached
                current_time = time.time()
                if message_batch and (current_time - last_flush_time) >= self.batch_timeout:
                    self._batch_update(message_batch)
                    message_batch = []
                    last_flush_time = current_time
                
            except Exception as e:
                logger.error(f"Error receiving messages from Kafka: {e}")
                time.sleep(1)
        
        # Flush any remaining messages before shutdown
        if message_batch:
            self._batch_update(message_batch)
        
        logger.info("Stats computation loop ended.")

    def shutdown(self):
        """Graceful shutdown - alias for stop() for backward compatibility."""
        self.stop()

topic_stats = TopicStats(settings)

def compute_stats():
    """Start computing statistics from Kafka messages.
    This function should be called at application startup.
    For FastAPI, use lifespan events or startup/shutdown events.
    """
    topic_stats.start()