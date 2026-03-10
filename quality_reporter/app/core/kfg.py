import json
import logging
import ssl
import time

from kafka.structs import OffsetAndMetadata, TopicPartition

from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)


class KafkaCommunicationGateway:
    """This class is the only communication interface linked to Kafka of the backend
    architecture.
    It is able to send/receive simulation/ai messages to/from Kafka.
    In particular, when a message is received, the local statistics are immediately updated.

    """

    def __init__(
        self,
        group_id: str,
        topic: str,
        broker: str,
        security_protocol: str,
        mechanism: str,
        sasl_username: str,
        sasl_password: str,
        kafka_server_certificate_location: str,
        auto_offset_reset: str = "latest",
        poll_timeout_ms: int = 250,
        max_poll_records: int = 1000,
    ):
        """It instanciates the consumer and producer for the specified `topic`.

        Args:
            topic (str): name of the topic.
        """

        self.topic = topic
        self.broker = broker
        self.security_protocol = security_protocol
        self.mechanism = mechanism
        self.username = sasl_username
        self.password = sasl_password
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.poll_timeout_ms = poll_timeout_ms
        self.max_poll_records = max_poll_records

        self.context = ssl.create_default_context(
            cafile=kafka_server_certificate_location
        )
        self.context.check_hostname = True
        self.context.verify_mode = ssl.CERT_REQUIRED

        self.consumer = self.config_consumer()
        self.consumer.subscribe([topic])

    def config_consumer(self):
        """It returns the consumer configuration for the specified `topic`.

        Returns:
            KafkaConsumer: consumer object.
        """

        return KafkaConsumer(
            bootstrap_servers=self.broker,
            group_id=self.group_id,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.mechanism,
            sasl_plain_username=self.username,
            sasl_plain_password=self.password,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=False,
            max_poll_records=self.max_poll_records,
            ssl_context=self.context,
            ssl_check_hostname=True,
        )

    def receive(self):
        """It polls new messages from Kafka and it returns them.
        Returns:
            messages (list): list of messages read by the current poll.
        """
        messages = []

        # Poll for new messages with a short timeout to keep the status fresh.
        msg_pack = self.consumer.poll(timeout_ms=self.poll_timeout_ms)

        # Iterate through the messages received
        for tp, message_list in msg_pack.items():
            for message in message_list:
                if message is not None:
                    try:
                        # Skip Kafka transaction control records (commit/abort markers).
                        # These are emitted by transactional producers (e.g. NiFi
                        # PublishKafka with Transactions Enabled) and start with a
                        # null byte, making them invalid JSON.
                        if message.value and message.value[0] == 0:
                            continue

                        # Decode the message value
                        decoded_value = message.value.decode("utf-8")

                        # Skip empty messages
                        if not decoded_value.strip():
                            logging.warning(
                                f"Empty message received from topic {tp.topic}"
                            )
                            continue

                        # Parse JSON
                        parsed_value = json.loads(decoded_value)

                        messages.append(
                            {
                                "topic": tp.topic,
                                "partition": message.partition,
                                "offset": message.offset,
                                "timestamp": message.timestamp,
                                "headers": message.headers,
                                "value": parsed_value,
                            }
                        )

                    except json.JSONDecodeError as e:
                        logging.error(
                            f"Invalid JSON in message: {e}. Raw value: {message.value[:100]}"
                        )
                        continue
                    except Exception as e:
                        logging.error(f"Error processing message: {e}")
                        continue

        return messages

    def commit_offsets(self, offsets: dict[tuple[str, int], int]):
        """Commit next offsets for processed topic partitions."""
        if not offsets:
            return

        commit_map = {
            TopicPartition(topic, partition): OffsetAndMetadata(next_offset, None)
            for (topic, partition), next_offset in offsets.items()
        }
        self.consumer.commit(offsets=commit_map)

    def get_end_offsets(self) -> dict[tuple[str, int], int]:
        """Return the latest broker offsets for the assigned partitions."""
        assignments = list(self.consumer.assignment())
        if not assignments:
            return {}

        end_offsets = self.consumer.end_offsets(assignments)
        return {
            (topic_partition.topic, topic_partition.partition): offset
            for topic_partition, offset in end_offsets.items()
        }

    def initialize_offsets(
        self,
        persisted_offsets: dict[tuple[str, int], int],
        discovery_timeout_s: float = 5.0,
    ):
        """Assign partitions explicitly and seek to SQLite offsets when available."""
        deadline = time.time() + discovery_timeout_s
        partitions = self.consumer.partitions_for_topic(self.topic)
        while partitions is None and time.time() < deadline:
            self.consumer.poll(timeout_ms=self.poll_timeout_ms)
            partitions = self.consumer.partitions_for_topic(self.topic)

        if not partitions:
            raise RuntimeError(f"No Kafka partitions discovered for topic {self.topic}")

        topic_partitions = [
            TopicPartition(self.topic, partition) for partition in sorted(partitions)
        ]
        self.consumer.assign(topic_partitions)

        seek_to_beginning = []
        seek_to_end = []

        for topic_partition in topic_partitions:
            key = (topic_partition.topic, topic_partition.partition)
            if key in persisted_offsets:
                self.consumer.seek(topic_partition, persisted_offsets[key])
                continue

            committed_offset = self.consumer.committed(topic_partition)
            if committed_offset is not None:
                self.consumer.seek(topic_partition, committed_offset)
            elif self.auto_offset_reset == "earliest":
                seek_to_beginning.append(topic_partition)
            else:
                seek_to_end.append(topic_partition)

        if seek_to_beginning:
            self.consumer.seek_to_beginning(*seek_to_beginning)
        if seek_to_end:
            self.consumer.seek_to_end(*seek_to_end)

    def close(self):
        """Release the Kafka consumer resources."""
        if self.consumer is not None:
            self.consumer.close()
