from kafka import KafkaProducer, KafkaConsumer
import ssl
from kafka.errors import KafkaError
from kafka.structs import TopicPartition

import os
import time
import json

import logging 
logging.basicConfig(level=logging.INFO)

class KafkaCommunicationGateway:
    """This class is the only communication interface linked to Kafka of the backend
    architecture.
    It is able to send/receive simulation/ai messages to/from Kafka.
    In particular, when a message is received, the local statistics are immediately updated.

    """

    def __init__(self, 
        name: str, 
        topic: str,
        broker: str,
        security_protocol: str,
        mechanism: str,
        sasl_username: str,
        sasl_password: str,
        kafka_server_certificate_location: str):
        """It instanciates the consumer and producer for the specified `topic`.

        Args:
            topic (str): name of the topic.
        """

        partition = TopicPartition(topic, 0)
        self.topic = topic
        self.broker = broker
        self.security_protocol = security_protocol
        self.mechanism = mechanism
        self.username = sasl_username
        self.password = sasl_password
        
        self.context = ssl.create_default_context(cafile=kafka_server_certificate_location)
        self.context.check_hostname = True
        self.context.verify_mode = ssl.CERT_REQUIRED
        
        self.consumer = self.config_consumer(name)
        self.consumer.assign([partition])


    def config_consumer(self, name: str):
        """It returns the consumer configuration for the specified `topic`.

        Args:
            topic (str): name of the topic.

        Returns:
            KafkaConsumer: consumer object.
        """

        return KafkaConsumer(
            bootstrap_servers = self.broker,
            group_id = name,
            security_protocol = self.security_protocol,
            sasl_mechanism = self.mechanism,
            sasl_plain_username = self.username,
            sasl_plain_password = self.password,
            auto_offset_reset = "earliest",
            ssl_context = self.context,
            ssl_check_hostname = True
        )
    
    def receive(self):
        """It polls new messages from Kafka and it returns them.
        Returns:
            messages (list): list of messages read by the current poll.
        """
        messages = []
            
        # Poll for new messages with a 5 second timeout
        msg_pack = self.consumer.poll(timeout_ms=5000)
        
        # Iterate through the messages received
        for tp, message_list in msg_pack.items():
            for message in message_list:
                if message is not None:
                    try:
                        # Decode the message value
                        decoded_value = message.value.decode('utf-8')
                        
                        # Skip empty messages
                        if not decoded_value.strip():
                            logging.warning(f"Empty message received from topic {tp.topic}")
                            continue
                        
                        # Parse JSON
                        parsed_value = json.loads(decoded_value)
                        
                        messages.append({
                            "timestamp": message.timestamp,
                            "headers": message.headers,
                            "value": parsed_value
                        })
                        
                    except json.JSONDecodeError as e:
                        logging.error(f"Invalid JSON in message: {e}. Raw value: {message.value[:100]}")
                        continue
                    except Exception as e:
                        logging.error(f"Error processing message: {e}")
                        continue
        
        return messages

if __name__ == "__main__":
    kfg = KafkaCommunicationGateway()