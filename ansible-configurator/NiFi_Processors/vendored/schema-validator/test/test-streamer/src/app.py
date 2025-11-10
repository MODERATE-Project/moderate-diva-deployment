import json
import time
import ssl
from datetime import datetime
import requests
import random
import paho.mqtt.client as mqtt
import os
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT Broker!")
    else:
        logging.error(f"Failed to connect, return code {rc}")

def on_publish(client, userdata, mid):
    logging.info(f"Message {mid} published.")

# Initialize MQTT client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)

# Attach the callbacks
client.on_connect = on_connect
client.on_publish = on_publish

# MQTT Configuration from Environment Variables
broker_address = os.environ.get("MQTT_BROKER_ADDRESS", "localhost")
port = int(os.environ.get("MQTT_PORT", 8883))
topic = os.environ.get("MQTT_TOPIC", "default_topic")
username = os.environ.get("MQTT_USERNAME")
password = os.environ.get("MQTT_PASSWORD")
interval = float(os.environ.get("MESSAGE_INTERVAL", 10))

logging.info(f"Loaded env variables: {broker_address}, {port}, {topic}, {username}, {password}, {interval}")

# SSL/TLS support, enable if certificates are required
# client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS, ciphers=None)
# client.tls_insecure_set(False)

# Connect to MQTT broker
if username and password:
    client.username_pw_set(username, password)

client.connect(broker_address, port=port)
client.loop_start()  # Start the loop

logging.info("Connecting to broker") 


for i in range(0,100):

    if i % 7 == 0:
        payload = {
            'name': 'pippo',
            'value': [
                {
                'v1': 'pluto',
                'v2': 'paperino',
                'v3': 'topolino'
                }
            ]
        }
    elif i == 80 or i == 92:
        payload = {
            'name': 'sto',
            'final': 'cazzo'
        }
    else:
        payload = {
            'name': 'lacoca',
            'value': [
                {
                'a0': 'ammacca',
                'a1': 'banana',
                }
            ]
        }

    result = client.publish(topic, json.dumps(payload))

    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        logging.error("Failed to publish message")
    
    time.sleep(interval)
