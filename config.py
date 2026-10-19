import os

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC_GREETING = os.getenv("TOPIC_GREETING", "saludos/hola")
TOPIC_SENSOR = os.getenv("TOPIC_SENSOR", "sensores/sala/temperatura")

# Long enough to survive stopping a subscriber during the demo; meanwhile the broker
# keeps its subscriptions and queues QoS 1/2 messages for it.
SESSION_EXPIRY_SECONDS = 3600


def build_client(client_id):
    return mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv5,
    )


def connect(client, persistent=False):
    properties = None
    if persistent:
        properties = Properties(PacketTypes.CONNECT)
        properties.SessionExpiryInterval = SESSION_EXPIRY_SECONDS
    client.connect(
        MQTT_HOST,
        MQTT_PORT,
        keepalive=60,
        clean_start=not persistent,
        properties=properties,
    )
