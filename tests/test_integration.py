import queue
import socket
import threading
import uuid

import pytest

import config
import publisher

WAIT_SECONDS = 2


@pytest.fixture(autouse=True, scope="module")
def require_broker():
    try:
        socket.create_connection((config.MQTT_HOST, config.MQTT_PORT), timeout=2).close()
    except OSError:
        pytest.fail(f"No hay broker en {config.MQTT_HOST}:{config.MQTT_PORT}. Ejecuta: docker compose up -d")


def unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def drain(messages, seconds=WAIT_SECONDS):
    collected = []
    try:
        while True:
            collected.append(messages.get(timeout=seconds))
    except queue.Empty:
        return collected


def start_subscriber(client_id, topic, qos=1, persistent=False):
    messages = queue.Queue()
    subscribed = threading.Event()
    client = config.build_client(client_id)
    client.on_message = lambda c, u, msg: messages.put(msg)
    client.on_subscribe = lambda c, u, mid, codes, props: subscribed.set()
    config.connect(client, persistent=persistent)
    client.loop_start()
    client.subscribe(topic, qos=qos)
    assert subscribed.wait(5), "el broker no confirmo la suscripcion"
    return client, messages


def reconnect(client_id):
    messages = queue.Queue()
    client = config.build_client(client_id)
    client.on_message = lambda c, u, msg: messages.put(msg)
    config.connect(client, persistent=True)
    client.loop_start()
    return client, messages


def stop(client):
    client.disconnect()
    client.loop_stop()


def send(topic, count, qos, retain=False):
    client = config.build_client(unique("pub"))
    config.connect(client)
    client.loop_start()
    for seq in range(count):
        publisher.publish(client, topic, publisher.build_payload(f"msg {seq}", qos), qos, retain)
    stop(client)


def test_greeting_reaches_only_matching_subscriber():
    ns = unique("test")
    greetings, greetings_inbox = start_subscriber(unique("sub-saludos"), f"{ns}/saludos/#")
    sensors, sensors_inbox = start_subscriber(unique("sub-sensores"), f"{ns}/sensores/+/temperatura")

    send(f"{ns}/saludos/hola", 1, qos=1)

    assert len(drain(greetings_inbox)) == 1
    assert drain(sensors_inbox, seconds=0.5) == []
    stop(greetings)
    stop(sensors)


@pytest.mark.parametrize("qos, expected", [(0, 0), (1, 3), (2, 3)])
def test_offline_persistent_session_only_keeps_qos1_and_qos2(qos, expected):
    topic = f"{unique('test')}/saludos/hola"
    client_id = unique("sub-persistente")
    client, _ = start_subscriber(client_id, topic, qos=2, persistent=True)
    stop(client)

    send(topic, 3, qos=qos)

    client, inbox = reconnect(client_id)
    received = drain(inbox)
    stop(client)
    assert len(received) == expected


def test_retained_message_delivered_to_late_subscriber():
    topic = f"{unique('test')}/saludos/hola"
    send(topic, 1, qos=1, retain=True)

    client, inbox = start_subscriber(unique("sub-tardio"), topic)
    received = drain(inbox)
    stop(client)

    assert len(received) == 1
    assert received[0].retain
