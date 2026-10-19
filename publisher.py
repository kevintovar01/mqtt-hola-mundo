import argparse
import json
import os
import random
from datetime import datetime, timezone

import config


def build_payload(message, qos, **extra):
    return {
        "mensaje": message,
        "qos": qos,
        "enviado_en": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        **extra,
    }


def publish(client, topic, payload, qos, retain=False):
    info = client.publish(topic, json.dumps(payload), qos=qos, retain=retain)
    info.wait_for_publish()
    print(f"[publicador] {topic} qos={qos} retain={retain} -> {payload['mensaje']}")


def parse_args():
    parser = argparse.ArgumentParser(description="Publica mensajes de saludo y de sensor en el broker MQTT.")
    parser.add_argument("--qos", type=int, choices=[0, 1, 2], default=0, help="nivel de calidad de servicio")
    parser.add_argument("--burst", type=int, default=1, help="cuantos mensajes enviar por topico")
    parser.add_argument("--retain", action="store_true", help="el broker guarda el ultimo mensaje del topico")
    parser.add_argument("--topic", choices=["saludo", "sensor", "ambos"], default="ambos")
    parser.add_argument("--message", default="Hola mundo", help="texto del saludo")
    return parser.parse_args()


def main():
    args = parse_args()
    client = config.build_client(f"publicador-{os.getpid()}")
    config.connect(client)
    client.loop_start()

    for seq in range(1, args.burst + 1):
        if args.topic in ("saludo", "ambos"):
            payload = build_payload(f"{args.message} #{seq}", args.qos)
            publish(client, config.TOPIC_GREETING, payload, args.qos, args.retain)
        if args.topic in ("sensor", "ambos"):
            reading = round(random.uniform(18, 26), 1)
            payload = build_payload(f"Lectura #{seq}", args.qos, temperatura_c=reading)
            publish(client, config.TOPIC_SENSOR, payload, args.qos, args.retain)

    client.disconnect()
    client.loop_stop()


if __name__ == "__main__":
    main()
