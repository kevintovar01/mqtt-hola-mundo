import argparse
import json

import config


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"[{userdata['id']}] no se pudo conectar: {reason_code}")
        return
    print(f"[{userdata['id']}] conectado a {config.MQTT_HOST}:{config.MQTT_PORT} "
          f"(sesion previa recuperada: {flags.session_present})")
    client.subscribe(userdata["topic"], qos=userdata["qos"])


def on_subscribe(client, userdata, mid, reason_code_list, properties):
    granted = reason_code_list[0]
    print(f"[{userdata['id']}] suscrito a '{userdata['topic']}' ({granted})")


def on_message(client, userdata, msg):
    data = json.loads(msg.payload) if msg.payload else None
    print(f"[{userdata['id']}] {msg.topic} qos={msg.qos} retain={bool(msg.retain)} -> {data}")


def parse_args():
    parser = argparse.ArgumentParser(description="Se suscribe a un topico MQTT e imprime lo que llega.")
    parser.add_argument("--id", required=True, help="identificador del cliente (client id)")
    parser.add_argument("--topic", required=True, help="topico o filtro con comodines + y #")
    parser.add_argument("--qos", type=int, choices=[0, 1, 2], default=1, help="QoS maximo de la suscripcion")
    parser.add_argument("--persistent", action="store_true",
                        help="sesion persistente: el broker guarda mensajes QoS 1/2 mientras este apagado")
    return parser.parse_args()


def main():
    args = parse_args()
    client = config.build_client(args.id)
    client.user_data_set({"id": args.id, "topic": args.topic, "qos": args.qos})
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message

    config.connect(client, persistent=args.persistent)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()
        print(f"[{args.id}] desconectado")


if __name__ == "__main__":
    main()
