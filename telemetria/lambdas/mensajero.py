"""Lambda "mensajero": API Gateway WebSocket la invoca en $connect / $disconnect / $default.

Guarda cada conexión abierta del frontend en la tabla DynamoDB ConexionesWebSocket
(clave: connectionId) y la borra al desconectarse. La Lambda "streams" lee esa tabla
para saber a quién enviarle la telemetría.
"""
import os
import time

import boto3

TABLA = os.environ.get("TABLA_CONEXIONES", "ConexionesWebSocket")
_dynamo = boto3.client("dynamodb")


def handler(event, context):
    contexto = event.get("requestContext") or {}
    ruta = contexto.get("routeKey")
    conexion = contexto.get("connectionId")

    if ruta == "$connect":
        params = event.get("queryStringParameters") or {}
        cliente = params.get("clientId") or "anonimo"
        ahora = int(time.time())
        try:
            _dynamo.put_item(
                TableName=TABLA,
                Item={
                    "connectionId": {"S": conexion},
                    "clientId": {"S": cliente},
                    "conectadoEn": {"N": str(ahora * 1000)},
                    "ttl": {"N": str(ahora + 86400)},  # limpieza automática a las 24 h
                },
            )
        except Exception as e:  # noqa: BLE001
            print(f"Error guardando conexión {conexion}: {e}")
            return {"statusCode": 500, "body": "Error interno del servidor"}
        print(f"Cliente conectado: {conexion} ({cliente})")
        return {"statusCode": 200, "body": "Conectado exitosamente"}

    if ruta == "$disconnect":
        try:
            _dynamo.delete_item(TableName=TABLA, Key={"connectionId": {"S": conexion}})
        except Exception as e:  # noqa: BLE001
            print(f"Error borrando conexión {conexion}: {e}")
        print(f"Cliente desconectado: {conexion}")
        return {"statusCode": 200, "body": "Desconectado"}

    # $default: cualquier mensaje que el navegador envíe (por ejemplo un ping)
    return {"statusCode": 200, "body": "ok"}
