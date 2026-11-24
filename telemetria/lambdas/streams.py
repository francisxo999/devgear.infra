"""Lambda "streams": la dispara el DynamoDB Stream de la tabla TelemetriaDevGear.

Por cada lote de registros nuevos hace broadcast a todas las conexiones WebSocket
guardadas en ConexionesWebSocket. Si una conexión ya no existe (GoneException / 410),
la borra de la tabla.
"""
import json
import os

import boto3
from botocore.exceptions import ClientError

TABLA_CONEXIONES = os.environ.get("TABLA_CONEXIONES", "ConexionesWebSocket")
_dynamo = boto3.client("dynamodb")
_api = None


def _cliente_api():
    global _api
    if _api is None:
        endpoint = os.environ["WS_ENDPOINT"]  # https://<api-id>.execute-api.<region>.amazonaws.com/prod
        _api = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint)
    return _api


def _valor(av):
    if "S" in av:
        return av["S"]
    if "N" in av:
        n = av["N"]
        return int(n) if n.lstrip("-").isdigit() else float(n)
    if "BOOL" in av:
        return av["BOOL"]
    return None


def _evento(imagen):
    return {k: _valor(v) for k, v in imagen.items() if k != "ttl"}


def _conexiones():
    ids, clave = [], None
    while True:
        kwargs = {"TableName": TABLA_CONEXIONES, "ProjectionExpression": "connectionId"}
        if clave:
            kwargs["ExclusiveStartKey"] = clave
        r = _dynamo.scan(**kwargs)
        ids.extend(i["connectionId"]["S"] for i in r.get("Items", []) if "connectionId" in i)
        clave = r.get("LastEvaluatedKey")
        if not clave:
            return ids


def handler(event, context):
    lote = []
    for registro in (event or {}).get("Records", []):
        if registro.get("eventName") in ("INSERT", "MODIFY"):
            imagen = (registro.get("dynamodb") or {}).get("NewImage")
            if imagen:
                lote.append(_evento(imagen))
    if not lote:
        return {"enviados": 0, "conexiones": 0}

    datos = json.dumps(lote).encode("utf-8")
    api = _cliente_api()
    conexiones = _conexiones()
    enviados = 0
    for conexion in conexiones:
        try:
            api.post_to_connection(ConnectionId=conexion, Data=datos)
            enviados += 1
        except ClientError as e:
            codigo = e.response.get("Error", {}).get("Code", "")
            if codigo in ("GoneException", "410"):
                _dynamo.delete_item(TableName=TABLA_CONEXIONES, Key={"connectionId": {"S": conexion}})
                print(f"Conexión {conexion} cerrada; eliminada de la tabla")
            else:
                print(f"Error enviando a {conexion}: {e}")
    print(f"{len(lote)} evento(s) enviados a {enviados}/{len(conexiones)} conexión(es)")
    return {"enviados": enviados, "conexiones": len(conexiones)}
