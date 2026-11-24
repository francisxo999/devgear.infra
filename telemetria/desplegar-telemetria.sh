#!/usr/bin/env bash
# Crea en AWS toda la telemetría en tiempo real de DevGear. Se ejecuta UNA vez desde CloudShell:
#   git clone https://github.com/francisxo999/devgear.infra.git && bash devgear.infra/telemetria/desplegar-telemetria.sh
#
#   DynamoDB: ConexionesWebSocket (quién está conectado) y TelemetriaDevGear (eventos, con Stream)
#   Lambda mensajero: API Gateway WebSocket -> guarda/borra conexiones
#   API Gateway WebSocket (wss): el navegador se conecta aquí
#   Lambda streams: la dispara el DynamoDB Stream y hace broadcast a todas las conexiones
# Se puede volver a ejecutar: lo que ya existe se actualiza o se deja como está.
set -euo pipefail
cd "$(dirname "$0")"

REGION=us-east-1
export AWS_DEFAULT_REGION=$REGION
ETAPA=prod
INSTANCIAS_JAVA="i-03e850328df42c9e7 i-0bc493738631a6130"   # ec2-recomendaciones y ec2-pagos

CUENTA=$(aws sts get-caller-identity --query Account --output text)
ROL=$(aws iam get-role --role-name LabRole --query Role.Arn --output text)
echo "Cuenta $CUENTA · rol $ROL"

# ---------- 1) Tablas DynamoDB ----------
crear_tabla() {  # nombre clave [stream]
  if aws dynamodb describe-table --table-name "$1" >/dev/null 2>&1; then
    echo "Tabla $1: ya existe"
    return
  fi
  local extra=()
  [ "${3:-}" = "stream" ] && extra=(--stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE)
  aws dynamodb create-table --table-name "$1" \
    --attribute-definitions AttributeName="$2",AttributeType=S \
    --key-schema AttributeName="$2",KeyType=HASH \
    --billing-mode PAY_PER_REQUEST "${extra[@]}" >/dev/null
  aws dynamodb wait table-exists --table-name "$1"
  aws dynamodb update-time-to-live --table-name "$1" \
    --time-to-live-specification Enabled=true,AttributeName=ttl >/dev/null 2>&1 || true
  echo "Tabla $1: creada"
}
crear_tabla ConexionesWebSocket connectionId
crear_tabla TelemetriaDevGear id stream
STREAM_ARN=$(aws dynamodb describe-table --table-name TelemetriaDevGear --query Table.LatestStreamArn --output text)

# ---------- 2) Lambdas ----------
( cd lambdas && zip -q -j ../mensajero.zip mensajero.py && zip -q -j ../streams.zip streams.py )

desplegar_lambda() {  # nombre archivo.handler zip variables-en-JSON
  if aws lambda get-function --function-name "$1" >/dev/null 2>&1; then
    aws lambda update-function-code --function-name "$1" --zip-file "fileb://$3" >/dev/null
    aws lambda wait function-updated --function-name "$1"
    aws lambda update-function-configuration --function-name "$1" --environment "{\"Variables\":$4}" >/dev/null
    echo "Lambda $1: actualizada"
  else
    aws lambda create-function --function-name "$1" --runtime python3.12 --role "$ROL" \
      --handler "$2" --zip-file "fileb://$3" --timeout 30 --memory-size 256 \
      --environment "{\"Variables\":$4}" >/dev/null
    aws lambda wait function-active-v2 --function-name "$1" 2>/dev/null || sleep 10
    echo "Lambda $1: creada"
  fi
}
desplegar_lambda devgear-mensajero mensajero.handler mensajero.zip '{"TABLA_CONEXIONES":"ConexionesWebSocket"}'
ARN_MENSAJERO=$(aws lambda get-function --function-name devgear-mensajero --query Configuration.FunctionArn --output text)

# ---------- 3) API Gateway WebSocket ----------
WS=$(aws apigatewayv2 get-apis --query "Items[?Name=='devgear-ws'].ApiId | [0]" --output text)
if [ -z "$WS" ] || [ "$WS" = "None" ]; then
  WS=$(aws apigatewayv2 create-api --name devgear-ws --protocol-type WEBSOCKET \
        --route-selection-expression '$request.body.action' --query ApiId --output text)
  INT=$(aws apigatewayv2 create-integration --api-id "$WS" --integration-type AWS_PROXY \
        --integration-uri "arn:aws:apigateway:$REGION:lambda:path/2015-03-31/functions/$ARN_MENSAJERO/invocations" \
        --query IntegrationId --output text)
  for RUTA in '$connect' '$disconnect' '$default'; do
    aws apigatewayv2 create-route --api-id "$WS" --route-key "$RUTA" --target "integrations/$INT" >/dev/null
  done
  aws apigatewayv2 create-stage --api-id "$WS" --stage-name "$ETAPA" --auto-deploy >/dev/null
  echo "API WebSocket: creada ($WS)"
else
  echo "API WebSocket: ya existe ($WS)"
fi
aws lambda add-permission --function-name devgear-mensajero --statement-id apigw-ws \
  --action lambda:InvokeFunction --principal apigateway.amazonaws.com \
  --source-arn "arn:aws:execute-api:$REGION:$CUENTA:$WS/*" >/dev/null 2>&1 || true
WS_HTTPS="https://$WS.execute-api.$REGION.amazonaws.com/$ETAPA"
WS_WSS="wss://$WS.execute-api.$REGION.amazonaws.com/$ETAPA"

# ---------- 4) Lambda streams + disparador del DynamoDB Stream ----------
desplegar_lambda devgear-streams streams.handler streams.zip "{\"TABLA_CONEXIONES\":\"ConexionesWebSocket\",\"WS_ENDPOINT\":\"$WS_HTTPS\"}"
MAPEO=$(aws lambda list-event-source-mappings --function-name devgear-streams --query 'EventSourceMappings[0].UUID' --output text)
if [ -z "$MAPEO" ] || [ "$MAPEO" = "None" ]; then
  aws lambda create-event-source-mapping --function-name devgear-streams --event-source-arn "$STREAM_ARN" \
    --starting-position LATEST --batch-size 50 --maximum-batching-window-in-seconds 0 >/dev/null
  echo "Disparador DynamoDB Stream -> devgear-streams: creado"
else
  echo "Disparador DynamoDB Stream -> devgear-streams: ya existe"
fi

# ---------- 5) Credenciales de AWS dentro de los contenedores Java ----------
# El contenedor llega al servicio de metadatos con un salto más que la instancia: se sube el límite a 2.
for I in $INSTANCIAS_JAVA; do
  aws ec2 modify-instance-metadata-options --instance-id "$I" --http-put-response-hop-limit 2 --http-endpoint enabled >/dev/null \
    && echo "Instancia $I: metadatos con hop-limit 2" || echo "AVISO: no se pudo ajustar $I (¿existe y está encendida?)"
done

# ---------- 6) Prueba de punta a punta ----------
MAPEO=$(aws lambda list-event-source-mappings --function-name devgear-streams --query 'EventSourceMappings[0].UUID' --output text)
echo -n "Esperando que el disparador quede activo"
for _ in $(seq 1 30); do
  ESTADO=$(aws lambda get-event-source-mapping --uuid "$MAPEO" --query State --output text)
  [ "$ESTADO" = "Enabled" ] && break
  echo -n "."; sleep 4
done
echo " ($ESTADO)"
ID="prueba-$(date +%s)"
aws dynamodb put-item --table-name TelemetriaDevGear --item \
  "{\"id\":{\"S\":\"$ID\"},\"origen\":{\"S\":\"recomendaciones\"},\"tipo\":{\"S\":\"PRUEBA\"},\"clienteId\":{\"S\":\"cloudshell\"},\"timestamp\":{\"N\":\"$(date +%s)000\"},\"ttl\":{\"N\":\"$(( $(date +%s) + 3600 ))\"}}"
sleep 8
echo "--- Últimas líneas del log de devgear-streams ---"
aws logs tail /aws/lambda/devgear-streams --since 3m 2>/dev/null | tail -8 || echo "(aún sin log)"

echo
echo "=============================================================="
echo " URL del WebSocket para el dashboard:"
echo "   $WS_WSS"
echo "=============================================================="
