# Telemetría en tiempo real (DynamoDB + Streams + Lambda + WebSocket)

```
Kafka (navegacion.eventos, pagos.intentos)
   │
   ├─ ms-recomendaciones  TelemetriaListener  (consumer group telemetria-recomendaciones-group)
   └─ ms-pagos            AntifraudeService   (intento de pago y bloqueo antifraude)
          │  PutItem
          ▼
   DynamoDB  TelemetriaDevGear  ── DynamoDB Stream ──▶  Lambda "devgear-streams"
                                                              │  lee conexiones y hace broadcast
   DynamoDB  ConexionesWebSocket ◀── Lambda "devgear-mensajero" ◀── API Gateway WebSocket (wss)
                                                                          ▲
                                                       Angular: Administración → Telemetría
```

## Métricas del dashboard (todas en vivo)
1. Eventos de navegación por minuto y gráfico de actividad de los últimos 2 minutos
2. Usuarios activos (últimos 5 minutos)
3. Intentos de pago por minuto
4. Bloqueos antifraude (alerta)
5. Eventos por tipo, productos con más actividad y feed de últimos eventos

## Despliegue (una sola vez, en CloudShell)
```bash
git clone https://github.com/francisxo999/devgear.infra.git
bash devgear.infra/telemetria/desplegar-telemetria.sh
```
Al final imprime la URL `wss://…/prod`. Se puede volver a ejecutar sin problema.

## Qué activa la telemetría en cada servicio
Variable `TELEMETRIA_HABILITADA=true` en el `docker-compose.override.yml` de ec2-recomendaciones y ec2-pagos.
Por defecto está en `false`, así que en local (Docker Desktop, sin credenciales de AWS) no hace nada.
Las credenciales salen del rol de la instancia (LabInstanceProfile).

## Pruebas locales de las Lambdas
`python3 -I telemetria/tests/test_lambdas.py` (usa dobles de boto3, no necesita AWS).

## Si algo falla
| Síntoma | Causa probable |
|---|---|
| El dashboard conecta pero no llegan eventos | Revisar el log: `aws logs tail /aws/lambda/devgear-streams --since 10m` |
| En el log del servicio: "No se pudo escribir telemetría en DynamoDB" | La instancia no obtiene credenciales: repetir el paso del hop-limit del script y reiniciar el contenedor |
| `wss` no conecta | URL mal copiada o laboratorio apagado; el dashboard reintenta solo cada 3 s |
| Conecta y se corta a los 10 min sin actividad | Es el límite de API Gateway; el dashboard se reconecta solo |
