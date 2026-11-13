# Kafka de DevGear (1 Zookeeper + 3 brokers + Kafka UI)

Cumple la pauta de la EP6 con la simplificación que autorizó el profesor (un solo Zookeeper): un Zookeeper,
tres brokers (para poder tener 3 réplicas), Kafka UI y dos tópicos con 3 particiones y 3 réplicas cada uno.

## 1. Levantarlo (local)

```
copy .env.example .env
docker compose up -d
docker compose ps
```

Espera 1 o 2 minutos. `kafka-init` aparece como `Exited (0)`: es normal, solo crea los tópicos y termina.
Kafka UI: http://localhost:8090 (Brokers: 3, Topics: 2).

Memoria: el conjunto usa ~3,5 GB. En Docker Desktop (WSL2) conviene fijar un tope en
`C:\Users\<tu usuario>\.wslconfig`:

```
[wsl2]
memory=8GB
```

y luego `wsl --shutdown` y reiniciar Docker Desktop.

## 2. Comprobar que quedó bien

```
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --describe
docker exec kafka-1 zookeeper-shell zookeeper:2181 ls /brokers/ids
```

- Cada tópico debe mostrar `PartitionCount: 3` y `ReplicationFactor: 3`, con `Isr` de 3 brokers.
- El segundo comando debe responder `[1, 2, 3]`.

Prueba rápida con la consola (dos terminales):

```
docker exec -it kafka-1 kafka-console-producer --bootstrap-server kafka-1:29092 --topic navegacion.eventos
docker exec -it kafka-2 kafka-console-consumer --bootstrap-server kafka-2:29092 --topic navegacion.eventos --group prueba --from-beginning
```

## 3. Decisiones de diseño (para justificar en la presentación)

| Tema | Decisión | Por qué |
|---|---|---|
| 1 Zookeeper | Un solo nodo (autorizado por el profesor) | Suficiente para el alcance; en producción serían 3 para tolerar fallas del propio Zookeeper |
| 3 brokers, réplica 3 | Cada partición vive en los 3 brokers | Si un broker cae, otro toma el liderazgo sin perder datos |
| 3 particiones | Hasta 3 consumidores del mismo grupo leen en paralelo | Más rendimiento; la clave del mensaje (`usuarioId`) mantiene el orden por usuario |
| `navegacion.eventos` | retención 1 día, `min.insync.replicas=1`, productor `acks=1` | Mucho volumen y bajo valor por mensaje: se prioriza rendimiento |
| `pagos.intentos` | retención 7 días, `min.insync.replicas=2`, productor `acks=all` | Más crítico (antifraude y auditoría de pagos): se prioriza consistencia sobre disponibilidad |
| Autocreación de tópicos | Desactivada | Los tópicos solo existen con la configuración definida aquí |

## 4. Diseño de los eventos (propuesta)

`navegacion.eventos` (publica ms-productos al ver el detalle de un producto):

```json
{ "eventId": "uuid", "tipo": "PRODUCTO_VISTO", "usuarioId": "oid de Azure",
  "productoId": 12, "categoria": "Teclados", "timestamp": "2026-11-12T20:15:00Z" }
```

`navegacion.eventos` también recibe los eventos del carrito (publica ms-carrito):

```json
{ "eventId": "uuid", "tipo": "ITEM_AGREGADO | ITEM_ELIMINADO", "usuarioId": "oid de Azure",
  "productoId": 12, "categoria": "Teclados", "cantidad": 2, "timestamp": "2026-11-12T20:16:00Z" }
```

`pagos.intentos` (publica ms-pagos antes de iniciar el pago; la clave es el `idCliente`, así todos los intentos de un cliente caen en la misma partición y se evalúan en orden):

```json
{ "idCliente": "oid de Azure", "monto": 286970, "idTransaccion": "TX-1794208707445",
  "timestamp": "2026-11-12T20:20:00Z" }
```

Justificación de campos: `eventId` evita contar dos veces; `tipo` permite varios casos de uso por tópico;
`usuarioId` es la clave de partición y sirve para recomendar; `categoria` evita consultar productos para
recomendar; `timestamp` ordena y permite métricas por ventana de tiempo.

## 5. Guion de demostración (mapeado a la pauta)

1. **Arquitectura:** `docker compose ps` y Kafka UI > Brokers. Explica Zookeeper (metadatos y elección de líder) y brokers (almacenan y sirven los mensajes).
2. **Tópicos:** Kafka UI > Topics > cada tópico: particiones, réplicas, `In sync replicas`. Explica la tabla de la sección 3.
3. **Administración:** Topics > Settings muestra `retention.ms` y `min.insync.replicas`. Menciona consumer groups.
4. **Flujo en vivo:** entra a un producto en la tienda. Muestra el mensaje en Topics > Messages y cómo el servicio de analytics reacciona.
5. **Monitoreo (offsets y lag):** detén el consumidor (ms-analytics), genera varios eventos y en Consumers verás el `Lag` subir. Enciéndelo y baja a 0.
6. **Tolerancia a fallos:** `docker stop kafka-2`. En Kafka UI el tópico queda con 2 réplicas sincronizadas, pero se sigue publicando y consumiendo. `docker start kafka-2` y se recupera.
   No apagues dos brokers a la vez en la demo: los consumidores no pueden confirmar offsets. Explícalo de palabra.

## 6. En AWS

- Una sola instancia t3.large basta para todo este compose.
- En `.env`, `KAFKA_EXTERNAL_HOST` = IP **privada** de la instancia (los microservicios están en la misma VPC).
- Security Group: 9092 a 9094 solo desde la VPC (ya cubierto por la regla interna) y 8090 (Kafka UI) solo desde tu IP.
- No expongas los puertos de Kafka a internet.
- Ojo: este compose no se pudo ejecutar donde se escribió (no había Docker en marcha). El archivo está validado, pero si algo falla al primer arranque, mira `docker compose logs <servicio>`.
