# Clase práctica 4 — Streaming: logs, ventanas y garantías

**Nombre:** Eliana Gómez Coumroglon

**student_id:** elianacoumroglon

**Escala:** small

## Propósito

Ver funcionando los conceptos de la clase de streaming sobre la misma plataforma de e-commerce: un log con consumidores independientes, ingesta incremental de archivos, ventanas por tiempo del evento con datos tardíos, garantías de entrega y Change Data Capture.

La práctica es **demostrativa**: los notebooks no se modifican, sólo se ejecutan en orden. La entrega es un README con 20 preguntas cortas que se responden mirando los resultados.

Se usa **Structured Streaming en Databricks Free Edition** con tablas Delta y `trigger(availableNow=True)`: cada consulta procesa lo pendiente y termina, y su checkpoint recuerda hasta dónde llegó. No hace falta Kafka ni crear un Job.

## Requisitos previos

- Clase 2 ejecutada: se usan `silver_customers` y `silver_products` para enriquecer los eventos.
- Mismo `student_id` y misma escala que en las clases anteriores.
- Compute serverless. No hay que instalar librerías.
- Para importación manual, conservar `clase-04/` y `common/` como carpetas hermanas.

## Secuencia

Ejecutá los notebooks en orden, celda por celda. Cada uno reinicia sus propias tablas `stream_*` y su carpeta `streaming_lab/` al comenzar, así que se puede volver a correr desde el principio sin pasos manuales. Los notebooks 03 y 04 usan el resultado del 02.

| Notebook | Tema de la teoría | Qué se ve | Minutos |
|---|---|---|---:|
| `00_preflight.ipynb` | — | Verifica las tablas de la clase 2 y muestra los eventos del experimento | 5 |
| `01_log_y_offsets.ipynb` | Pub/Sub, log, offsets, fan-out, replay | Una tabla Delta como *topic*; dos consumidores a distinto ritmo; replay desde el inicio | 20 |
| `02_ingesta_y_enriquecimiento.ipynb` | Ingesta incremental, calidad, join stream-tabla | Cinco llegadas de archivos con Auto Loader; cuarentena; enriquecimiento con clientes y productos | 20 |
| `03_tiempo_y_ventanas.ipynb` | Event time, watermark, ventanas, tardíos | Watermark por llegada; `late_ok` aceptado y `late_bad` descartado; tumbling, hopping y session; append vs. update | 30 |
| `04_garantias_y_fallas.ipynb` | At-least-once, exactly-once, idempotencia | Duplicados, reinicio con checkpoint, checkpoint perdido y escritura idempotente con `MERGE` | 20 |
| `05_cdc.ipynb` | CDC, estado y stream, log compaction | Change Data Feed de una tabla de clientes y reconstrucción de la tabla desde el log | 15 |

## El experimento

Un productor ficticio envía compras del 12/03/2026 entre las 12:00 y las 12:45 (UTC) en cinco llegadas. Con escala `small` el bloque base se repite 20 veces con otros IDs y clientes.

| Llegada | Eventos del bloque base | Qué pasa |
|---|---|---|
| `stream_001` | Compras a 12:00, 12:01 (`e002`) y 12:04 | Todavía no se cierra ninguna ventana |
| `stream_002` | Compras a 12:06 y 12:08; `late_ok` (12:03); `e002` repetido; dos errores de calidad | Tardío tolerado, duplicado y cuarentena |
| `stream_003` | Compras a 12:25 y 12:26 | El watermark avanza y se cierran las primeras ventanas |
| `stream_004` | `late_bad` (12:02) y una compra a 12:27 | `late_bad` es más viejo que el watermark: se descarta |
| `stream_005` | Control a 12:45 (importe cero) | Hace avanzar el tiempo para cerrar la última ventana |

## Tablas que se crean

```text
01: stream_topic_payments, stream_consumer_fraud, stream_consumer_reports, stream_consumer_replay
02: stream_bronze_events, stream_silver_classified
03: stream_arrivals, stream_gold_tumbling, stream_gold_hopping, stream_gold_sessions, stream_gold_updates
04: stream_sink_naive, stream_sink_dedup, stream_sink_merge
05: stream_cdc_customers, stream_cdc_log
Volumen: landing/streaming_lab/<notebook>/ (archivos de entrada y checkpoints)
```

Ninguna tabla de las clases anteriores se modifica.

## Preguntas

Las preguntas son cortas: buscan comprobar que entendiste cada concepto y sus ventajas y desventajas. Cuando piden un dato, copiá el número que te dio el notebook (puede variar según tu escala). Alcanza con una a tres oraciones por respuesta.

### Log y offsets (`01_log_y_offsets`)

1. En el ejemplo en Python, ¿cuántos mensajes quedaron en la cola y cuántos en el log después de leer tres? ¿Qué diferencia hay entre una cola tradicional y un log como Kafka?

En la cola quedaron 2 mensajes y en el log quedaron 5. Cuando se lee un mensaje se borra de la cola (ya no puede volver a ser leído). En cambio, leer un mensaje no modifica el log, solo avanza su offset. 

2. Después de la llegada 2, ¿cuántos mensajes había leído el consumidor `antifraude` y cuántos `reportes`? ¿El atraso de `reportes` afectó a `antifraude`? ¿Por qué?

Después de la llegada 2 el consumidor antifraude había leído aproximadamente 180 mensajes, reportes había leído 0 mensajes. 
El retraso de reportes no afectó a antifraudes. Esto se debe a que cada consumidor lleva su propio offset y leer no borra ni modifica el log.

3. ¿Qué es un offset y dónde lo guarda cada consumidor en esta práctica?

El Offset es la posición del log hasta la que un consumidor ya leyó. Indica cuál es la próxima versión a leer y por eso le permite retomar donde quedó. Cada consummidor lo guarda en el checkpoint de su propia consulta. 

4. ¿Cuántos mensajes leyó el replay? ¿Qué propone la arquitectura Kappa para reprocesar datos y en qué se diferencia de Lambda?

El replay leyó 280 mensajes. La arquitectura Kappa propone un único código de streaming. Lambda en cambio mantiene dos sistemas con la misma lógica escriba dos veces. 

### Ingesta y enriquecimiento (`02_ingesta_y_enriquecimiento`)

5. ¿Cuántas filas nuevas procesó Bronze en cada llegada y cuántas al reejecutar sin archivos nuevos? ¿Qué componente evita leer dos veces el mismo archivo?

Bronze procesó 60, 120, 40, 40 y 20 filas nuevas. Al reejecutar sin archivos nuevos se procesaron 0 filas. El componente que evita leer dos veces el mismo archivo es Auto Loader (`cloudFiles`): recuerda en su checkpoint qué archivos ya leyó, así que cada ejecución procesa sólo los archivos nuevos.

6. ¿Qué motivos de rechazo aparecieron en la cuarentena y en qué llegada? ¿Por qué Bronze guarda el texto original en lugar de descartar los registros con errores?

Los motivos de rechazo que aparecieron en la cuarentena fueron 'INVALID_AMOUNT' y 'UNKNOWN_CUSTOMER' en la llegada 'stream_002'. Bronze guarda el texto original en lugar de descartar los registros con errores porque así el dato crudo nunca se pierde: si cambia el contrato o se corrige una regla de calidad, se puede reprocesar desde Bronze sin pedir de nuevo los datos.

7. ¿Qué columnas agregó el join con `silver_customers` y `silver_products`? ¿Qué tipo de join es (stream-stream, stream-tabla o tabla-tabla) según la teoría?

El join con 'silver_customers' agregó la columna 'country' y el join con 'silver_products' agregó la columna 'category'. Es un join stream-tabla porque cada evento del stream se enriquece consultando tablas estáticas que no cambian mientras llegan los eventos.

### Tiempo y ventanas (`03_tiempo_y_ventanas`)

8. ¿Qué diferencia hay entre event time y processing time? Usá `late_ok` como ejemplo.

El event time es el momento en que el evento ocurrió realmente, y el processing time es el momento en que el sistema lo recibe y lo procesa. Pueden diferir: los eventos pueden llegar desordenados o con retraso. 

**Ejemplo:** `late_ok_000` ocurrió a las **12:03** pero se procesó en la **llegada 2**, después de haber visto un evento de las 12:04. Como el watermark todavía estaba en **11:54**, el evento es más nuevo que el watermark y **se acepta**. En cambio `late_bad_000` ocurrió a las **12:02** y llegó cuando el watermark ya estaba en **12:16**: es más viejo, así que **se descarta**.

9. ¿Cuánto valía el watermark al terminar cada llegada? ¿Cómo se calcula a partir del máximo event time visto?

El watermark al terminar cada llegada valía: **11:54** (llegada 1), **11:58** (2), **12:16** (3), **12:17** (4) y **12:35** (5). Se calcula como el máximo event time visto menos el retraso tolerado (10 minutos). 

10. ¿En qué llegada aparecieron las primeras ventanas tumbling en modo append y por qué no antes?

Las primeras ventanas tumbling en modo append aparecieron en la llegada 3: 4 ventanas emitidas, entre ellas la de 12:00 a 12:10. No aparecieron antes porque en modo append una ventana se emite una sola vez, con su valor final, recién cuando el watermark supera el fin de la ventana.

11. ¿Qué pasó con `late_bad`? ¿Por qué se aceptó `late_ok` y se descartó `late_bad`?

`late_bad` (event time **12:02**) llegó en la llegada 4, cuando el watermark ya estaba en **12:16**. Como es más viejo que el watermark, **se descartó**: no sumó en ninguna ventana (los 20 eventos tardíos de esa llegada). La diferencia con `late_ok` (event time 12:03) está en cuánto había avanzado el watermark: `late_ok` llegó en la llegada 2, con el watermark en **11:54**, y 12:03 es más nuevo que eso, así que **se aceptó** y se sumó a su ventana. Ambos llegaron tarde, pero `late_ok` llegó dentro del margen de tolerancia (10 minutos) y `late_bad` ya lo había superado: el watermark ya había cerrado las ventanas donde habría contado.

12. Mirando el gráfico de ventanas, ¿en qué se diferencian tumbling, hopping y session? Da un ejemplo de negocio para cada una.

Tumbling usa ventanas de tamaño fijo, consecutivas y sin superposición, así que cada evento cae en una sola ventana. Sirve, por ejemplo, para las ventas totales por cada 5 minutos o por hora. 

Hopping también usa ventanas de tamaño fijo, pero se superponen porque avanzan menos de lo que miden (10 min con salto de 5), así que un evento puede caer en varias ventanas. Sirve para un promedio móvil de los últimos 10 minutos, actualizado cada 5, para detectar picos. 

Session usa ventanas de tamaño variable, definidas por la actividad de cada cliente: se abren con su primer evento y se cierran tras 5 minutos sin compras (el cliente 0 tiene sesiones de 4 y 3 compras, y el cliente 1 una de 2). Sirve para medir cuántas compras hace un cliente por visita en un e-commerce.

13. Para la ventana [12:00, 12:05) del canal card, ¿cuántas veces y con qué valores se emitió en modo update y en modo append? ¿Qué ventaja y qué desventaja tiene cada modo?

En modo update la ventana [12:00, 12:05) del canal card se emitió 2 veces: en la llegada 1 con 40 compras y monto 6.000 y en la llegada 2, corregida cuando llegó `late_ok`, con 60 compras y monto 8.400. 

En modo append se emitió 1 sola vez, en la llegada 3, con el valor final (60 compras, 8.400). 

Update tiene la ventaja de dar resultados rápidos (apenas hay datos) y la desventaja de que la misma ventana se emite varias veces y el consumidor debe tolerar recibirla repetida y corregida. Append tiene la ventaja de emitir cada ventana una sola vez, con su valor final (más simple para el consumidor) y la desventaja de la demora: hay que esperar a que el watermark cierre la ventana.

14. ¿Qué se gana y qué se pierde si el watermark fuera de 1 minuto en lugar de 10?

Con un watermark de 1 minuto en lugar de 10, las ventanas se cierran antes, pero se pierden más eventos tardíos porque el margen de tolerancia es menor. Por ejemplo, `late_ok` (12:03, llegó con el máximo en 12:04) quedaría a sólo 1 minuto del máximo y por poco sobreviviría, pero cualquier evento con más de 1 minuto de retraso se descartaría, así que los resultados serían menos completos. Con 10 minutos pasa lo contrario: se toleran eventos más atrasados y los resultados son más completos, pero hay que esperar más para obtener las ventanas cerradas.

### Garantías y fallas (`04_garantias_y_fallas`)

15. ¿Cuántas filas quedaron en el destino sin deduplicar y cuántas deduplicando por `event_id`? ¿Por qué el productor puede enviar el mismo evento dos veces?

En el destino quedaron 220 filas sin deduplicar y 200 deduplicando por 'event_id', es decir 20 duplicados. El productor puede enviar el mismmo evento dos veces porque ante una falla se reintenta el envío para no perder datos.

16. ¿Qué pasó al reiniciar con el mismo checkpoint y qué pasó al perderlo? ¿Qué garantía de entrega se observa en cada caso?

Al reiniciar con el mismo checkpoint, el destino siguió en 200 filas: la consulta retomó desde su offset y no reprocesó nada, así que no hubo duplicados. Al perder el checkpoint y escribir en modo append, la consulta olvidó qué archivos había leído, los releyó todos y el destino pasó a 400 filas (el doble, se volvió a leer todo).

17. ¿Por qué con `MERGE` el resultado no cambió aunque se perdiera el checkpoint? ¿Qué significa que una escritura sea idempotente?

Con `MERGE` el resultado no cambió porque cada microbatch se escribe haciendo un `MERGE` por `event_id`: si el evento ya está en el destino, no se inserta de nuevo. Que una escritura sea idempotente significa que aplicarla más de una vez tiene el mismo efecto que aplicarla una sola vez, de modo que los reintentos y las relecturas no generan duplicados.

### CDC (`05_cdc`)

18. ¿Cuántos eventos de cambio generó el log y de qué tipos? ¿Por qué un `UPDATE` genera dos eventos?

El log generó 13 eventos de cambio: 6 `insert`, 6 de tipo update y 1 `delete`. 
Un `UPDATE` genera dos eventos porque el log registra la fila antes y después del cambio. Así se sabe qué valor había y a cuál pasó (por ejemplo, el cliente 2 pasó de UY a CL y luego de CL a BR), un historial que la tabla, que sólo muestra el estado actual, no conserva.

19. ¿Se pudo reconstruir la tabla a partir del log? ¿Qué información tiene el log que la tabla actual no tiene?

Si, la tabla se pudo reconstruir a partir del log: quedándose con el último cambio de cada `customer_id` y descartando los que terminan en `delete`, se obtuvo una tabla igual a la original. El log tiene información que la tabla actual no tiene: el historial de cambios. La tabla sólo muestra el estado actual, por eso con el log se pueden construir otras vistas sin tocar la base original.

### Cierre

20. Según la comparación de motores de la teoría, ¿cuándo elegirías Spark Structured Streaming, Flink o Kafka Streams? Mencioná una ventaja de cada uno.

## Entrega

La publicación, el acceso público y el envío a los profesores siguen el [formato común de entrega](../README.md#formato-común-de-entrega).

La entrega es **un único `README.md`** con las respuestas a las 20 preguntas, dentro de `resolucion-practica-4/` en tu repositorio personal:

```text
resolucion-practica-4/
└── README.md
```

El README debe incluir nombre, `student_id` y escala. Podés partir de la [plantilla](PLANTILLA_ENTREGA.md). No hace falta exportar los notebooks.

No incluyas datos generados, archivos del volumen, checkpoints, credenciales ni tokens.

## Problemas frecuentes

- **`Falta ... silver_customers`**: ejecutá la clase 2 con el mismo `student_id` y escala.
- **`Ejecutá primero 02_ingesta_y_enriquecimiento`**: los notebooks 03 y 04 usan la tabla que crea el 02.
- **Una celda tarda**: cada consulta streaming arranca, procesa y termina; en serverless cada una puede tardar algunos segundos. No interrumpas la celda.
- **Querés empezar de nuevo**: volvé a ejecutar el notebook desde el principio; él mismo borra sus tablas y checkpoints.
