# Diseño de streaming - Lab 2b

**Curso:** ST1630-2026-2 - **Semana:** S7 - **Fecha:** 2026-09-13  
**Estudiantes:** Emmanuel Cortes, Sara Hurtado, Juan Jose Osorio y Mariana Sanchez

## Evidencia de la ejecucion

- Fuente: pedidos-ventas en Kafka, con 4 particiones.
- Tres publicaciones: 3.000 pedidos confirmados por el productor.
- Resultado Delta: 12 filas, suma de num_pedidos igual a 3.000.
- Las dos publicaciones de la ventana actual conservaron las mismas 6
  claves y actualizaron sus agregados mediante MERGE.
- Claves duplicadas por (window_start, window_end, region): 0.
- Trigger configurado: 30 segundos.
- Ventana implementada: 5 minutos.
- Watermark implementado: 10 minutos.
- Ruta de checkpoint: .lab2b/lake/checkpoints/lab2b-kafka.

## Pregunta 1 - Ventana y watermark

¿Por que una ventana de 5 minutos y un watermark de 10 minutos son
adecuados para este productor? Explica tambien que ocurre con un evento
que supera el watermark respecto al evento mas reciente observado.

**Respuesta:** Elegimos ventanas fijas de 5 minutos porque el productor genera los 1.000 pedidos en muy poco tiempo, así podemos ver resultados con frecuencia y repetir la prueba varias veces sin tener que esperar demasiado. También usamos un watermark de 10 minutos para darle margen a los eventos que lleguen tarde, permitiendo retrasos de hasta dos ventanas. Al mismo tiempo, evitamos que Spark tenga que guardar el estado de esas ventanas por tiempo indefinido.

Spark calcula el watermark a partir del mayor kafka_time observado. Si  un pedido llega con un tiempo de evento anterior al limite marcado por el watermark y la ventana  correspondiente ya fue cerrada, Spark lo descarta de esta agregacion con estado.

## Pregunta 2 - Checkpoint vs. commit manual

¿Quien recuerda el progreso en Structured Streaming? Compara el
`checkpointLocation` con el `consumer.commit()` manual del Lab 2a.

**Respuesta:** Spark Structured Streaming guarda automáticamente el progreso del procesamiento en checkpointLocation. Ahí registra qué mensajes de Kafka ya fueron procesados y también guarda información necesaria de las ventanas.

Se parece a consumer.commit() porque ambos permiten continuar desde donde se quedó el procesamiento si ocurre un reinicio. La diferencia es que con consumer.commit() nosotros controlamos cuándo guardar el progreso, mientras que Spark lo hace automáticamente y además guarda el estado de las ventanas y el watermark.

## Pregunta 3 - outputMode

Justifica `outputMode("update")` y explica que ocurriria con `complete`
y `append` para esta agregacion con ventanas.

**Respuesta:** Usamos update porque Spark solo envía los resultados que cambiaron en cada micro-batch. Esto sirve porque una misma ventana puede seguir recibiendo datos antes de cerrarse. Con complete, Spark volvería a enviar todos los resultados cada vez, aunque muchos no hayan cambiado, lo que genera trabajo innecesario. Con append, Spark tendría que esperar a que la ventana esté completamente cerrada para mostrar el resultado.En cambio  update permite ir actualizando los resultados mientras la ventana sigue abierta.

## Pregunta 4 - La llave del MERGE

Explica por que la llave cambio de `pedido_id` a
`(window_start, window_end, region)` y por que el resultado sigue siendo
idempotente.

**Respuesta:** En el Lab 2a cada fila era un pedido, así que pedido_id servía como identificador único. En este caso, cada fila representa el total de pedidos de una región dentro de una ventana de tiempo, por eso pedido_id ya no sirve.

la única clave es: (window_start, window_end, region)

Con esta  combinación, si llegan más pedidos para la misma región y ventana, el MERGE actualiza el resultado existente en vez de crear otro registro. Esto hace que el proceso sea idempotente, porque si se vuelve a procesar el mismo resultado, se actualiza la misma fila y no se generan duplicados.

## Pregunta 5 - Trigger interval

Analiza el trigger de 30 segundos frente a la ventana de 5 minutos y el
trade-off entre latencia y costo de micro-batches mas frecuentes.

**Respuesta:** Usamos processingTime= 0 seconds para que Spark procese los datos cada 30 segundos. Como la ventana dura 5 minutos, puede actualizar los resultados varias veces antes de que termine.Si usamos un tiempo más corto, los resultados aparecen más rápido, pero Spark tiene que trabajar más veces. Si usamos un tiempo más largo, se reduce el trabajo, pero los resultados tardan más en mostrarse. Por eso 30 segundos es un punto medio adecuado para el laboratorio, ademas se menciono en clase.

## Pregunta 6 - Reinicio del job

Explica que ocurre al detener y reiniciar el pipeline con el mismo
checkpoint y por que el sink Delta protege el resultado si un batch se
reintenta.

**Respuesta:** Con el mismo checkpoint, Spark recuerda hasta qué punto había procesado Kafka y continúa desde ahí. Si ocurre una falla, puede repetir algún micro-batch que no alcanzó a quedar comiteado so no genera duplicados porque usamos MERGE en vez de append. El MERGE busca la combinación:  (window_start, window_end, region)  Si esa fila ya existe, la actualiza; si no existe, la crea. Si borramos el checkpoint, Spark puede volver a leer los datos desde el inicio, pero el MERGE sigue evitando que se creen claves duplicadas.

## Pregunta 7 - Kinesis

No realizada. El rol temporal de AWS Academy permitio autenticar y listar
streams, pero respondio AccessDeniedException para la creacion del Stream en kinesis.
