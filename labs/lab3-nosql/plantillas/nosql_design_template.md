# nosql_design.md — Lab 3: Comparativa de motores NoSQL

**Curso:** ST1630 · **Semana:** S10 · **Fecha:** _(completar)_
**Estudiante:** _(nombre y correo @eafit.edu.co)_
**Usuario del dataset:** _(el mismo que usaste en todos los comandos)_
**Huella de `verificar.py`:** _(los 12 caracteres que imprime al final)_
**Resultado de `verificar.py`:** _(p. ej. 21/21 verificaciones correctas)_

> Copia este archivo a `entregas/<tu-usuario>/nosql_design.md`. Responde con la evidencia de
> TU ejecución: los tiempos y conteos que imprimieron tus scripts, tus llaves primarias, tus
> resultados. Una respuesta que solo repite teoría sin citar tu corrida cuenta como parcial.

## Parte 1 — Documento y columnar ancho

### Pregunta 1 — El documento de MongoDB

¿Qué datos del cliente y del producto embebiste en el documento del pedido y cuáles dejaste
fuera? ¿Guardaste el precio del catálogo o el `precio_unit` pagado, y por qué importa?

Si un cliente cambia de nombre mañana, ¿qué pasa con sus pedidos ya guardados? ¿Es un error o
el comportamiento correcto para un pedido? (Piensa en *normalización vs. desnormalización* del
cap. 2 de Kleppmann.)

→ [tu respuesta]

### Pregunta 2 — La llave primaria de Cassandra

Copia aquí tus dos `CREATE TABLE`. Para cada uno explica la partition key y las clustering
columns que elegiste.

Para `ventas_por_region_dia`: ¿por qué no basta con `region` como partition key? Estima cuántas
filas tendría una partición con tu llave y con solo `region`, si la empresa llega a 500.000
pedidos diarios durante un año. ¿Qué problema tendría una partición así?

→ [tu respuesta]

### Pregunta 3 — Duplicar para consultar

Un mismo pedido quedó escrito en dos tablas de Cassandra. ¿Por qué en Cassandra esto es normal y
en un modelo relacional sería un problema? Si mañana el negocio pide "ventas por canal y día",
¿qué harías?

→ [tu respuesta]

## Parte 2 — Clave-valor y grafo

### Pregunta 4 — El top 10 en Redis

Compara tus tiempos de Q3 en Redis y en MongoDB (cópialos de la salida de tus scripts). ¿De
dónde sale la diferencia? ¿Qué trabajo hace Redis al escribir que MongoDB hace al leer?

¿Qué pasa con tu ranking si el contenedor de Redis se reinicia? ¿Usarías Redis como la base de
datos principal de los pedidos? ¿Por qué?

→ [tu respuesta]

### Pregunta 5 — Recomendaciones: documento vs. grafo

Compara tu Q4 en MongoDB y en Neo4j: cuántos pasos o consultas necesitaste en cada uno, y sus
tiempos (cópialos). ¿Fue Neo4j más rápido en tu medición? Si no lo fue, ¿por qué crees que pasó,
y en qué situación sí esperarías que ganara?

Si la consulta cambiara a tres saltos ("productos comprados por clientes que compraron lo mismo
que los clientes parecidos a ti"), ¿cómo cambiaría cada versión?

→ [tu respuesta]

## Parte 3 — Stream con reentregas

### Pregunta 6 — Cuánto se equivoca el sink ingenuo

En tu corrida de `stream_lab.py`, ¿cuántas unidades de más contó el sink ingenuo? Esas unidades
vienen de dos fuentes distintas: ¿cuáles son? Relaciónalo con la semántica at-least-once y el
momento del commit del Lab 2a.

→ [tu respuesta]

### Pregunta 7 — Idempotencia en Redis y en Cassandra

Explica tu mecanismo de idempotencia en Redis: cómo recuerdas los eventos ya vistos y por cuánto
tiempo. ¿Sigue siendo correcto si dos consumidores procesan el mismo evento al mismo tiempo?
¿Qué cambiarías para que lo fuera?

Explica por qué tu tabla `ventas_stream` es idempotente. ¿Por qué una columna de tipo `counter`
de Cassandra (que se incrementa con `UPDATE ... SET x = x + 1`) NO lo sería?

→ [tu respuesta]

## Parte 4 — Comparativa

### Pregunta 8 — Tabla comparativa con tus números

| | MongoDB | Cassandra | Redis | Neo4j |
|---|---|---|---|---|
| Modelo de datos | | | | |
| Tiempo de carga (s) | | | | |
| Consultas que resolviste y su mediana (ms) | | | | |
| Qué consulta le resultó natural | | | | |
| Qué consulta le resultaría difícil o imposible | | | | |
| Cómo escala (más datos, más nodos) | | | | |

### Pregunta 9 — Si solo pudieras elegir uno

Para este negocio, si solo pudieras operar uno de los cuatro motores, ¿cuál elegirías y qué
sacrificarías? Tu Cassandra corre con factor de replicación 1: ¿qué pasa si ese nodo se cae? Con
factor 3 y consistencia `QUORUM`, ¿qué ganarías y qué pagarías? (Kleppmann, cap. 1: fiabilidad y
escalabilidad.)

→ [tu respuesta]
