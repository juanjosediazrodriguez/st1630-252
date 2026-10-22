# Lab 3 · Comparativa de motores NoSQL

**Curso:** ST1630 Sistemas Intensivos en Datos — EAFIT
**Semana:** S10
**Modalidad:** individual · **opcional**
**Uso de la nota:** supletorio de quices (ver abajo)
**Tiempo estimado:** 6-8 horas
**Entrega:** PR a este repo antes del **viernes 30 de octubre de 2026, 23:59**

## Para qué sirve este lab

Este lab es **opcional**. Su nota se usa como **supletorio de los quices**: si no presentaste
un quiz, la nota de este lab reemplaza la de ese quiz. Si presentaste todos los quices, el lab
sigue siendo opcional. El lab cubre los temas de los quices:

| Quiz | Tema | Dónde lo trabajas aquí |
|---|---|---|
| 1 | Kleppmann caps. 1-2 | Partes 1 y 2: modelos de datos (documento, columnar, clave-valor, grafo), relaciones muchos-a-muchos, escalabilidad de una partición |
| 2 | Streaming y Kafka | Parte 3: ingesta at-least-once con reentregas e idempotencia, igual que en el Lab 2a |
| 3 | Modelos de datos y NoSQL | Todo el lab |

Es **individual**: cada estudiante entrega su propio PR con su propio dataset.

## Objetivo

Vas a modelar el **mismo negocio** (clientes, productos y pedidos de un e-commerce
colombiano) en cuatro motores NoSQL, uno por modelo de datos, y a comprobar con tus propias
mediciones para qué sirve cada uno:

| Motor | Modelo | Consulta que le toca |
|---|---|---|
| MongoDB | Documento | Q1: el pedido completo en una sola lectura · Q3 y Q4 para comparar |
| Cassandra | Columnar ancho | Q2a: ventas de una región en un día · Q2b: últimos pedidos de un cliente |
| Redis | Clave-valor | Q3: top 10 de productos en tiempo real |
| Neo4j | Grafo | Q4: recomendaciones "quienes compraron lo mismo que tú también compraron…" |

Al terminar deberías poder:
- **Diseñar** el modelo de cada motor a partir de las consultas que debe servir (y no al revés).
- **Explicar** por qué la misma consulta es trivial en un motor y costosa en otro.
- **Garantizar** que la ingesta de un stream con reentregas cuente cada evento una sola vez.
- **Justificar** qué motor usarías para cada necesidad del negocio, con tus propios números.

## Tu dataset es único

Cada estudiante genera su dataset a partir de su **usuario EAFIT** (la parte antes de
`@eafit.edu.co`). Tus conteos, tiempos y recomendaciones son tuyos y de nadie más, y la
autoevaluación produce una **huella** que depende de tu usuario y de tus resultados. Usa
siempre el mismo usuario en todos los comandos.

## Parte 0 — Preparar el entorno (30 min)

Requisitos: Docker y Docker Compose, con al menos **4 GB de memoria** asignados a Docker.

```bash
cd labs/lab3-nosql
docker compose up -d --build      # la primera vez descarga ~2 GB de imágenes
docker compose ps                 # espera a que mongo, cassandra, redis y neo4j digan "healthy"
```

Cassandra tarda 1-2 minutos en quedar lista; el contenedor `app` arranca solo cuando los
cuatro motores están sanos. **Todos los comandos de Python del lab se corren dentro de `app`**
(trae Python 3.11 y los drivers; el de Cassandra no funciona en Python 3.12 o superior):

```bash
docker compose exec app bash
```

Dentro del contenedor, genera tu dataset y copia los scripts a tu carpeta de entrega. **Trabaja
siempre sobre tu copia**, nunca sobre `scripts/`:

```bash
python datos/gen_dataset.py --usuario <tu-usuario>
mkdir -p entregas/<tu-usuario>
cp -r scripts entregas/<tu-usuario>/scripts
```

**Verifica:** `python entregas/<tu-usuario>/scripts/verificar.py --usuario <tu-usuario>` debe
terminar con `0/21 verificaciones correctas … 21 pendientes`.

En adelante, `S=entregas/<tu-usuario>/scripts` y `U=<tu-usuario>`.

## Parte 1 — Documento y columnar ancho (2.5 horas)

### 1.1 MongoDB (`mongo_lab.py`, TODO 1-5)

Diseña el documento de un pedido para que **Q1** (el pedido completo con nombres de cliente y
productos) salga de **una sola lectura** por `_id`. Decide qué embebes y qué no, crea los
índices que necesitan Q3 y Q4, y resuelve Q3 y Q4 con aggregation pipelines. Q4 en un modelo de
documento es incómodo **a propósito**: es la relación muchos-a-muchos del cap. 2 de Kleppmann.

```bash
python $S/mongo_lab.py --usuario $U
python $S/verificar.py --usuario $U --parte mongo
```

### 1.2 Cassandra (`cassandra_lab.py`, TODO 1-5)

En Cassandra se diseña **una tabla por consulta**. Escribe los `CREATE TABLE` de
`ventas_por_region_dia` (Q2a) y `pedidos_por_cliente` (Q2b), con la `PRIMARY KEY` que permita
responder cada consulta leyendo **una sola partición**, en el orden correcto y **sin
`ALLOW FILTERING`**. La llave primaria es la decisión central del lab: justifícala.

```bash
python $S/cassandra_lab.py --usuario $U
python $S/verificar.py --usuario $U --parte cassandra
```

**Error frecuente:** que Q2a dé los conteos bien pero los 5 pedidos "más recientes" mal. El
orden dentro de una partición lo decide el `CLUSTERING ORDER BY`, no un `ORDER BY` en la
consulta.

## Parte 2 — Clave-valor y grafo (2 horas)

### 2.1 Redis (`redis_lab.py`, TODO 1-3)

Mantén el ranking de unidades vendidas en un **sorted set** que se actualiza al escribir, y
responde Q3 sin recorrer los pedidos. Compara el tiempo con tu Q3 de MongoDB.

```bash
python $S/redis_lab.py --usuario $U
python $S/verificar.py --usuario $U --parte redis
```

### 2.2 Neo4j (`neo4j_lab.py`, TODO 1-2)

Carga el grafo `(:Cliente)-[:COMPRO]->(:Producto)` con Cypher (`UNWIND` + `MERGE`) y escribe Q4
como un recorrido de dos saltos. Compara la consulta (y su tiempo) con tu Q4 de MongoDB. Puedes
explorar el grafo en http://localhost:7474 (usuario `neo4j`, clave `lab3-st1630`).

```bash
python $S/neo4j_lab.py --usuario $U
python $S/verificar.py --usuario $U --parte neo4j
```

## Parte 3 — Ingesta de un stream con reentregas (1.5 horas)

`stream_lab.py` simula las ventas en vivo con la misma semántica **at-least-once** del Lab 2a:
la fuente reentrega eventos (el mismo `event_id` llega más de una vez) y el consumidor se cae a
mitad del stream y reprocesa desde su último commit. El script ya incluye un sink "ingenuo" que
suma todo, para que veas cuánto se equivoca.

Tu trabajo (TODO 1-3): que el ranking en Redis y la tabla en Cassandra cuenten **cada venta una
sola vez**.

```bash
python $S/stream_lab.py --usuario $U
python $S/verificar.py --usuario $U --parte stream
```

**Verifica:** el script imprime las unidades reales, las del sink ingenuo (que deben salir de
más) y las de tus dos sinks, que deben coincidir exactamente con las reales.

## Parte 4 — Análisis y entrega (1.5 horas)

1. Corre la verificación completa y guarda la salida:
   ```bash
   python $S/verificar.py --usuario $U > entregas/$U/verificacion.txt
   ```
2. Copia `plantillas/nosql_design_template.md` a `entregas/<tu-usuario>/nosql_design.md` y
   responde las preguntas **con tus números**: los tiempos y conteos que imprimieron tus
   scripts, no cifras teóricas. Incluye la huella de `verificar.py`.
3. Dibuja en [draw.io](https://app.diagrams.net/) los cuatro modelos de datos (cómo quedó el
   mismo pedido en cada motor) y exporta `diagrama_modelos.drawio` y `diagrama_modelos.png`.
4. Completa `bitacora_delegacion.md` a partir de `plantillas/bitacora_delegacion_template.md`.

Para apagar todo al terminar: `docker compose down -v` (borra también los datos de los motores).

## Entregable

Pull request **individual** a este repo, con título `lab(3): <tu-usuario> — comparativa NoSQL`:

```
labs/lab3-nosql/entregas/<tu-usuario>/
├── scripts/                      # tu copia de scripts/, con los TODO completos
├── verificacion.txt              # salida completa de verificar.py
├── nosql_design.md
├── diagrama_modelos.drawio
├── diagrama_modelos.png
└── bitacora_delegacion.md
```

**No subas:** `datos/<tu-usuario>/` (se regenera con tu usuario y está en `.gitignore`), ni
cambios a `scripts/`, `plantillas/` o cualquier archivo fuera de tu carpeta de entrega.

## Rúbrica

| Criterio | Peso | Completo | Parcial | Incompleto |
|---|---|---|---|---|
| Modelado y carga en los 4 motores | 25% | Documento de MongoDB que responde Q1 en una lectura, tablas de Cassandra con llaves primarias que sirven cada consulta sin `ALLOW FILTERING`, sorted set en Redis y grafo con `MERGE` en Neo4j; decisiones justificadas | Los motores cargan, pero alguna llave o estructura no sirve bien su consulta (p. ej. requiere `ALLOW FILTERING` u ordenar en Python), o las decisiones no se justifican | Uno o más motores no se cargan |
| Consultas Q1-Q4 | 25% | Las verificaciones de MongoDB, Cassandra, Redis y Neo4j en `verificacion.txt` están en ✓ | Algunas consultas en ✗ o pendientes | La mayoría sin implementar |
| Ingesta idempotente (Parte 3) | 20% | Las dos verificaciones del stream en ✓, con el mecanismo de idempotencia explicado para Redis y para Cassandra | Solo uno de los dos sinks es idempotente, o funciona sin explicar por qué | Ninguno de los dos sinks es idempotente |
| `nosql_design.md` | 25% | Las preguntas respondidas con evidencia de tu propia ejecución (tus tiempos, tus conteos, tu huella) y criterio propio para elegir motor | Respuestas genéricas que repiten teoría sin conectarla con tu ejecución | Menos de la mitad de las preguntas respondidas |
| Diagrama de los modelos | 5% | Los 4 modelos con el mismo pedido representado en cada uno, en draw.io | Diagrama incompleto o genérico | Sin diagrama |

`verificacion.txt` debe corresponder a **tu** usuario: el profesor puede regenerar tu dataset y
volver a correr tus scripts para comprobar la huella.

## Bitácora de delegación

Recuerda incluir tu bitácora (ver `../../docs/politica-ia.md`). Qué se puede delegar en este lab:

| Tarea | ¿Se puede delegar? | Justificación |
|---|---|---|
| Sintaxis puntual (aggregation pipelines, CQL, comandos de Redis, Cypher) | Sí | Bajo valor de aprendizaje memorizar sintaxis de cuatro lenguajes |
| Troubleshooting de Docker y de los drivers | Sí | Configuración de entorno, no decisión de diseño |
| Qué embeber en el documento de MongoDB (TODO 1) | **No** | Es la decisión de modelado de la Parte 1.1 |
| `PRIMARY KEY` de las tablas de Cassandra (TODO 1-2 y la de `ventas_stream`) | **No** | Es el objetivo central del lab: modelar a partir de la consulta |
| Mecanismo de idempotencia de los sinks (Parte 3) | **No** | Conecta directamente con lo que aprendiste en el Lab 2a |
| `nosql_design.md` | **No** | Es el entregable de análisis; debe reflejar tus decisiones y tus números |

## Troubleshooting

| Problema | Causa probable | Solución |
|---|---|---|
| `docker compose ps` muestra `cassandra` en `starting` varios minutos | Cassandra tarda en arrancar, más con poca memoria | Espera 2-3 minutos; si no llega a `healthy`, sube la memoria de Docker a 4 GB o más |
| `app` no arranca | Algún motor no llegó a `healthy` | `docker compose ps` y `docker compose logs <motor>` |
| `NoHostAvailable` o `ServiceUnavailable` al correr un script | Corriste el script en tu máquina y no dentro de `app` | `docker compose exec app bash` y vuelve a correrlo ahí |
| `InvalidRequest: ... ALLOW FILTERING` | Tu consulta filtra por una columna que no es parte de la partition key | Rediseña la `PRIMARY KEY` de la tabla; no agregues `ALLOW FILTERING` |
| Q2a da bien los conteos pero mal los 5 más recientes | `CLUSTERING ORDER BY` ascendente o faltante | El orden de las filas dentro de la partición se define en el `CREATE TABLE` |
| `verificar.py` dice que el ranking del stream está vacío | No has corrido `stream_lab.py` (o lo corriste con otro usuario) | Córrelo con tu usuario y vuelve a verificar |
| Los resultados no coinciden después de cambiar de usuario | Los motores tienen cargados los datos de otra corrida | Vuelve a correr el script de esa parte con tu usuario: cada uno borra y recarga sus datos |

## Referencias

- Kleppmann, *Designing Data-Intensive Applications*, cap. 2 (modelos de datos y lenguajes de consulta) y cap. 11 (procesamiento de streams).
- [MongoDB — Data Modeling](https://www.mongodb.com/docs/manual/data-modeling/)
- [Cassandra — Data Modeling](https://cassandra.apache.org/doc/latest/cassandra/developing/data-modeling/index.html)
- [Redis — Sorted sets](https://redis.io/docs/latest/develop/data-types/sorted-sets/)
- [Neo4j — Cypher Manual](https://neo4j.com/docs/cypher-manual/current/)
- `../lab2a-kafka/README.md` — at-least-once e idempotencia con Kafka.
