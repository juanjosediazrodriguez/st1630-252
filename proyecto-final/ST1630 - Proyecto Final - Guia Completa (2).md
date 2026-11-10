## ST1630

### Sistemas Intensivos en Datos

##### Universidad EAFIT

# Proyecto Final

*Guía completa de entregables, criterios y rúbricas de evaluación*

##### Profesor Andrés Sacre Alzate · csanch35@eafit.edu.co

**Modalidad** Grupal · grupos de 4 personas · formación libre

**Peso en la nota** 40% de la nota total del curso (20% E1 + 20% E2)

##### Entregable 1

Antes del inicio de S13 · Diagnóstico, requisitos y arquitectura

##### Entregable 2

S16 · Implementación, presentación oral y demo en vivo

*Versión 1.0 · Septiembre 2026*

#### 1. Propósito del proyecto final

El proyecto final integra todos los conceptos del curso en un sistema de datos real, diseñado y construido por el equipo. No es un ejercicio académico aislado: es un pipeline de datos completo con un problema de negocio real, una arquitectura justificada, una implementación funcional y una defensa técnica ante el docente. El objetivo no es que el sistema sea perfecto. Es que el equipo pueda justificar cada decisión de diseño: por qué este motor y no otro, por qué esta garantía de consistencia, por qué este modelo de datos. Esa capacidad de argumentación es el núcleo de la evaluación. La pregunta central del proyecto: "dado este problema de negocio, ¿qué pipeline de datos construirías y por qué exactamente así?"

#### 2. Conformación de grupos

||Grupos de exactamente 4 personas. 3.1 Elección del dominio funcionado bien en cursos similares: ● ● ● ● ● ● 3.2 Restricciones técnicas obligatorias automática en la rúbrica:|La conformación es libre y definitiva después de la primera sesión de asesorías (S15). Ningún grupo puede tener menos de 4 ni más de 4 personas. Cada integrante debe poder defender cualquier parte del sistema en la presentación final. Para el Entregable 1, el equipo debe indicar en la portada del documento: nombre completo y código EAFIT de los cuatro integrantes, nombre del proyecto y dominio elegido. Todos los integrantes reciben la misma nota de grupo, excepto si el docente detecta en la defensa que un integrante no puede responder preguntas sobre partes del sistema que otro integrante implementó. En ese caso, la nota del integrante se penaliza hasta un 30%. 3. Dominio del proyecto y restricciones técnicas El equipo elige libremente el dominio de negocio. El único requisito es que el problema genere datos con volumen y variedad suficientes para justificar una arquitectura de datos distribuida. Dominios que han Fintech colombiana: pagos, transferencias, detección de fraude, análisis de crédito. E-commerce: catálogo de productos, órdenes, comportamiento de usuarios, recomendaciones. Salud: historiales clínicos, sensores de pacientes, epidemiología. IoT / industria: sensores de manufactura, telemetría de flotas, smart cities. Medios y entretenimiento: plataformas de streaming, análisis de audiencia, contenido. Logística: rastreo de envíos, optimización de rutas, inventario en tiempo real. El equipo puede proponer cualquier otro dominio con justificación en el Entregable 1. El pipeline debe incluir los siguientes componentes. Su ausencia descalifica el proyecto o genera penalización|||
|---|---|---|---|---|
||Componente|Requisito mínimo|Justificación pedagógica||
||Streaming|Al menos un flujo de datos en tiempo real con procesamiento por evento o micro-batch. No es suficiente simular con batch.|S6-S7: Kafka + Spark SSt o Flink||
||Spark|Al menos una transformación no trivial (join, windowing, agregación) implementada en PySpark o Spark SQL.|S4-S5: el motor central del curso||
||Lakehouse|Arquitectura Bronze/Silver/Gold sobre Delta Lake u otro formato de tabla abierta (Iceberg). La capa Gold debe tener un modelo dimensional consultable.|S9: modelo dimensional + S4: Delta Lake||
|Decisión de motor libre malas. Fecha límite Formato Herramienta de diagrama||Al menos un componente adicional que el equipo elija con justificación: NoSQL (MongoDB/Cassandra/DynamoDB/Neo4j), vector database, motor de consulta (Athena/Trino), etc. El stack tecnológico exacto (AWS vs GCP vs local, Docker vs cloud managed) es decisión del equipo y debe justificarse en el Entregable 1. No hay una respuesta correcta de plataforma, solo justificaciones buenas o 4. Entregable 1: Diagnóstico, requisitos y arquitectura Antes del inicio de S13 (subir al repositorio del grupo antes de las 11:59 PM del día anterior) Documento PDF (máximo 15 páginas sin anexos) + diagrama exportado en PNG/SVG Lucidchart o Draw.io (compartir enlace editable en el documento)|S10: el equipo aplica el criterio de decisión NoSQL||
|Entrega||Entrega en EAFIT Interactiva|||
|Peso||20% de la nota total del curso|||
|de una base de datos relacional simple.||4.1 Secciones obligatorias del documento Sección 1. Definición del problema (1-2 páginas) Describe el problema de negocio que el sistema va a resolver. Debe quedar claro qué preguntas analíticas u operacionales necesita responder el sistema, quiénes son los usuarios del sistema y qué decisiones toman con los datos, y por qué la escala o la variedad del problema justifica una arquitectura de datos distribuida en lugar Error frecuente: describir el sistema técnico en lugar del problema de negocio. "Vamos a construir un pipeline con Kafka y Spark" no es la definición del problema. "Los analistas de Nequi necesitan detectar patrones de fraude en menos de 2 minutos desde que ocurre la transacción sospechosa" sí lo es. Sección 2. Requisitos funcionales y no funcionales (2-3 páginas)|||

-- page 1 --

**Requisitos funcionales:** qué debe hacer el sistema. Cada requisito en una oración con verbo de acción: "El sistema debe ingestar las transacciones de Nequi en tiempo real con latencia menor a 5 segundos". Mínimo 8 requisitos funcionales. **Requisitos no funcionales:** con qué restricciones de calidad. Deben usar los conceptos del curso explícitamente:

- Escalabilidad: "el sistema debe soportar un pico de X eventos por segundo sin degradación".
- Disponibilidad: citar la posición CAP del sistema y justificarla (CP vs AP).
- Consistencia: qué garantía de entrega se requiere (at-most-once, at-least-once, exactly-once) y en qué parte del pipeline.
- Latencia: tiempo máximo aceptable desde el evento hasta que esté disponible en Gold.
- Tolerancia a fallos: qué pasa si un nodo del pipeline falla y cómo se recupera.
- Trazabilidad: cómo se puede auditar el linaje de un dato desde la fuente hasta Gold.
##### Sección 3. Diagrama de arquitectura de datos por capas (1 página)

Un solo diagrama que muestre las cinco capas del pipeline. Debe ser legible (exportado en PNG/SVG de alta resolución). El diagrama debe incluir:

- Fuente: los sistemas origen de los datos (base de datos operacional, API, sensores, eventos de usuario). Indicar el formato y la frecuencia de generación.
- Ingestión: cómo los datos entran al pipeline (Kafka topic, API pull, batch desde S3). Indicar el mecanismo de transporte y la garantía de entrega.
- Procesamiento: las transformaciones que ocurren (Spark batch, Spark Streaming, Flink). Indicar qué cambia entre la entrada y la salida de cada transformación.
- Almacenamiento: las capas Bronze, Silver y Gold con el motor de cada una (Delta Lake, NoSQL, etc.) y el modelo de datos de Gold (star schema o equivalente).
- Consumo: cómo los usuarios o sistemas consumen los datos de Gold (Athena, Power BI, API, alertas en tiempo real). Cada componente del diagrama debe tener una etiqueta con el nombre del servicio o herramienta concreta (no "base de datos" sino "MongoDB" o "Delta Lake en S3"). El diagrama es el artefacto central del Entregable 1. Un buen diagrama permite que alguien externo al equipo entienda la arquitectura sin leer el texto. Si el diagrama necesita más de 2 minutos de explicación para ser entendible, necesita más claridad.
##### Sección 4. Justificación de decisiones técnicas (3-4 páginas)

Para cada decisión técnica no trivial, el equipo debe escribir un ADR (Architecture Decision Record) con las cuatro secciones vistas en S10:

- Contexto: qué restricción o requisito motiva la decisión.
- Alternativas consideradas: qué otras opciones se evaluaron y por qué se descartaron.
- Decisión: qué se eligió y el argumento técnico concreto (citar conceptos del curso: CAP, garantías de entrega, sharding, schema evolution, etc.).
- Consecuencias: qué se sacrifica con esta decisión. Decisiones mínimas que deben justificarse:
- Elección del motor de streaming (Kafka, Kinesis u otro) y la garantía de entrega configurada.
- Elección de Spark Streaming vs Flink vs batch puro para el procesamiento.
- Elección del motor de la capa Gold (Delta Lake u otro) y por qué el modelo dimensional es adecuado para el caso.

-- page 4 --

- Elección del componente adicional libre (NoSQL u otro) y su posición CAP en el contexto del problema.
- Elección de la herramienta de orquestación y cómo se modelan las dependencias del DAG.
##### Sección 5. Plan de implementación (1 página)

Tabla con la distribución de responsabilidades entre los cuatro integrantes y el cronograma de implementación. Debe incluir qué componente implementa cada integrante, los hitos intermedios antes de S16 y los riesgos técnicos identificados con su plan de mitigación.

##### 4.2 Rúbrica del Entregable 1

|Criterio|Completo (100%)|Parcial (60%)|Insuficiente (0%)|Peso|
|---|---|---|---|---|
|Definición del problema|El problema de negocio es claro, acotado y justifica la escala de la solución propuesta. Las preguntas analíticas son específicas y medibles.|El problema está descrito pero es vago o no queda claro por qué necesita una arquitectura distribuida.|El documento describe el sistema técnico, no el problema de negocio.|10%|
|Requisitos funcionales|8 o más requisitos con verbo de acción. Todos son verificables y trazables al diagrama.|Entre 4 y 7 requisitos, o algunos no son verificables.|Menos de 4 requisitos o son demasiado genéricos para ser evaluados.|15%|
|Requisitos no funcionales|Citan explícitamente: CAP, garantía de entrega, latencia, escalabilidad y tolerancia a fallos con valores concretos.|Al menos 3 de los 5 conceptos del curso mencionados, con valores concretos.|Requisitos no funcionales genéricos sin citar los conceptos del curso.|15%|
|Diagrama de arquitectura|Las 5 capas están presentes. Cada componente tiene el nombre del servicio concreto. El diagrama es legible sin explicación adicional. El enlace editable funciona.|Las 5 capas están pero algunos componentes son genéricos o el diagrama requiere explicación extensa.|Faltan capas, el diagrama es un flowchart genérico, o el enlace no funciona.|25%|
|Justificación de decisiones|Al menos 5 ADRs completos con contexto, alternativas, decisión y consecuencias. Cada ADR cita conceptos del curso.|Al menos 3 ADRs o algunos ADRs incompletos (falta la sección de consecuencias en varios).|Menos de 3 ADRs o las justificaciones son opiniones sin soporte técnico del curso.|25%|
|Plan de implementación|Distribución de tareas clara, cronograma realista antes de S16, riesgos identificados con mitigación.|Cronograma existe pero es vago o los riesgos no tienen plan de mitigación.|Sin cronograma o sin distribución de tareas entre integrantes.|10%|

|||El Entregable 1 se evalúa como documento, no como implementación. No se pide código en esta entrega. El diagrama y las justificaciones son el artefacto central. 5. Entregable 2: Implementación, presentación y defensa|
|---|---|---|
||Sesión Duración por grupo|S16 (fecha de sustentaciones finales) 25 minutos: 10 min presentación + 8 min demo en vivo + 7 min defensa con preguntas del docente|
||Formato de presentación|Slides propios del equipo (no hay template obligatorio). Máximo 15 slides.|
||Demo|El pipeline debe correr en vivo durante la sesión. Se permite tener un dataset pre- cargado pero el procesamiento debe ejecutarse en tiempo real frente al docente.|
||Repositorio Peso|El PR final con todo el código debe estar abierto antes de las 11:59 PM del día anterior a S16 20% de la nota total del curso|
|● ● ● ● ● ●|vivo).|5.1 Estructura de los 25 minutos Bloque 1: Presentación (10 minutos) El equipo presenta el problema, la arquitectura y las decisiones técnicas clave. No es una repetición del Entregable 1: es una síntesis de lo que construyeron y por qué las decisiones del E1 se mantuvieron, cambiaron o fallaron durante la implementación. Slides sugeridos: Slide 1-2: el problema de negocio y las preguntas que el sistema responde. Slide 3-4: el diagrama de arquitectura final (puede diferir del E1 si hubo cambios justificados). Slide 5-8: las decisiones técnicas más importantes con sus resultados reales (latencias medidas, throughput real, errores encontrados). Slide 9-10: qué cambiaría el equipo si tuviera más tiempo y por qué. Bloque 2: Demo en vivo (8 minutos) El equipo demuestra que el pipeline funciona. El docente puede pedir ver partes específicas del sistema. Se recomienda tener preparados: El productor de Kafka generando eventos en tiempo real (o con datos pre-cargados que se procesan en El job de Spark corriendo y mostrando el procesamiento de los datos.|

-- page 5 --

- La capa Gold consultable: al menos una query analítica que responda una de las preguntas de negocio definidas en el E1.
- El DAG de Airflow (u orquestador elegido) mostrando el pipeline completo y su estado. Si el pipeline falla durante la demo, el equipo debe poder explicar exactamente por qué y cómo lo resolvería. Un fallo explicado técnicamente bien puede mantener la nota de defensa. Un fallo no explicado la baja significativamente.
##### Bloque 3: Defensa con preguntas (7 minutos)

El docente hace preguntas técnicas directamente a integrantes específicos, sin que los otros puedan intervenir. Las preguntas típicas incluyen:

- "¿Por qué eligieron [motor X] y no [motor Y] para este caso?" — el equipo debe poder responder con el ADR del E1 y con lo que aprendieron durante la implementación.
- "Si el volumen de datos se multiplicara por 100, qué cambiarían primero en la arquitectura?"
- "¿Cómo garantizan que un evento no se procese dos veces si el job de Spark falla a mitad?"
- "¿Cuál es la posición CAP del componente más crítico de su sistema y qué sacrificaron?"
- "¿Qué pasa con el pipeline si el broker de Kafka cae? Muéstrenme en el código cómo lo manejan."
- "¿Por qué la partition key de [tabla NoSQL] es [campo X] y no [campo Y]?" Las preguntas se dirigen deliberadamente a partes del sistema que el integrante no implementó, para verificar que todos entienden el sistema completo.
##### 5.2 Rúbrica del Entregable 2

##### Repositorio y código (evaluado antes de la presentación)

|Criterio|Completo (100%)|Parcial (60%)|Insuficiente (0%)|Peso|
|---|---|---|---|---|
|Estructura del repositorio|Carpetas organizadas por componente. README con instrucciones para correr el pipeline desde cero. docker- compose o equivalente funcional.|README existe pero incompleto. El pipeline requiere pasos no documentados para correr.|Sin README o el pipeline no corre con las instrucciones del README.|10%|
|Streaming funcional|El flujo de datos en tiempo real funciona de extremo a extremo: productor → Kafka → consumidor con procesamiento.|El streaming existe pero con errores no manejados o sin procesamiento no trivial.|El streaming es simulado con batch o no existe.|20%|
|Spark con transformaciones|Al menos una transformación no trivial (join entre datasets, windowing, agregación con GROUP BY) implementada y documentada.|Transformaciones simples sin joins ni windowing.|Sin uso de Spark o solo lecturas sin transformación.|20%|

-- page 7 --

**Lakehouse** Las tres capas existen en Las tres capas existen Menos de tres capas 20% **Bronze/Silver/Gold** Delta Lake (u equivalente). pero Gold no tiene o no usa formato de Gold tiene un modelo modelo dimensional o no tabla abierta. dimensional consultable con es consultable. al menos una query de negocio.

##### Orquestación 15%

|El DAG orquesta el pipeline|||El DAG existe pero no|Sin orquestación o el||
|---|---|---|---|---|---|
|completo con dependencias|||orquesta todo el pipeline|pipeline|se corre|
|correctas. Los steps fallan|||o las dependencias están|manualmente|sin|
|limpiamente|si hay|error|incompletas.|DAG.||

upstream.

**Componente libre** El componente adicional El componente existe El componente no 15% **justificado** funciona y su justificación en pero con funcionalidad existe o no es el que el E1 se cumplió en la limitada. se propuso en el E1 implementación. sin justificación del cambio.

##### Presentación y demo (evaluado durante S16)

|Criterio|Completo (100%)|Parcial (60%)|Insuficiente (0%)|Peso|
|---|---|---|---|---|
|Claridad de la presentación|El problema de negocio, la arquitectura y las decisiones técnicas quedan claros en los 10 minutos. El diagrama final es coherente con el código.|La presentación es entendible pero requiere preguntas adicionales para clarificar la arquitectura.|La presentación no es coherente con el sistema implementado o supera los 10 minutos.|15%|
|Demo funcional|El pipeline corre en vivo. Al menos una query de Gold responde una pregunta de negocio del E1 en tiempo real.|El pipeline corre pero con errores no explicados o la query de Gold no responde una pregunta de negocio concreta.|El pipeline no corre o la demo es un video pregrabado sin justificación.|25%|
|Defensa técnica individual|Cada integrante responde correctamente y con argumentos técnicos del curso las preguntas sobre su parte del sistema.|Cada integrante responde correctamente las preguntas sobre su parte pero no puede responder sobre otras partes.|Al menos un integrante no puede responder preguntas básicas sobre el sistema.|40%|
|Reflexión sobre decisiones|El equipo puede comparar lo que propuso en el E1 con lo que implementó y explicar los cambios con argumentos|El equipo menciona los cambios pero sin|El equipo no puede explicar por qué el sistema|20%|

-- page 8 --

técnicos, no solo "era más justificación técnica implementado difícil de implementar". sólida. difiere del E1.

La defensa técnica individual pesa el 40% del E2 por una razón: en un proyecto grupal es posible que un integrante no contribuya equitativamente. La defensa individual elimina esa posibilidad. Si un integrante no puede responder preguntas básicas sobre el sistema, su nota individual puede ser hasta un 40% menor que la del grupo.

#### 6. Sesiones de asesoría

Las semanas S15 están reservadas para asesorías de proyecto final. Cada grupo tiene derecho a una sesión de 20 minutos con el docente. El grupo debe prepararse con preguntas concretas; el docente no revisa código sin que el equipo haya intentado resolver el problema primero.

|Semana|Actividad|Qué traer preparado|
|---|---|---|
|S13|Retroalimentación del E1 (si fue entregado a tiempo). El docente devuelve comentarios sobre el diagrama y los ADRs.|El E1 entregado. Preguntas concretas sobre las decisiones técnicas que el equipo no terminó de resolver.|
|S14|Asesoría de implementación (opcional). El equipo puede pedir ayuda sobre problemas técnicos específicos del pipeline.|Un problema técnico concreto documentado: qué intentaron, qué error obtuvieron, qué alternativas probaron.|

Las asesorías son para equipos que ya intentaron resolver sus problemas. No es una sesión donde el docente dice qué tecnología usar — esa decisión es del equipo y forma parte de la evaluación.

#### 7. Política de uso de agentes de IA

El uso de agentes de IA (Claude, ChatGPT, Copilot, Cursor y similares) está permitido y es esperado en el proyecto final. La habilidad de usar agentes de forma efectiva es parte del perfil profesional que el curso busca desarrollar. Sin embargo:

- El equipo es responsable de entender y poder explicar todo el código que entrega, independientemente de quién o qué lo escribió.
- Usar un agente para generar la justificación técnica de una decisión sin entenderla es la forma más rápida de reprobar la defensa. El docente preguntará exactamente sobre esas decisiones.
- La bitácora de delegación (documento en el repositorio que registra qué hizo el agente y qué decidió el equipo) es obligatoria. Sin bitácora se descuenta el 10% de la nota del E2.

-- page 9 --

- Un buen uso del agente: "genera el esqueleto del productor de Kafka en Python" o "ayúdame a escribir la query de Spark que hace esta transformación". Un mal uso: "diseña la arquitectura completa para mi problema de Nequi". La defensa técnica es la mejor prueba de si el equipo entiende el sistema. Un equipo que usó bien los agentes puede defender cada decisión. Un equipo que delegó sin entender no puede.
#### 8. Integridad académica

El proyecto final es trabajo original del grupo. Las siguientes situaciones se consideran violaciones de integridad académica y resultan en nota de cero para todos los integrantes del grupo:

- Presentar código de otro grupo como propio.
- Presentar un sistema que no fue implementado por el equipo (por ejemplo, un tutorial seguido punto a punto sin modificaciones sustanciales).
- Alterar los resultados de la demo para ocultar que el pipeline no funciona correctamente.
#### 9. Checklist de entrega

##### Entregable 1 (antes de S13)

- ☐ Documento PDF en /docs/entregable1/e1_[nombre_proyecto].pdf
- ☐ Diagrama de arquitectura en PNG o SVG en /docs/entregable1/arquitectura.[ext]
- ☐ Enlace editable al diagrama en Lucidchart o Draw.io en la Sección 3 del documento
- ☐ Las 5 secciones del documento completas
- ☐ Nombres y códigos de los 4 integrantes en la portada
- ☐ Pull Request abierto con descripción del proyecto
##### Entregable 2 (antes de S16 y durante S16)

- ☐ PR final con todo el código antes de las 11:59 PM del día anterior a S16
- ☐ README con instrucciones para correr el pipeline desde cero
- ☐ docker-compose.yml (o equivalente) en la raíz del repositorio
- ☐ Bitácora de delegación de agentes de IA en /docs/bitacora_ia.md
- ☐ Slides de la presentación en /docs/entregable2/
- ☐ El pipeline corre localmente antes de S16 (ensayo obligatorio el día anterior)
- ☐ Cada integrante puede responder preguntas sobre cualquier parte del sistema
#### 10. Resumen de pesos en la nota final

##### Componente Peso en el curso Desglose

-- page 10 --

|Entregable 1 (E1)|20%|Diagnóstico 10%, Requisitos 30%, Diagrama 25%, ADRs 25%, Plan 10%|
|---|---|---|
|Entregable 2 (E2) — Repositorio|12%|Streaming 20%, Spark 20%, Lakehouse 20%, Orquestación 15%, Comp. libre 15%, README 10%|
|Entregable 2 (E2) — Presentación y defensa|8%|Presentación 15%, Demo 25%, Defensa individual 40%, Reflexión 20%|
|TOTAL PROYECTO FINAL|40%|E1: 20% + E2 repo: 12% + E2 defensa: 8%|

**Nota:** la nota del proyecto final (40%) se suma a Quices (10%), Parcial 1 (30%) y Parcial 2 (20%) para completar el

100% de la nota del curso.

*ST1630 · Sistemas Intensivos en Datos · Universidad EAFIT · Prof. Andrés Sacre Alzate*

-- page 11 --
