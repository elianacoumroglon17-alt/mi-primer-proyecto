# Clase práctica 3 — Bases de datos NoSQL: el mismo dato, cinco modelos

**Nombre:** Eliana Gómez Coumroglon

**student_id:** elianacoumroglon

**Escala:** small

## Propósito

Tomar los clientes, productos y transacciones Silver de la clase 2 y reorganizarlos según los modelos NoSQL vistos en la teoría: clave-valor, documental, grafos, vectorial y columnar. Antes de eso se simula el problema común a todos al distribuirse: CAP y PACELC.

La práctica es **demostrativa y visual**. Databricks no es Redis, MongoDB, Neo4j ni Pinecone: cada modelo se simula con Spark, tablas Delta y Python para observar su idea central (cómo se guarda, cómo se consulta, qué cuesta distribuirlo) sin instalar motores externos.

## Objetivos

- Explicar con un ejemplo concreto qué se pierde ante una partición en un sistema CP y en uno AP, y el trade-off latencia/consistencia de PACELC.
- Reconocer el acceso por clave de un almacén clave-valor y el rol de la función hash dentro de un nodo y entre nodos.
- Diseñar un documento a partir de un patrón de lectura: qué se embebe y qué se referencia.
- Expresar un patrón de grafo en Cypher y en SQL, y entender por qué cada salto es costoso de distribuir.
- Buscar vecinos por similitud y entender el trade-off recall/costo de un índice ANN.
- Relacionar Parquet y Delta con el almacenamiento columnar: column pruning, compresión y data skipping.

## Requisitos previos

- Clases 1 y 2 ejecutadas en el mismo catálogo y esquema personal: se usan `silver_customers`, `silver_products` y `silver_transactions`.
- Mismo `student_id` y misma escala que en la clase 2.
- Databricks Free Edition con compute serverless. No hay que instalar librerías: se usan Spark, NumPy, pandas, matplotlib y pyarrow incluidos en la plataforma.
- Para importación manual, conservar `clase-03/` y `common/` como carpetas hermanas.

## Secuencia

Los notebooks son **demostrativos**: no hay que modificar ni completar código. Se ejecutan en orden, celda por celda, leyendo el texto y mirando cada resultado y gráfico. Las preguntas de la entrega se responden con lo que muestran.

| Notebook | Modelo | Qué se ve | Minutos |
|---|---|---|---:|
| `00_preflight.ipynb` | Relacional | Verifica las tablas Silver y muestra el punto de partida | 5 |
| `01_cap_pacelc.ipynb` | Distribución | Réplicas con una partición, modos CP y AP; latencia vs. `W`; quórum `W+R>N` | 15 |
| `02_clave_valor.ipynb` | Clave-valor | Perfiles `customer:<id>`; GET en memoria vs. Spark; hash mod N vs. hashing consistente | 20 |
| `03_documental.ipynb` | Documental | Un documento por cliente con sus transacciones embebidas; consultas anidadas; esquema flexible; tamaño | 20 |
| `04_grafos.ipynb` | Grafos | Clientes y dispositivos; Cypher vs. SQL; componentes conexos; dibujo; particionamiento | 20 |
| `05_vectorial.ipynb` | Vectorial | Vectores de comportamiento; PCA; k-NN exacto; índice IVF y curva recall/costo | 20 |
| `06_columnar.ipynb` | Columnar | Fila vs. columna; CSV/JSON/Parquet; plan físico; footer Parquet; data skipping | 15 |


## Tablas y archivos que se crean

```text
nosql_kv_customer_profile        (key, value JSON)
nosql_customer_documents         (_id, profile, stats, transactions[])
nosql_graph_nodes / nosql_graph_edges / nosql_graph_components
nosql_graph_cc_a / nosql_graph_cc_b   (auxiliares de la propagación de etiquetas)
nosql_customer_vectors           (customer_id, embedding[], flagged)
landing/nosql_lab/customer_documents/   JSON Lines
landing/nosql_lab/flexible_documents/   JSON con esquemas distintos
landing/nosql_lab/columnar/             CSV, JSON y Parquet de las transacciones
```

Ninguna tabla de las clases 1 y 2 se modifica. Todos los notebooks pueden reejecutarse: sobrescriben sus propias tablas y archivos.

## Preguntas

Las preguntas son cortas: buscan comprobar que entendiste la idea de cada modelo y sus ventajas y desventajas. Cuando piden un dato, copiá el número que te dio el notebook (puede variar un poco según tu escala). Alcanza con una a tres oraciones por respuesta.

### CAP y PACELC (`01_cap_pacelc`)

1. Con la red partida, ¿qué respondió la réplica `C` en modo CP y qué respondió en modo AP? ¿Qué propiedad de CAP sacrifica cada modo?

**Modo CP**

C quedó aislada de A y B y rechazó las operaciones 4 y 5. Mantuvo el saldo de 100 que tenía antes de 'write saldo=80'.

Al estar en modo CP se sacrifica disponibilidad. Por eso en c se muestra el saldo anterior, no tenía certezas de que el nuevo saldo sea el correcto, entonces no se muestra. Se mantiene la consistencia.

**Modo AP**

C quedó aislada de A y B pero igualmente devolvió un resultado en todos los casos. Mantuvo el saldo de 100 aun cuando en A y B era 80. Luego, cambia a 50 solamente en C y A y B mantienen los 80. 

Al estar en modo AP se sacrifica consistencia, pero se mentiene la disponibilidad.

2. En modo AP, ¿qué saldo quedó en las tres réplicas después de la reparación y qué escritura se perdió?

Después de la reparación, A, B y C quedaron con saldo 50. 
La escritura que se perdió fue **`saldo=80`**, hecha en `A` durante la partición (paso 3). Como indica last-write-wins, se mantiene la escritura mas reciente (50), por lo que no 80 es sobrescrita.

3. Según el gráfico de latencia, ¿cuál es la p99 con `W=1` y con `W=3`? ¿Qué se gana a cambio de esperar más réplicas? (PACELC)

La latencia con W=1 es de 10ms. La latencia con W=3 es de aproximadamente 55ms.     

A cambio de esperar más réplicas se gana consistencia. Con W=3 la escritura se confirma cuando las tres réplicas la tienen. Por lo tanto, cualquier lectura verá el dato más reciente, venga de la réplica que venga.

4. Con cinco réplicas partidas en `ABC | DE`, ¿qué lado pudo seguir escribiendo en modo CP y por qué?

En modo CP se acepta una escritura si se le puede confirmar una mayoría. En este caso se puede seguir escrbiendo del lado 'ABC', ya que representa 3 de las 5 réplicas (la mayoría). 

### Clave-valor (`02_clave_valor`)

5. ¿Cuántas veces más rápido fue el GET en memoria que el GET con Spark? ¿Por qué Redis guarda los datos en memoria?

El get en memoria fue aroximadamente unas 2.4 millones de veces más rápido que el get con Spark. 
Redis guarda los datos en memoria porque es mucho más rápida que el disco. Redis es un almacén clave-valor y usa estructuras simples (tablas hash), así que encontrar un valor a partir de su clave es una operación directa y de costo casi constante. Spark, en cambio, está pensado para recorrer grandes volúmenes de datos, y cada consulta implica arrancar un plan de ejecución y leer archivos del almacenamiento, lo que le suma muchos milisegundos incluso para buscar un solo registro.

6. ¿Por qué para contar los clientes de AR hubo que leer y parsear todos los valores? Mencioná una ventaja y una desventaja del modelo clave-valor.

La base de datos solo sabe encontrar el valor a partir de su clave, pero no entiende lo que hay adentro. Para contar los clientes de AR hay que recorrer todas las claves, hacer un `GET` de cada una y parsear el valor para revisar el país y contar. 

**Ventaja:** el acceso por clave es rápido y simple. Buscar, escribir o borrar un dato a partir de su clave es una operación directa de costo casi constante.

**Desventaja:** no permite consultar por el contenido de los valores. No hay filtros, `WHERE`, `JOIN` ni agregaciones nativas, y cualquier consulta que no sea por clave obliga a leer y procesar todo desde la aplicación. 

7. Al pasar de 4 a 5 nodos, ¿qué porcentaje de claves se movió con `hash mod N` y con hashing consistente? ¿Por qué conviene el segundo?

Aproximadamente el 80% de las clsves se movió a `hash mod N`y el 25% con hashing consistente.
Conviene el hashing consistente porque agregar o quitar un nodo implica mover una fracción pequeña de los datos y no casi todos. Así se evita una migración masiva por la red, que saturaría el sistema.

### Documental (`03_documental`)

8. ¿Cuántas filas devolvió la consulta relacional del cliente 42 y cuántos documentos la documental? ¿Qué datos quedaron embebidos dentro del documento?

La consulta relacional del cliente 42 devolvió 11 filas. La documental devolvió 1 documento. Los datos que quedaron embebidos en el documento son:

- **`profile`**: los datos del cliente (país, segmento, fecha de creación y email).
- **`stats`**: estadísticas ya calculadas (`tx_count` 11, `total_amount` 9379,8 y `fraud_transactions` 1).
- **`transactions`**: un arreglo con las 11 transacciones del cliente (fecha, monto, canal de pago, dispositivo, marca de fraude), y dentro de cada una, el **`product`** con su `product_id`, categoría y precio.

9. Al leer documentos con campos distintos, ¿qué hizo Spark con los campos que faltaban en algunos? ¿Por qué se dice que el modelo documental tiene esquema flexible?

Spark unió todos los campos que aparecen en cualquiera de los documentos en un único esquema y completó con **`null`** los campos que faltaban en cada documento. 
El modelo documental tiene esquema flexible porque cada documento puede tener su propia estructura: campos distintos, estructuras anidadas, arreglos o una versión de esquema propia, sin tener que definir ni modificar un esquema fijo de antemano.

10. ¿Cuánto pesa el documento más grande? ¿Qué problema aparece si un documento crece sin límite?

El documento más grande pesa **5,2 KB**, con un promedio de unos **225 bytes por transacción** embebida. Es pequeño hoy, pero al ritmo de 225 bytes por transacción un cliente llegaría a **16 MB con unas 74.599 transacciones**, que es el máximo que permiten algunas bases documentales, como MongoDB.

11. Para calcular el monto por categoría hubo que usar `explode`. ¿Qué tipo de consultas resuelve bien el modelo documental y cuáles le cuestan más?

`explode` fue necesario porque las transacciones están embebidas como un arreglo dentro de cada documento de cliente, y la categoría vive dentro de cada transacción (en `product`). Para agrupar por categoría hay que "desarmar" ese arreglo en una fila por transacción y recién entonces sumar. En el modelo relacional las transacciones ya son filas y se resolvería con un `GROUP BY`.
El modelo documental resuelve bien las consultas que no requerirían un 'JOIN' en el modelo relacional. Le cuestan más consultas que cruzan muchos documentos. 

### Grafos (`04_grafos`)

12. ¿Cuántos nodos `Customer`, nodos `Device` y aristas `USES` tiene el grafo?

- **Nodos `Customer`:** 5.000 (738 marcados, ≈ 14,8 %)
- **Nodos `Device`:** 2.361

13. ¿Cuántos joins necesitó SQL para el patrón de 2 saltos y para el de 4? ¿Por qué Neo4j recorre relaciones más eficientemente que una base relacional?

- **2 saltos:** 3 joins (1.389 clientes alcanzados).
- **4 saltos:** 5 joins (254 clientes alcanzados).

Cada salto agrega un join y la consulta se complica.

Neo4j es más eficiente porque cada nodo guarda punteros directos a sus vecinos, así que recorrer una relación es seguir un puntero, sin buscar en tablas ni hacer joins. El costo depende de lo que se visita y no del tamaño total del grafo. En SQL, cada join compara filas entre tablas y los resultados intermedios crecen con cada salto.

14. ¿Cuántas iteraciones tardó en converger la búsqueda de componentes conexos y cuántos nodos tiene el componente más grande?

Converge en **14 iteraciones**. El componente m'as grande tiene  **23 nodos**.

15. ¿Qué porcentaje de aristas quedó entre máquinas distintas al repartir los nodos con `hash(id) mod 4`? ¿Por qué es difícil distribuir una base de grafos?

- **Con `hash(id) mod 4`:** **74,4 %** de las aristas quedan entre máquinas distintas.
- **Por componente conexo:** 0 %.

Es difícil distribuir un grafo porque los datos están muy conectados, así que cortar el grafo rompe aristas. Cada arista cortada es un salto por red, mucho más lento que uno local, y encontrar un reparto con pocas aristas cortadas y carga equilibrada es difícil.

### Vectorial (`05_vectorial`)

16. ¿Qué representa el vector de cada cliente y qué mide la similitud coseno?

El vector de cada cliente es una lista de 10 números que resume su comportamiento de compra. Así, cada cliente queda representado como un punto en un espacio de 10 dimensiones, y dos clientes con vectores parecidos se comportan de forma parecida.

La similitud coseno mide qué tan parecida es la dirección de dos vectores: es el coseno del ángulo que forman. Da 1 cuando apuntan en la misma dirección (clientes con el mismo perfil de comportamiento), 0 cuando son perpendiculares (sin relación) y -1 cuando son opuestos.

17. ¿Qué porcentaje de clientes marcados hay entre los vecinos de clientes marcados y cuál es la tasa base? ¿Para qué sirve buscar "vecinos parecidos"?

- Entre los 10 vecinos más parecidos de los clientes marcados, el **31,6 %** también está marcado. 
- Entre los vecinos de los clientes no marcados el **13,2 %** está marcado.
- La **tasa base** (porcentaje de marcados en todos los clientes) es de **14,8 %**.

Buscar "vecinos parecidos" sirve para detectar candidatos a revisar, segmentación de clientes, generar reomendaciones en base a gustos.

18. En la curva del índice IVF, ¿qué recall y qué porcentaje de vectores escaneados se obtienen con `nprobe=4`? ¿Qué se gana y qué se pierde con la búsqueda aproximada (ANN)?

Con `nprobe=4` el recall@10 es de ≈ **0,87** y se compara sólo ≈ **6 %** de los vectores (4 de 70 clusters).

**Se gana:** mucha velocidad, menor costo y la posibilidad de escalar y particionar la base entre nodos.

**Se pierde:** exactitud. Se pierde cerca del 13 % de los vecinos verdaderos, porque si un vecino está en un cluster que no se revisó, no aparece.

Es el trade-off **recall vs. costo**: con `nprobe=16` el recall llega a ≈ 1,0 pero escaneando ≈ 24 % de los vectores.

### Columnar (`06_columnar`)

19. ¿Cuánto ocupan los datos en CSV, JSON y Parquet? ¿Qué columnas leyó Spark para la consulta de `amount` sobre Parquet (`ReadSchema`)? ¿Cuántos archivos se pueden saltear con los datos ordenados?

- **CSV:** 50,0 MB
- **JSON:** 121,1 MB
- **Parquet:** 5,3 MB (≈ 9 veces menos que CSV y ≈ 23 veces menos que JSON)

Parquet es columnar y comprime mejor, y JSON es el más pesado porque repite los nombres de los campos en cada registro.

20. ¿Por qué el formato columnar es bueno para analítica y poco conveniente para modificar una fila por vez?

**Bueno para analítica:**
- Las consultas analíticas suelen leer pocas columnas de muchas filas. Con el formato columnar se leen sólo esas columnas y se ahorra I/O.
- Los valores de una columna son del mismo tipo y parecidos, así que se comprimen muy bien.
- Guarda mínimo y máximo por archivo, lo que permite saltear archivos enteros.

**Poco conveniente para modificar una fila por vez:**
- Una fila está repartida entre varias columnas y archivos, así que modificarla obliga a tocar varios lugares.
- Los archivos son comprimidos e inmutables: para cambiar un valor hay que reescribir el archivo entero (o agregar capas, como hace Delta Lake).
- Para eso conviene un formato por filas (base transaccional, OLTP), donde la fila está junta y se lee o escribe de una vez.

## Entrega

La publicación, el acceso público y el envío a los profesores siguen el [formato común de entrega](../README.md#formato-común-de-entrega).

La entrega es **un único `README.md`** con las respuestas a las 20 preguntas, dentro de `resolucion-practica-3/` en tu repositorio personal:

```text
resolucion-practica-3/
└── README.md
```

El README debe incluir nombre, `student_id` y escala. Podés partir de la [plantilla](PLANTILLA_ENTREGA.md). No hace falta exportar los notebooks.

No incluyas datos generados, archivos del volumen, credenciales ni tokens.
