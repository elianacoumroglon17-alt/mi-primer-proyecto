#Práctica 1 - Eliana Gómez Coumroglon

## 1) Observaciones sobre JSON/CSV, Parquet, Delta

Para el caso de customers y transactions (ambas vienen de csv), puedo ver que se infiere que todas las columnas son del tipo string, incluso amount que debería ser numérico. Cuando se aplica inferSchema=true se reconoce transaction_id, customer_id y product_id como integer, event_ts como timestamp, pero amount sigue quedando como string. 
Con respecto al Parquet (products), vemos que se lee con los tipos de datos correctos (product_id:long, category:string y price:decimal(12,2)) sin necesidad de aplicar inferSchema=true.
Por último, luego guardar las tablas en formato Delta, DESCRIBE HISTORY muestra el versionado de cada escritura y DESCRIBE DETAIL agrega metadatos que un Parquet no ofrece por sí solo. Delta agrega esa capa transaccional sobre el mismo formato columnar de Parquet.

## 2) Las 5 V

 - Volumen: Las cuatro tablas Bronze generadas suman un volumen considerable (bronze_customers (5.000 filas), bronze_products (500 filas), bronze_transactions (50.011 filas) y bronze_events (200.000 filas).
 - Velocidad: Si bien la ingesta se hizo en modo batch, el volumen representaría una fuente de alta frecuencia (como clics o interacciones de usuarios), que en este caso llegarían de forma continua.
 - Variedad: Los datos de origgen combinan distintos formatos, JSON, Parquet y csv. 
 - Veracidad: el diagnóstico de calidad detectó inconsistencias, de 50011 filas totales, 50000 corresponden a valores distintos (11 duplicados). Además, 52 valores de amount no pueden convertirse a decimal. 
 - Valor: una vez resueltos los problemas de calidad en capas posteriores (Silver/Gold), estos datos permitirían análisis de negocio como detección de fraude (is_fraud), ventas por canal de pago (payment_channel) o comportamiento de clientes por segmento.


