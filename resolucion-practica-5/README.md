# Entrega práctica 5 — MLOps con MLflow

- **Nombre:** Eliana Gómez Coumroglon
- **student_id:** elianacoumroglon
- **Escala:** small

## Mis runs

| run | modelo | features | class_weight | umbral | hiperparametros | train_pr_auc | pr_auc | accuracy | precision | recall | f1 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| exp6 | gradient_boosting | completas | ninguno | 0.4 | learning_rate=0.03, max_depth=8 | 0.64 | 0.585 | 0.916 | 0.633 | 0.454 | 0.529 |
| exp1 | gradient_boosting | completas | ninguno | 0.5 | learning_rate=0.1, max_depth=5 | 0.669 | 0.582 | 0.918 | 0.709 | 0.353 | 0.471 |
| max_depth=5 | gradient_boosting | completas | balanced | 0.4 | learning_rate=0.03, max_depth=5 | 0.613 | 0.581 | 0.748 | 0.274 | 0.859 | 0.415 |
| max_depth=sin_limite | gradient_boosting | completas | balanced | 0.4 | learning_rate=0.03, max_depth=sin_limite | 0.612 | 0.577 | 0.755 | 0.277 | 0.845 | 0.418 |
| max_depth=8 | gradient_boosting | completas | balanced | 0.4 | learning_rate=0.03, max_depth=8 | 0.613 | 0.577 | 0.756 | 0.278 | 0.845 | 0.418 |
| exp7 | gradient_boosting | completas | balanced | 0.4 | learning_rate=0.03, max_depth=8 | 0.613 | 0.577 | 0.756 | 0.278 | 0.845 | 0.418 |
| max_depth=3 | gradient_boosting | completas | balanced | 0.4 | learning_rate=0.03, max_depth=3 | 0.585 | 0.576 | 0.721 | 0.255 | 0.876 | 0.395 |
| exp6 | gradient_boosting | completas | ninguno | 0.4 | learning_rate=0.3, max_depth=8 | 0.658 | 0.567 | 0.911 | 0.591 | 0.457 | 0.515 |
| max_depth=8 | random_forest | completas | ninguno | 0.5 | n_estimators=200, max_depth=8 | 0.644 | 0.564 | 0.914 | 0.804 | 0.222 | 0.347 |
| exp8 | logistic_regression | completas | ninguno | 0.3 | C=1.0 | 0.531 | 0.545 | 0.899 | 0.515 | 0.507 | 0.511 |
| exp5 | logistic_regression | completas | ninguno | 0.3 | C=1.0 | 0.531 | 0.545 | 0.899 | 0.515 | 0.507 | 0.511 |
| exp3 | logistic_regression | completas | ninguno | 0.3 | C=1.0 | 0.531 | 0.545 | 0.899 | 0.515 | 0.507 | 0.511 |
| max_depth=sin_limite | random_forest | completas | ninguno | 0.5 | n_estimators=200, max_depth=sin_limite | 1.0 | 0.544 | 0.914 | 0.673 | 0.328 | 0.441 |
| max_depth=5 | random_forest | completas | ninguno | 0.5 | n_estimators=200, max_depth=5 | 0.559 | 0.54 | 0.903 | 0.927 | 0.069 | 0.128 |
| exp1 | random_forest | completas | ninguno | 0.5 | n_estimators=200, max_depth=5 | 0.559 | 0.54 | 0.903 | 0.927 | 0.069 | 0.128 |

**Run registrado:** exp6

## Datos y tracking

**1.**
**Filas y porcentaje de contracargos por split:**
- `train` (1 al 5 de marzo): **35.654** filas, **11,05 %** de contracargos.
- `test` (6 de marzo): **7.129** filas, **10,38 %** de contracargos.
- `nuevas` (7 de marzo): **7.165** filas, **11,05 %** de contracargos.

Se entrena con el pasado y se evalúa con lo que viene después, como pasaría en producción. Con una partición al azar, el entrenamiento incluiría transacciones posteriores a las del test, es decir, información del futuro y la métrica saldría más optimista que el rendimiento real. La partición temporal también refleja que el comportamiento del fraude puede cambiar con el tiempo.

**2.**
Un "modelo" que siempre dice que no hay contracargo obtiene una accuracy de **89,6 %** sobre `test`, sin detectar ni un solo contracargo. Se usa el PR-AUC como métrica principal porque las clases están desbalanceadas (≈ 10 % de contracargos): la accuracy engaña, ya que un modelo inútil saca una nota alta. El PR-AUC resume la relación entre precision y recall. Se centra en la clase minoritaria que importa y no depende de un umbral de decisión.

**3.**
En el run `01_manual_logistica` quedó registrado:
- **Parámetros**: `model_type = logistic_regression`, `feature_set = basicas`, `C = 1.0`, `class_weight = ninguno`, `threshold = 0.5`, `data_table = ml_features` y `data_version`.
- **Métricas**
- **Artefactos:** la matriz de confusión (`matriz_confusion.png`) y el modelo con su firma (*signature*). También quedaron etiquetas (*tags*), como el tipo de run y la ubicación del modelo (`model_uri`).

**Diferencia entre experimento y run:** un experimento es el contenedor que agrupa todos los intentos de resolver un mismo problema y un run es una ejecución concreta de un entrenamiento dentro de él, con sus propios parámetros, métricas y artefactos.

**4.**
Autolog registró 46 parámetros (los del pipeline y del bosque, como `modelo__max_depth = 8`, `modelo__criterion = gini` y `modelo__max_features = sqrt`) y 7 métricas (`training_accuracy_score` (0,91), `training_f1_score` (0,887), `training_log_loss` (0,229), `training_precision_score` (0,901), `training_recall_score` (0,91), `training_roc_auc` (0,899) y `training_score` (0,91)). También guardó artefactos (`estimator.html` y los gráficos de matriz de confusión, curva precision-recall y curva ROC de entrenamiento).

Lo que autolog no registra y sí registramos a mano:
- Parámetros propios del experimento: `feature_set`, `threshold`, `class_weight` elegido, `data_table` y `data_version`.
- Las métricas sobre `test`, incluido el PR-AUC, y `train_pr_auc` para comparar y detectar sobreajuste.
- Artefactos y etiquetas propios, como la matriz de confusión sobre `test` y el `model_uri`.

**5.**
Sí, `01_repro_a` y `01_repro_b` dieron las mismas métricas: PR-AUC de test 0,5099, ROC-AUC 0,8693, accuracy 0,9094, precision 0,7217, recall 0,2068, F1 0,3214, 212 alertas y `train_pr_auc` 0,5941. Las cuatro cosas que hay que fijar para que un run sea reproducible son: los datos (la misma tabla y versión, `data_version`), el código, la semilla aleatoria (`random_state`) y el entorno (las mismas versiones de las librerías).

**6.**
Al llamar al modelo sin la columna `amount`, MLflow lo frenó con un error antes de predecir. La firma signature del modelo declara qué columnas y tipos de datos espera como entrada y qué devuelve. Sirve para validar los datos antes de predecir: si no cumplen el contrato, el modelo falla en lugar de dar resultados erróneos. 

## Tus experimentos

**7.**
Mi mejor run fue un **gradient boosting** con las variables `completas`, `learning_rate = 0.03`, `max_depth = 8`, umbral 0.4 y sin `class_weight`, con un PR-AUC de test de 0,585 (`train_pr_auc` 0,640, ROC-AUC 0,888, precision 0,633 y recall 0,454). Usando sólo `rule_alert` (`solo_regla`) el PR-AUC es de 0,170, así que el modelo mejora 0,415 puntos de PR-AUC.

**8.**
Con la regresión logística** (`C = 1,0`), el PR-AUC de test pasó de 0,339 con `basicas` a 0,448 con `cliente` y a 0,545 con `completas` (**+0,097**), es decir, +0,206 en total. El PR-AUC no depende del umbral, así que la comparación es válida aunque esos runs usaron umbrales distintos. Con random forest (`max_depth = 8`) pasa lo mismo: 0,318 → 0,511 → 0,564. En mi mejor run (gradient boosting con `completas`), medidas por la caída del PR-AUC al desordenar cada columna, las variables más importantes fueron `amount` (≈ 0,34), `payment_channel` (≈ 0,19), `hour` (≈ 0,13), `category` (≈ 0,08) y `account_age_days` (≈ 0,06). `device_customers` y `segment` aportaron poco, y `price`, `is_night`, `country`, `day_of_week` y `amount_to_price` casi nada.

**9.**
Con `completas` anduvo mejor el boosting (gradient boosting, PR-AUC de test 0,585), seguido de random forest (0,564, con `max_depth = 8`) y de la logística (0,545). Un modelo de árboles aprovecha combinaciones de variables porque cada árbol divide los datos por pasos: primero por una variable y después, dentro de cada rama, por otra (por ejemplo, "de noche" y, dentro de eso, "por transferencia" y "monto alto"). Así puede aprender que el riesgo sube sólo cuando se dan varias condiciones juntas. La logística es un modelo lineal: suma el efecto de cada variable por separado, así que no ve esas interacciones a menos que se las agreguemos a mano como una variable nueva. El boosting además construye los árboles en secuencia y cada uno corrige los errores del anterior, lo que lo ayuda a captar patrones más finos.

**10.**
Con el mismo modelo (gradient boosting, `completas`, umbral 0.4, `max_depth = 8`, `learning_rate = 0,03`), pasar de `class_weight = ninguno` a `balanced` cambió así:
- Precision: bajó de 0.633 a 0.278.
- Recall: subió de 0.454 a 0.845.
- Alertas: subieron de ≈ 530 a ≈ 2.250.
- PR-AUC: casi no cambió: de 0.585 a 0.577.

Con `balanced` el modelo da más peso a la clase minoritaria (los contracargos), así que detecta muchos más casos (más recall) pero genera muchas más falsas alertas (menos precision). Como cambia la forma de decidir y no tanto el orden de riesgo que el modelo asigna a las transacciones, el PR-AUC varía poco.

**11.**
Con la regresión logística (`completas`, `C = 1.0`), al bajar el umbral de 0.5 a 0.3: bajó la precisión, aumentó el recall, aumentaron las alertas y PR-AUN se mantuvo igual. 
Como se bajó el umbral, el modelo marca como alerta mayor cantidad de transacciones, así que detecta más contragargos pero genera también más falsas alertas. El PR-AUC no depende del umbral porque se calcula recorriendo todos los umbrales posiblesa partir de los puntajes del modelo. 

**12.**
Barrí el hiperparámetro `max_depth` en un `random_forest` con `completas`. El mejor PR-AUC de test se obtuvo con `max_depth = 8`: 0.568 (con 3, 0.525; con 5, 0.541; y sin límite, 0.537). Con `max_depth = sin_limite` el `train_pr_auc` fue 1.0, mucho más alto que el de test (0.537): el modelo memorizó los datos de entrenamiento y no generaliza a datos nuevos. Ese fenómeno se llama overfitting. 

## Comparar y elegir

**13.**
No coincidieron. Mi mejor run por PR-AUC fue el gradient boosting con `completas`, umbral 0.4, `max_depth = 8` y `learning_rate = 0.03` (PR-AUC 0.585, accuracy 0.916). El mejor por accuracy fue el gradient boosting con `completas`, umbral 0.5, `max_depth = 5` y `learning_rate = 0,=.1` (accuracy 0.918, PR-AUC 0.582).

Pueden no coincidir porque miden cosas distintas. El accuracy depende del umbral y cuenta todos los aciertos. El PR-AUC mide qué tan bien el modelo ordena el riesgo de la clase minoritaria, sin depender de un umbral. Por eso un run con `class_weight = balanced` tiene PR-AUC casi igual (0,577) pero una accuracy mucho menor (0,756): detecta más contracargos, pero con muchas más falsas alertas.

**14.**
Los runs que quedan más lejos de la diagonal son los random forest con `max_depth = sin_limite`: tienen un PR-AUC de train ≈ 1.0 pero un PR-AUC de test mucho menor. Eso indica overfitting: con árboles sin límite de profundidad el modelo memoriza los datos de entrenamiento, rinde casi perfecto ahí y generaliza mal a datos que no vio. 

**15.**
Consultar con `mlflow.search_runs` permite filtrar, ordenar y analizar los runs desde el código: devuelve un DataFrame de pandas, es reproducible y escala a muchos runs. También permite automatizar pasos posteriores. La interfaz sirve para explorar, pero es manual.

`filter_string` para traer sólo los bosques con PR-AUC de test mayor a 0.55:

```python
filter_string = "params.model_type = 'random_forest' and metrics.test_pr_auc > 0.55"
```

**16.**
En fraude cuesta más un contracargo no detectado: se pierde el dinero de la compra, se paga el contracargo y puede haber una multa o un daño al comercio, mientras que una alerta falsa cuesta el tiempo de revisión y a veces, una molestia al cliente. Si el equipo de riesgo sólo puede revisar una cantidad fija de alertas por día, usaría el umbral para ajustar el volumen de alertas a esa capacidad: bajar el umbral si sobra capacidad y subirlo si hay demasiadas alertas.

## Registro, uso y monitoreo

**17.**
Sí, el modelo se pudo registrar en Unity Catalog:
- **Nombre:** `workspace.bigdata_elianacoumroglon.fraud_model`
- **Versión:** 1
- **Alias:** `champion`

Ventaja de cargar `models:/...@champion` en lugar de `runs:/<run_id>/model`: el alias apunta siempre a la versión del modelo que está vigente, así que el código de inferencia no cambia cuando se publica una versión mejor: basta con mover el alias `champion` a la nueva versión. Con `runs:/<run_id>/model` hay que cambiar el `run_id` en el código cada vez, y no hay control de versiones ni de qué modelo está en producción.

**18.**
Se puntuaron 7.165 transacciones nuevas y 540 generaron alerta (7.5 %). Las columnas de `ml_predictions` que permiten auditar cada decisión son: `transaction_id` (qué transacción), `score` (el puntaje de riesgo del modelo), `alerta` (la decisión), `threshold` (el umbral con que se decidió), `model_name` y `model_version` (qué modelo y qué versión puntuó), `run_id` (el run de entrenamiento que originó ese modelo) y `scored_at` (cuándo se puntuó). Con ellas se puede reconstruir cada decisión de punta a punta. El umbral se guarda fuera del modelo porque es una decisión de negocio y no algo que el modelo aprende: así se puede cambiar sin reentrenar ni crear una nueva versión del modelo, y se registra junto a cada predicción para poder auditarla.

**19.**
Sí, el rendimiento sobre `nuevas` fue muy parecido al de `test`: el PR-AUC pasó de 0.585 a 0.565, el ROC-AUC de 0.888 a 0.886, la precision se mantuvo, el recall bajó levemente y la accuracy también. El porcentaje de alertas también se mantuvo.

Drift es el cambio en el tiempo de los datos que recibe el modelo, de modo que deja de parecerse a los datos con los que se entrenó y su rendimiento se degrada. Se puede detectar*antes de tener las etiquetas porque se observan señales que no requieren saber si hubo contracargo: la distribución de las variables de entrada, la distribución de los puntajes y el porcentaje de alertas. Como los contracargos confirmados llegan con días o semanas de retraso, esas señales sirven de alarma temprana.

## Cierre

**20.**

**Datos**: armé las tablas `ml_features` respetando el orden temporal para evitar fuga de información. 

**Entrenamiento:** probé varios modelos, features, umbrales y barridos de hiperparámetros, eligiendo por PR-AUC por el desbalance de clases. 

**Tracking:** registré parámetros, métricas, firma y artefactos de cada corrida en MLflow (autolog y manual) para compararlas y reproducirlas. 

**Registro:** registré el mejor modelo (exp6, gradient boosting, PR-AUC 0,585) en Unity Catalog como `fraud_model` con el alias `champion`. 

**Despliegue:** hice inferencia batch cargando `models:/...@champion` y guardé las predicciones en `ml_predictions` con trazabilidad (versión, `run_id`, umbral, fecha). 

**Monitoreo:** comparé las métricas de test contra `nuevas` (PR-AUC 0,585 vs 0,565) y miré el porcentaje de alertas para detectar drift.

**Faltaría:** 
 1) **Automatizar el pipeline** con un job programado y reentrenamiento periódico
 2) **monitoreo continuo con alertas** (drift de datos y de puntajes, caída de métricas cuando llegan los contracargos) y un plan de rollback.

