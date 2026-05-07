# 💻Clase 23 - SparkML

---

# Agenda:

<aside>
💡

#### 9:00 - 9:50    → Introducción a SparkML

#### 9:50 - 11:20   →  Ejercicios

#### **11:20 - 11:40  →  Descanso**

#### 11:40 - 12:40  → Caso practico

#### 12:40 - 14:00  → Crear cuenta de Azure

</aside>

---

# 🤖  Introducción a SparkMLib

---

### 1. ¿Qué es MLlib y para qué sirve?

<aside>

**MLlib** es la biblioteca de **Machine Learning distribuido** incluida en Apache Spark. Está diseñada para entrenar modelos sobre grandes volúmenes de datos que no caben en una sola máquina.

</aside>

### ¿Por qué MLlib y no scikit-learn o TensorFlow?

| Situación | Herramienta recomendada |
| --- | --- |
| Dataset pequeño (cabe en RAM) | scikit-learn, statsmodels |
| Dataset enorme (millones de filas) | **Spark MLlib** |
| Deep Learning (redes neuronales) | TensorFlow, PyTorch |
| Pipeline ETL + ML en el mismo job | **Spark MLlib** |

MLlib brilla cuando los datos son tan grandes que necesitan estar distribuidos en un clúster. No es la mejor herramienta para datasets pequeños.

### ¿Qué tipos de problemas resuelve MLlib?

![image.png](image.png)

> En esta sesión trabajaremos con **regresión lineal**, el algoritmo más básico y el mejor punto de partida para entender el flujo completo de MLlib.
> 

---

### 2. Los tres conceptos clave: Transformer, Estimator, Pipeline

MLlib organiza todo el trabajo de machine learning en torno a tres piezas. La clave es entender qué hace cada una y **cuándo aparece en el flujo de trabajo**. 

### 2.1 Transformer (Transformador)

<aside>

Un **Transformer** recibe un DataFrame, le **añade una o más columnas nuevas**, y devuelve un DataFrame nuevo. No aprende nada del pasado. No tiene memoria. Sólo aplica una operación fija. 

</aside>

> Se usa llamando a `.transform(df)`.
> 

**Ejemplos:**

| Transformer | Qué hace |
| --- | --- |
| `VectorAssembler` | Junta varias columnas numéricas en una sola columna de tipo `Vector` |
| `StringIndexer` | Convierte texto a números: "Madrid"→0, "Barcelona"→1 |
| `Binarizer` | Convierte un número a 0 o 1 según un umbral que tú defines |
| `Tokenizer` | Divide una frase en palabras individuales |

```scala
// Un Transformer sólo aplica una transformación fija
val ensamblador = new VectorAssembler()
  .setInputCols(Array("edad", "salario", "experiencia"))
  .setOutputCol("features")          // nueva columna que añade

val dfConFeatures = ensamblador.transform(dfOriginal)
// dfOriginal tenía 3 columnas → dfConFeatures tiene 4 columnas (las 3 + "features")
```

```scala
// Un Transformer se aplica con .transform(df)
val dfTransformado = miTransformer.transform(dfOriginal)
```

### 2.2 Estimator (Estimador)

Un **Estimator** **aprende de los datos** (fase de entrenamiento) y luego produce un Transformer ya entrenado. Es decir: un Estimator no transforma directamente; primero *se entrena* con `.fit(df)`, y eso devuelve un modelo que sí puede transformar.

> Un Estimator es como un estudiante que estudia un examen (`fit`). Una vez que ha aprendido, se convierte en un experto (un Transformer entrenado) que puede responder preguntas (`transform`).
> 

![image.png](image%201.png)

![image.png](image%202.png)

```scala
// Primero: entrenar (aprender de los datos)
val lr = new LinearRegression()
  .setFeaturesCol("features")
  .setLabelCol("precio")

val modelo = lr.fit(dfEntrenamiento)   // aquí "estudia" los datos
// modelo es ahora un LinearRegressionModel, que es un Transformer

// Segundo: predecir sobre datos nuevos
val predicciones = modelo.transform(dfTest)
```

> La diferencia entre Transformer y Estimator es que el Estimator necesita ver los datos antes de poder funcionar. El Transformer no.
> 

### 2.3 Pipeline (Tubería)

Un **Pipeline** es una cadena de pasos (Transformers y/o Estimators) que se ejecutan en orden, uno tras otro, pasando el DataFrame de etapa en etapa. Es la forma de ensamblar un flujo de ML completo en un único objeto reutilizable.

![image.png](image%203.png)

```scala
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.classification.LogisticRegression

// Paso 1: convertir la columna "ciudad" (texto) a número
val indexer = new StringIndexer()
  .setInputCol("ciudad")
  .setOutputCol("ciudadIdx")

// Paso 2: juntar columnas numéricas en un vector de features
val assembler = new VectorAssembler()
  .setInputCols(Array("edad", "salario", "ciudadIdx"))
  .setOutputCol("features")

// Paso 3: el algoritmo de clasificación
val lr = new LogisticRegression()
  .setFeaturesCol("features")
  .setLabelCol("compro")

// Ensamblar el pipeline
val pipeline = new Pipeline().setStages(Array(indexer, assembler, lr))

// Entrenar TODO el pipeline de una sola vez
val modeloPipeline = pipeline.fit(dfEntrenamiento)

// Aplicar a datos nuevos de una sola vez
val resultados = modeloPipeline.transform(dfTest)
```

---

### 3. Preparación de features: `VectorAssembler`

<aside>

MLlib **sólo acepta vectores numéricos** como entrada para sus algoritmos. No puede trabajar directamente con múltiples columnas separadas. Por eso el primer paso casi siempre es usar `VectorAssembler`.

</aside>

> `VectorAssembler` toma varias columnas numéricas y las combina en **una sola columna de tipo `Vector`** llamada habitualmente `features`.
> 

```scala
import org.apache.spark.ml.feature.VectorAssembler

val assembler = new VectorAssembler()
  .setInputCols(Array("superficie", "habitaciones", "antiguedad"))
  .setOutputCol("features")

val dfConFeatures = assembler.transform(dfOriginal)
```

![image.png](image%204.png)

---

## 4. Regresión lineal y evaluación de modelos

### 4.1 ¿Qué es la regresión lineal?

![image.png](image%205.png)

![image.png](image%206.png)

### 4.2 Flujo completo en código

```scala
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator

// 1. Preparar features
val assembler = new VectorAssembler()
  .setInputCols(Array("superficie", "habitaciones"))
  .setOutputCol("features")

// 2. Definir el algoritmo
val lr = new LinearRegression()
  .setFeaturesCol("features")  // columna de entrada
  .setLabelCol("precio")       // columna a predecir
  .setMaxIter(10)              // iteraciones máximas

// 3. Construir el Pipeline
val pipeline = new Pipeline()
  .setStages(Array(assembler, lr))

// 4. Dividir datos: 80% entrenamiento, 20% prueba
val Array(trainData, testData) = df.randomSplit(Array(0.8, 0.2), seed = 42)

// 5. Entrenar
val model = pipeline.fit(trainData)

// 6. Predecir sobre datos de prueba
val predicciones = model.transform(testData)
predicciones.select("precio", "prediction").show(5)

// 7. Evaluar
val evaluator = new RegressionEvaluator()
  .setLabelCol("precio")
  .setPredictionCol("prediction")
  .setMetricName("rmse")   // Root Mean Squared Error

val rmse = evaluator.evaluate(predicciones)
println(s"RMSE = $rmse")
```

### 4.3 ¿Qué es el RMSE?

<aside>

**RMSE** (Root Mean Squared Error) mide cuánto se equivoca el modelo en promedio, en las mismas unidades que la variable que predecimos.

</aside>

```
Si predecimos precios de casas en euros:
  RMSE = 15.000 €  →  el modelo se equivoca ~15.000 € de media
  RMSE = 5.000 €   →  el modelo es más preciso
```

<aside>

Cuanto más bajo, mejor. Un RMSE de 0 significaría predicciones perfectas (lo cual es sospechoso y suele indicar sobreajuste).

</aside>

### 4.4 Otras métricas disponibles en `RegressionEvaluator`

![image.png](image%207.png)

---

### 5. Resumen: el flujo MLlib de principio a fin

![image.png](image%208.png)

---

## 💻 BLOQUE PRÁCTICO

---

### ⚙️ Celda 0 — Configuración del entorno

**Celda 1 — Dependencias** *(ejecutar una sola vez al inicio de la sesión)*

```scala
import $ivy.`org.apache.spark::spark-sql:4.1.1`
import $ivy.`org.apache.spark::spark-mllib:4.1.1`
```

**Celda 2 — Crear SparkSession**

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .master("local[*]")
	  .appName("MLlib-c23")
  .config("spark.ui.enabled", "false")
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

println(s"Spark ${spark.version} listo")
// Salida esperada: Spark 4.1.1 listo
```

---

### P1 — Crear y explorar el dataset de casas

Vamos a trabajar con datos sintéticos de precios de viviendas. Este ejercicio enseña a crear un DataFrame manualmente y a inspeccionarlo antes de aplicar ML.

**Celda 1**

```scala
import spark.implicits._

val casas = Seq(
  (50.0,  1, 30, 120000.0),
  (65.0,  2, 20, 155000.0),
  (80.0,  3, 10, 210000.0),
  (95.0,  3,  5, 265000.0),
  (110.0, 4,  8, 310000.0),
  (75.0,  2, 15, 190000.0),
  (130.0, 4,  2, 370000.0),
  (60.0,  2, 25, 145000.0),
  (90.0,  3, 12, 240000.0),
  (55.0,  1, 40, 110000.0),
  (100.0, 4, 18, 280000.0),
  (85.0,  3,  7, 225000.0)
).toDF("superficie", "habitaciones", "antiguedad", "precio")
```

**Celda 2**

```scala
println("=== Esquema ===")
casas.printSchema()
```

```scala
println("=== Primeras filas ===")
casas.show()
```

```scala
println("=== Estadísticas ===")
casas.describe().show()
```

**Salida esperada:**

```
=== Esquema ===
root
 |-- superficie: double (nullable = false)
 |-- habitaciones: integer (nullable = false)
 |-- antiguedad: integer (nullable = false)
 |-- precio: double (nullable = false)

=== Estadísticas ===
+-------+------------------+------------------+------------------+-----------------+
|summary|        superficie|      habitaciones|        antiguedad|           precio|
+-------+------------------+------------------+------------------+-----------------+
|  count|                12|                12|                12|               12|
|   mean| 82.91666666666667|2.8333333333333335|16.833333333333332|243333.3333333...|
|  stddev|24.237485...      |0.9370...         |12.027...         |81648.5...       |
|    min|              50.0|                 1|                 2|         110000.0|
|    max|             130.0|                 4|                40|         370000.0|
+-------+------------------+------------------+------------------+-----------------+
```

---

### P2 — Aplicar `VectorAssembler`

Convierte las columnas numéricas en la columna `features` que necesita MLlib.

**Celda 1**

```scala
import org.apache.spark.ml.feature.VectorAssembler

val assembler = new VectorAssembler()
  .setInputCols(Array("superficie", "habitaciones", "antiguedad"))
  .setOutputCol("features")
```

**Celda 2**

```scala
val casasConFeatures = assembler.transform(casas)

casasConFeatures.select("superficie", "habitaciones", "antiguedad", "features", "precio").show(5, truncate = false)
```

**Salida esperada:**

```
+----------+------------+----------+------------------+---------+
|superficie|habitaciones|antiguedad|features          |precio   |
+----------+------------+----------+------------------+---------+
|50.0      |1           |30        |[50.0,1.0,30.0]   |120000.0 |
|65.0      |2           |20        |[65.0,2.0,20.0]   |155000.0 |
|80.0      |3           |10        |[80.0,3.0,10.0]   |210000.0 |
|95.0      |3           |5         |[95.0,3.0,5.0]    |265000.0 |
|110.0     |4           |8         |[110.0,4.0,8.0]   |310000.0 |
+----------+------------+----------+------------------+---------+
```

> 📝 **Observa:** la columna `features` es un vector con exactamente los valores de las tres columnas de entrada, en el orden que especificamos en `setInputCols`.
> 

---

### P3 — Dividir datos y entrenar el modelo

**Celda 1**

```scala
val Array(trainData, testData) = casasConFeatures.randomSplit(Array(0.8, 0.2), seed = 42)

println(s"Filas de entrenamiento: ${trainData.count()}")
println(s"Filas de prueba:        ${testData.count()}")
```

**Salida esperada** *(puede variar ligeramente según el tamaño del dataset)*:

```
Filas de entrenamiento: 10
Filas de prueba:        2
```

**Celda 2**

```scala
import org.apache.spark.ml.regression.LinearRegression

val lr = new LinearRegression()
  .setFeaturesCol("features")
  .setLabelCol("precio")
  .setMaxIter(100)

val model = lr.fit(trainData)
```

**Celda 3**

```scala
// Inspeccionar los coeficientes aprendidos
println(s"Coeficientes: ${model.coefficients}")
println(s"Intercept:    ${model.intercept}")
```

**Salida esperada** *(valores aproximados — MLlib los ajusta durante el entrenamiento)*:

```
Coeficientes: [2857.659630559311,6285.207997557454,-495.8103905908932]
Intercept:    -26261.428930917165
```

> 📝 **Interpreta:** El primer coeficiente (~2800) indica que cada metro cuadrado extra suma aproximadamente 2.800 € al precio. El coeficiente negativo de antigüedad indica que a más años, menos precio.
> 

---

### P4 — Hacer predicciones y evaluarlas

**Celda 1**

```scala
val predicciones = model.transform(testData)

predicciones.select("superficie", "habitaciones", "antiguedad", "precio", "prediction").show(truncate = false)
```

**Salida:**

```scala
	+----------+------------+----------+--------+------------------+
|superficie|habitaciones|antiguedad|precio  |prediction        |
+----------+------------+----------+--------+------------------+
|65.0      |2           |20        |155000.0|162140.65523873508|
|85.0      |3           |7         |225000.0|232024.5909251604 |
+----------+------------+----------+--------+------------------+
```

**Celda 2**

```scala
import org.apache.spark.ml.evaluation.RegressionEvaluator

val evaluatorRMSE = new RegressionEvaluator()
  .setLabelCol("precio")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

val evaluatorR2 = new RegressionEvaluator()
  .setLabelCol("precio")
  .setPredictionCol("prediction")
  .setMetricName("r2")

val rmse = evaluatorRMSE.evaluate(predicciones)
val r2   = evaluatorR2.evaluate(predicciones)

println(f"RMSE = $rmse%.2f €")
println(f"R²   = $r2%.4f")
```

Salida:

```scala
RMSE = 7082,86 €
R²   = 0,9590
```

> 📝 **Interpreta el R²:** Un valor cercano a 1.0 indica que el modelo explica bien la varianza en los precios. Con tan solo 12 filas los resultados son orientativos; en datos reales se necesitan miles de ejemplos.
> 

---

### P5 — Pipeline completo de principio a fin

Ahora encadenamos todo en un `Pipeline` para ver cómo se hace profesionalmente.

**Celda 1**

```scala
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
```

**Celda 2**

```scala
val assembler2 = new VectorAssembler()
  .setInputCols(Array("superficie", "habitaciones", "antiguedad"))
  .setOutputCol("features")

val lr2 = new LinearRegression()
  .setFeaturesCol("features")
  .setLabelCol("precio")
  .setMaxIter(100)

val pipeline = new Pipeline()
  .setStages(Array(assembler2, lr2))
```

Salida:

```scala
assembler2: VectorAssembler = VectorAssembler: uid=vecAssembler_9931e4618876, handleInvalid=error, numInputCols=3
lr2: LinearRegression = linReg_de683b92b5fd
pipeline: Pipeline = pipeline_80bd58922089
```

**Celda 3**

```scala
// Partir los datos ANTES del pipeline (sobre el df original sin features)
val Array(train2, test2) = casas.randomSplit(Array(0.8, 0.2), seed = 42)

// Entrenar el pipeline completo
val pipelineModel = pipeline.fit(train2)

// Predecir
val preds2 = pipelineModel.transform(test2)
preds2.select("superficie", "precio", "prediction").show(truncate = false)
```

Salida:

```scala
+----------+--------+------------------+
|superficie|precio  |prediction        |
+----------+--------+------------------+
|65.0      |155000.0|162140.65523873508|
|85.0      |225000.0|232024.5909251604 |
+----------+--------+------------------+
```

**Celda 4**

```scala
val evaluator2 = new RegressionEvaluator()
  .setLabelCol("precio")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

val rmse2 = evaluator2.evaluate(preds2)
println(f"RMSE del Pipeline = $rmse2%.2f €")
```

Salida:

```scala
	RMSE del Pipeline = 7082,86 €
```

> 📝 **Ventaja del Pipeline:** En el ejercicio anterior teníamos que llamar a `assembler.transform()` manualmente sobre `trainData` y `testData` por separado. Con el Pipeline, basta con llamar a `pipeline.fit(train2)` y él se encarga de ejecutar todas las etapas en orden, tanto al entrenar como al predecir.
> 

---

### P6 — Comparar usando sólo una feature vs todas

¿Cambia mucho el modelo si sólo usamos la superficie en lugar de las tres variables?

**Celda 1**

```scala
val assemblerSolo = new VectorAssembler()
  .setInputCols(Array("superficie"))
  .setOutputCol("features")

val lrSolo = new LinearRegression()
  .setFeaturesCol("features")
  .setLabelCol("precio")
  .setMaxIter(100)

val pipelineSolo = new Pipeline()
  .setStages(Array(assemblerSolo, lrSolo))
```

**Celda 2**

```scala
val modelSolo = pipelineSolo.fit(train2)
val predsSolo = modelSolo.transform(test2)

val evaluator3 = new RegressionEvaluator()
  .setLabelCol("precio")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

val rmseSolo = evaluator3.evaluate(predsSolo)
println(f"RMSE con sólo superficie   = $rmseSolo%.2f €")
println(f"RMSE con las 3 variables   = $rmse2%.2f €")

if (rmse2 < rmseSolo)
  println("✅ El modelo con más variables predice mejor")
else
  println("⚠️ Con pocos datos, añadir variables no siempre mejora")
```

```scala
RMSE con sólo superficie   = 3052,14 €
RMSE con las 3 variables   = 7082,86 €
⚠️ Con pocos datos, añadir variables no siempre mejora
```

> 📝 **Reflexión:** Con datasets muy pequeños (12 filas) los resultados son poco fiables. En producción, con miles de registros, añadir variables relevantes casi siempre mejora el RMSE.
> 

---

# 🏭 Caso de Estudio — LogiTrack S.A.

### Predicción de Tiempo de Entrega en una Red Logística Nacional

### Dos vías de implementación: Local y Azure Databricks

---

---

## 🏢 Contexto empresarial

**LogiTrack S.A.** es una empresa de logística nacional que gestiona miles de envíos diarios entre almacenes y clientes finales en toda España. El equipo directivo ha detectado que los retrasos en las entregas están generando reclamaciones y penalizaciones contractuales. El **Director de Operaciones** ha encargado al equipo de datos construir un modelo predictivo que estime el tiempo de entrega (en horas) antes de que el paquete salga del almacén.

Como I**ngeniero de datos**, implementarás este modelo en dos entornos profesionales distintos que representan dos realidades habituales en la industria.

---

## 📋 Requerimientos del modelo

| Variable | Tipo | Descripción |
| --- | --- | --- |
| `distancia_km` | Double | Distancia entre almacén y destino |
| `peso_kg` | Double | Peso del paquete |
| `num_paradas` | Int | Paradas intermedias en la ruta |
| `hora_salida` | Int | Hora de salida (0–23) |
| `tiempo_entrega_horas` | Double | **Label** — lo que predecimos |

---

## 🗺️ Visión general de las dos vías

![image.png](image%209.png)

> ⚠️ **Nota sobre versiones:** Las dos vías usan versiones distintas de Spark (4.1.1 vs 3.5.2). El código MLlib es idéntico en ambas. El modelo serializado **no es portable** entre versiones — cada vía entrena y despliega su propio modelo en su propio entorno. Esto es exactamente lo que ocurre en la industria real.
> 

---

---

# ══════════════════════════════════════════════

# VÍA 1 — Entorno Local (Almond + sbt + spark-submit)

# Spark 4.1.1 · Scala 2.13.18 · Windows 11

# ══════════════════════════════════════════════

---

# PARTE A — Notebook de desarrollo (Almond / Jupyter / VSCode)

---

## 🔧 Inicialización

### Celda 1 — Dependencias *(ejecutar una sola vez)*

```scala
import $ivy.`org.apache.spark::spark-sql:4.1.1`
import $ivy.`org.apache.spark::spark-mllib:4.1.1`
```

### Celda 2 — SparkSession

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .master("local[*]")
  .appName("LogiTrack-V1")
  .config("spark.ui.enabled", "false")
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
import spark.implicits._
println(s"Spark ${spark.version} listo")
// Salida esperada: Spark 4.1.1 listo
```

---

## 📥 Fase 1 — Dataset de envíos

### Celda 3 — Crear los datos

```scala
val envios = Seq(
  (120.0,  2.5, 1,  8,  4.2),
  (350.0,  8.0, 3, 14,  9.8),
  ( 80.0,  1.2, 0,  6,  2.9),
  (500.0, 15.0, 4,  7, 14.1),
  (210.0,  4.5, 2, 10,  6.3),
  (430.0, 10.0, 3, 22, 13.0),
  ( 60.0,  0.8, 0,  9,  2.1),
  (300.0,  6.5, 2, 16,  8.7),
  (150.0,  3.0, 1, 11,  4.8),
  (620.0, 18.0, 5,  8, 17.2),
  (240.0,  5.0, 2, 13,  7.1),
  ( 90.0,  1.5, 0, 18,  3.2),
  (410.0,  9.0, 3, 20, 11.9),
  (170.0,  3.5, 1, 15,  5.3),
  (540.0, 12.0, 4,  6, 15.4),
  (280.0,  6.0, 2,  9,  8.0),
  (110.0,  2.0, 1,  7,  3.8),
  (390.0,  9.5, 3, 17, 11.2),
  ( 70.0,  1.0, 0, 12,  2.4),
  (460.0, 11.5, 4, 21, 13.7),
  (200.0,  4.0, 1, 10,  5.9),
  (330.0,  7.5, 2, 14,  9.3),
  (130.0,  2.8, 1,  8,  4.4),
  (570.0, 14.0, 5, 16, 16.1),
  (250.0,  5.5, 2, 11,  7.4),
  ( 95.0,  1.8, 0, 19,  3.4),
  (420.0,  9.8, 3,  7, 12.1),
  (180.0,  3.8, 1, 13,  5.6),
  (490.0, 13.0, 4,  9, 14.6),
  (310.0,  6.8, 2, 15,  8.9),
  (140.0,  2.6, 1, 22,  4.9),
  (380.0,  8.5, 3, 18, 10.8),
  ( 75.0,  1.1, 0,  6,  2.6),
  (550.0, 13.5, 4, 11, 15.8),
  (220.0,  4.8, 2,  8,  6.6),
  (450.0, 10.5, 3, 20, 12.8),
  (100.0,  1.9, 0, 14,  3.5),
  (360.0,  8.2, 3, 16, 10.3),
  (190.0,  4.2, 1,  9,  5.8),
  (610.0, 17.5, 5, 13, 17.0),
  (260.0,  5.8, 2, 10,  7.6),
  (115.0,  2.1, 1, 19,  4.0),
  (440.0, 10.2, 3,  7, 12.4),
  (160.0,  3.2, 1, 12,  5.1),
  (530.0, 12.5, 4, 15, 15.1),
  (290.0,  6.2, 2, 17,  8.4),
  ( 85.0,  1.4, 0, 21,  3.0),
  (400.0,  9.2, 3, 11, 11.5),
  (175.0,  3.6, 1, 14,  5.5),
  (480.0, 11.8, 4, 18, 13.9)
).toDF("distancia_km", "peso_kg", "num_paradas", "hora_salida", "tiempo_entrega_horas")

println(s"Registros cargados: ${envios.count()}")
// Salida esperada: Registros cargados: 50
```

---

## 🔍 Fase 2 — Exploración de datos (EDA)

### Tarea 2.1 — Inspección básica

📌 Muestra el esquema, el número de filas y las estadísticas descriptivas con `describe()`.

```scala
// ── Tu código aquí ──
```

### Tarea 2.2 — Verificación de nulos

📌 Un pipeline de MLlib falla si hay nulos. Cuenta los nulos por columna.

```scala
// ── Tu código aquí ──
```

### Tarea 2.3 — Análisis exploratorio

📌 Muestra el tiempo de entrega medio agrupado por `num_paradas`.

```scala
// ── Tu código aquí ──
```

---

## ⚙️ Fase 3 — VectorAssembler

### Tarea 3.1 — Crear el assembler

📌 Construye un `VectorAssembler` con las 4 columnas predictoras → columna `features`.

```scala
import org.apache.spark.ml.feature.VectorAssembler

// ── Tu código aquí ──
```

### Tarea 3.2 — Verificar la transformación

📌 Aplica el assembler y muestra 5 filas con `truncate = false`.

```scala
// ── Tu código aquí ──
```

> 💡 **Reflexión:** ¿En qué orden aparecen los valores en el vector? ¿De qué depende?
> 

---

## 🤖 Fase 4 — Pipeline y entrenamiento

### Tarea 4.1 — Dividir los datos

📌 Divide el DataFrame **original** 80/20 con `seed = 42`. Imprime el tamaño de cada partición.

```scala
// ── Tu código aquí ──
// val Array(trainData, testData) = envios.randomSplit(...)
```

### Tarea 4.2 — Definir el algoritmo

📌 Crea un `LinearRegression` con `featuresCol="features"`, `labelCol="tiempo_entrega_horas"`, `maxIter=100`.

```scala
import org.apache.spark.ml.regression.LinearRegression

// ── Tu código aquí ──
```

### Tarea 4.3 — Construir y entrenar el Pipeline

📌 Crea un `Pipeline` con etapas `[assembler, lr]`, entrénalo sobre `trainData` e imprime los coeficientes.

```scala
import org.apache.spark.ml.Pipeline

// ── Tu código aquí ──
```

> 💡 **Reflexión:** ¿El coeficiente de `hora_salida` es positivo o negativo? ¿Tiene sentido desde el punto de vista logístico?
> 

---

## 📊 Fase 5 — Evaluación

### Tarea 5.1 — Predicciones sobre test

📌 Aplica `model.transform(testData)` y muestra `distancia_km`, `num_paradas`, `tiempo_entrega_horas` y `prediction`.

```scala
// ── Tu código aquí ──
```

### Tarea 5.2 — RMSE y R²

📌 Calcula ambas métricas con `RegressionEvaluator`. ¿Cumple el modelo el criterio de aceptación?

> **Criterio:** RMSE < 1.5 horas y R² > 0.90
> 

```scala
import org.apache.spark.ml.evaluation.RegressionEvaluator

// ── Tu código aquí ──
```

### Tarea 5.3 — Comparativa de modelos

📌 Construye un segundo pipeline usando solo `distancia_km` y `num_paradas`. Compara el RMSE con el modelo completo.

```scala
// ── Tu código aquí ──
```

---

## 💾 Fase 6A — Guardar el modelo (último paso en el notebook)

### Celda — Serializar el modelo a disco

```scala
// Crear la carpeta si no existe
import java.nio.file.{Files, Paths}
Files.createDirectories(Paths.get("C:/Curso-Scala/modelos"))

val rutaModelo = "C:/Curso-Scala/modelos/logitrack_v1"
model.save(rutaModelo)

println(s"✅ Modelo guardado en: $rutaModelo")
// Salida esperada: ✅ Modelo guardado en: C:/Curso-Scala/modelos/logitrack_v1
```

> 📁 Spark crea una carpeta con esta estructura. No la muevas ni la renombres:
> 
> 
> ```
> logitrack_v1/
>   metadata/
>   stages/
>     0_VectorAssembler_.../
>     1_LinearRegressionModel_.../
> ```
> 

### Celda — Exportar datos de test a CSV (para usarlos en la Parte B)

```scala
// Guardamos los datos de test SIN la columna label para simular
// datos nuevos que llegarán al job de producción
testData
  .drop("tiempo_entrega_horas")
  .write
  .mode("overwrite")
  .option("header", "true")
  .csv("C:/Curso-Scala/datos/logitrack_test/")

println("✅ Datos de test exportados a CSV")
```

---

# PARTE B — Despliegue local (VSCode + sbt + spark-submit)

> ⚠️ **Este código NO se ejecuta en el notebook.** Es un proyecto Scala independiente que se compila con `sbt` y se lanza con `spark-submit` desde PowerShell. VSCode actúa como editor del código fuente.
> 

---

## 🛠️ Herramientas del flujo de despliegue

![image.png](image%2010.png)

---

## 📂 Paso 1 — Estructura del proyecto

Crea esta estructura de carpetas en PowerShell:

```powershell
# Crear la estructura del proyecto
New-Item -ItemType Directory -Force -Path "C:\Curso-Scala\logitrack-job\src\main\scala\logitrack"
New-Item -ItemType Directory -Force -Path "C:\Curso-Scala\logitrack-job\project"

Write-Host "✅ Estructura del proyecto creada"
```

La estructura resultante es:

```
C:\Curso-Scala\logitrack-job\
  build.sbt                          ← configuración del proyecto
  project\
    build.properties                 ← versión de sbt
    plugins.sbt                      ← plugin sbt-assembly
  src\main\scala\logitrack\
    LogiTrackPredJob.scala           ← el job de predicción
```

---

## 📄 Paso 2 — Ficheros de configuración

### `project\build.properties`

Crea el fichero en VSCode con este contenido:

```
sbt.version=1.9.9
```

### `project\plugins.sbt`

```scala
// Plugin para generar el fat JAR con todas las dependencias incluidas
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")
```

### `build.sbt`

```scala
name         := "logitrack-job"
version      := "1.0"
scalaVersion := "2.13.18"

// Versión de Spark — debe coincidir con la instalación local
val sparkVersion = "4.1.1"

libraryDependencies ++= Seq(
  // "provided" significa: Spark está en el clúster, no lo incluyas en el JAR
  // Para ejecución local con spark-submit, Spark lo provee el propio script
  "org.apache.spark" %% "spark-sql"   % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
)

// Estrategia de fusión para evitar conflictos de ficheros duplicados en el JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case x                             => MergeStrategy.first
}

// Nombre del JAR generado
assembly / assemblyJarName := "logitrack-assembly-1.0.jar"
```

> ⚠️ **Nota sobre `provided`:** al marcar Spark como `provided`, el JAR resultante no incluye Spark. Esto es correcto para un clúster real donde Spark ya está instalado. Para la ejecución con `spark-submit local[*]` en Windows, `spark-submit` inyecta Spark en el classpath automáticamente — funciona sin cambios.
> 

---

## 📝 Paso 3 — El job de predicción

### `src\main\scala\logitrack\LogiTrackPredJob.scala`

Abre VSCode en la carpeta `C:\Curso-Scala\logitrack-job\` y crea el fichero:

```scala
// ============================================================
// LogiTrack S.A. — Job de predicción de tiempo de entrega
// Versión: 1.0  |  Spark 4.1.1  |  Scala 2.13.18
//
// Compilar:   sbt assembly
// Ejecutar local (desarrollo/pruebas en Windows):
//   spark-submit --master local[*] \
//     --class logitrack.LogiTrackPredJob \
//     target\scala-2.13\logitrack-assembly-1.0.jar \
//     C:\Curso-Scala\modelos\logitrack_v1 \
//     C:\Curso-Scala\datos\logitrack_test\ \
//     C:\Curso-Scala\datos\logitrack_predicciones\
//
// ⚠️  En producción real, reemplazar --master local[*] por:
//     --master yarn --deploy-mode cluster
//     y las rutas locales por rutas HDFS o S3
// ============================================================

package logitrack

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._

object LogiTrackPredJob {

  def main(args: Array[String]): Unit = {

    // ── 1. Validar argumentos ────────────────────────────────
    if (args.length != 3) {
      println("""
        |Uso: spark-submit ... logitrack-assembly-1.0.jar \
        |  <ruta-modelo> <ruta-entrada> <ruta-salida>
        |
        |Ejemplo local:
        |  spark-submit --master local[*] --class logitrack.LogiTrackPredJob \
        |    target\scala-2.13\logitrack-assembly-1.0.jar \
        |    C:\Curso-Scala\modelos\logitrack_v1 \
        |    C:\Curso-Scala\datos\logitrack_test\ \
        |    C:\Curso-Scala\datos\logitrack_predicciones\
        |
        |En producción (clúster YARN):
        |  spark-submit --master yarn --deploy-mode cluster \
        |    --num-executors 4 --executor-memory 4g \
        |    --class logitrack.LogiTrackPredJob \
        |    logitrack-assembly-1.0.jar \
        |    hdfs:///modelos/logitrack_v1 \
        |    hdfs:///datos/envios_nuevos/ \
        |    hdfs:///resultados/predicciones/
        |""".stripMargin)
      System.exit(1)
    }

    val rutaModelo  = args(0)
    val rutaEntrada = args(1)
    val rutaSalida  = args(2)

    // ── 2. SparkSession ──────────────────────────────────────
    // No se especifica .master() aquí: lo decide spark-submit.
    // En local → spark-submit --master local[*]
    // En producción → spark-submit --master yarn
    val spark = SparkSession.builder()
      .appName("LogiTrack-Prediccion-v1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    println(s"[INFO] Spark ${spark.version} iniciado")
    println(s"[INFO] Modo: ${spark.sparkContext.master}")

    // ── 3. Cargar el modelo serializado ─────────────────────
    // El pipeline cargado contiene VectorAssembler + LinearRegressionModel.
    // No hace falta reconstruir las etapas manualmente.
    val modelo = PipelineModel.load(rutaModelo)
    println(s"[INFO] Modelo cargado desde: $rutaModelo")
    println(s"[INFO] Etapas del pipeline: ${modelo.stages.length}")

    // ── 4. Leer datos nuevos ─────────────────────────────────
    // El CSV de entrada tiene el mismo esquema que los datos de entrenamiento
    // pero SIN la columna tiempo_entrega_horas (es lo que predecimos).
    val datosNuevos = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(rutaEntrada)

    println(s"[INFO] Registros a predecir: ${datosNuevos.count()}")
    datosNuevos.printSchema()

    // ── 5. Generar predicciones ──────────────────────────────
    // El pipeline ejecuta en orden:
    //   VectorAssembler → crea columna "features"
    //   LinearRegressionModel → crea columna "prediction"
    val predicciones = modelo.transform(datosNuevos)

    // ── 6. Seleccionar columnas de salida ────────────────────
    val resultado = predicciones.select(
      col("distancia_km"),
      col("peso_kg"),
      col("num_paradas"),
      col("hora_salida"),
      round(col("prediction"), 2).alias("tiempo_estimado_horas"),
      current_timestamp().alias("timestamp_prediccion")
    )

    // ── 7. Mostrar muestra (útil en modo local para verificar) ──
    println("[INFO] Muestra de predicciones:")
    resultado.show(5, truncate = false)

    // ── 8. Persistir resultados en CSV ──────────────────────
    // En producción real se usaría Parquet o Delta Lake.
    // CSV aquí para que sea fácilmente verificable en Windows.
    resultado
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(rutaSalida)

    println(s"[INFO] Predicciones escritas en: $rutaSalida")

    // ── 9. Cerrar sesión ────────────────────────────────────
    spark.stop()
    println("[INFO] Job completado correctamente")
  }
}
```

---

## 🏗️ Paso 4 — Instalar la extensión Metals en VSCode (Si no lo tuvieras)

Metals es el servidor de lenguaje oficial para Scala en VSCode. Proporciona autocompletado, detección de errores en tiempo real y navegación de código.

1. Abre VSCode
2. Abre la carpeta `C:\Curso-Scala\logitrack-job\` con `File → Open Folder`
3. Ve a Extensions (`Ctrl + Shift + X`)
4. Busca **Scala (Metals)** → Install
5. Cuando Metals detecte el `build.sbt`, aparecerá una notificación: **"Import build"** → haz clic en **Import**
6. Metals descargará las dependencias y compilará el proyecto (1-3 minutos la primera vez)

> ✅ Cuando Metals esté listo, el archivo `.scala` mostrará resaltado de sintaxis completo y marcará errores en tiempo real.
> 

---

## ⚙️ Paso 5 — Compilar con sbt assembly

Abre el terminal integrado de VSCode (`Ctrl + ```  ``) — se abrirá en PowerShell dentro de la carpeta del proyecto.

```powershell
# Desde C:\Curso-Scala\logitrack-job\

# Limpiar compilaciones anteriores
sbt clean

# Generar el fat JAR con todas las dependencias
sbt assembly
```

Salida esperada al final:

```
[info] Assembly up to date: C:\Curso-Scala\logitrack-job\target\scala-2.13\logitrack-assembly-1.0.jar
[success] Total time: 45 s
```

> ⏱️ La primera compilación tarda 1-3 minutos porque sbt descarga las dependencias. Las siguientes son mucho más rápidas.
> 

---

## 🚀 Paso 6 — Ejecutar con spark-submit (modo local)

Desde el terminal integrado de VSCode (PowerShell):

```powershell
# Verificar que el JAR se creó
Test-Path "C:\Curso-Scala\logitrack-job\target\scala-2.13\logitrack-assembly-1.0.jar"
# Debe imprimir: True

# Lanzar el job en modo local[*] — usa todos los cores del portátil
spark-submit `
  --master local[*] `
  --class logitrack.LogiTrackPredJob `
  "C:\Curso-Scala\logitrack-job\target\scala-2.13\logitrack-assembly-1.0.jar" `
  "C:\Curso-Scala\modelos\logitrack_v1" `
  "C:\Curso-Scala\datos\logitrack_test\" `
  "C:\Curso-Scala\datos\logitrack_predicciones\"
```

Salida esperada:

```
[INFO] Spark 4.1.1 iniciado
[INFO] Modo: local[*]
[INFO] Modelo cargado desde: C:\Curso-Scala\modelos\logitrack_v1
[INFO] Etapas del pipeline: 2
[INFO] Registros a predecir: 10
[INFO] Muestra de predicciones:
+------------+-------+-----------+-----------+---------------------+---------------------+
|distancia_km|peso_kg|num_paradas|hora_salida|tiempo_estimado_horas|timestamp_prediccion |
+------------+-------+-----------+-----------+---------------------+---------------------+
|350.0       |8.0    |3          |14         |9.76                 |2026-05-06 ...       |
|...         |...    |...        |...        |...                  |...                  |
+------------+-------+-----------+-----------+---------------------+---------------------+
[INFO] Predicciones escritas en: C:\Curso-Scala\datos\logitrack_predicciones\
[INFO] Job completado correctamente
```

### Verificar el resultado

```powershell
# Ver los ficheros generados
Get-ChildItem "C:\Curso-Scala\datos\logitrack_predicciones\"

# Ver el contenido del CSV de predicciones
Get-Content (Get-ChildItem "C:\Curso-Scala\datos\logitrack_predicciones\*.csv" | Select-Object -First 1)
```

---

## 🏭 Paso 7 — Referencia: cómo sería en producción real

> Esta sección es **solo referencia** — no se ejecuta.
> 

En un clúster real (YARN sobre Hadoop), el único cambio es el flag `--master` y las rutas:

```bash
# En un servidor Linux con acceso al clúster YARN
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 4g \
  --executor-cores 2 \
  --class logitrack.LogiTrackPredJob \
  logitrack-assembly-1.0.jar \
  hdfs:///modelos/logitrack_v1 \
  hdfs:///datos/envios_nuevos/fecha=2026-05-06/ \
  hdfs:///resultados/predicciones/fecha=2026-05-06/
```

> El job de producción típicamente lo lanza un **orquestador** (Apache Airflow, Azure Data Factory) según un calendario o evento, sin intervención manual.
> 

---

---

# ══════════════════════════════════════════════

# VÍA 2 — Azure Databricks - Lo desarrollamos la otra semana

# Runtime 16.4 LTS · Scala 2.13 · Spark 3.5.2

# ══════════════════════════════════════════════

---

# PARTE A — Configuración de Azure Databricks

---

## ☁️ ¿Por qué Runtime 16.4 LTS con Scala 2.13?

Azure Databricks ofrece múltiples versiones de runtime. Para este curso usamos **16.4 LTS** por estas razones:

| Criterio | Runtime 16.4 LTS (Scala 2.13) | Runtime 17.x (Spark 4.0) |
| --- | --- | --- |
| Spark | 3.5.2 | 4.0.0 |
| Scala | 2.13 ✅ | 2.13 ✅ |
| Estado | **LTS — soporte largo plazo** | No-LTS, en evolución |
| Estabilidad | Alta | Media (versión nueva) |
| Recomendado para curso | ✅ **Sí** | No |

> El código MLlib que escribirás es **idéntico** al de la Vía 1. La diferencia de versión de Spark (3.5.2 vs 4.1.1) no afecta nada de lo que se trabaja en este nivel.
> 

---

## 🔧 Paso 1 — Crear el workspace de Azure Databricks

### Desde el portal de Azure (portal.azure.com)

1. Inicia sesión en **portal.azure.com** con tu cuenta de Azure
2. Haz clic en **"Crear un recurso"** (+ Create a resource)
3. Busca **"Azure Databricks"** → selecciona el resultado oficial de Microsoft
4. Haz clic en **"Crear"**

### Configuración del workspace

Rellena el formulario con estos valores:

| Campo | Valor recomendado |
| --- | --- |
| Suscripción | Tu suscripción de Azure |
| Grupo de recursos | Crear nuevo: `rg-logitrack-curso` |
| Nombre del workspace | `dbx-logitrack-curso` |
| Región | **West Europe** (menor latencia desde España) |
| Plan de tarifa | **Premium** (necesario para Unity Catalog) |
1. Haz clic en **"Revisar y crear"** → **"Crear"**
2. Espera 2-3 minutos a que se despliegue el workspace
3. Haz clic en **"Ir al recurso"** → **"Iniciar workspace"**

> ✅ Se abrirá la interfaz web de Azure Databricks en una nueva pestaña.
> 

---

## 🖥️ Paso 2 — Crear el clúster de cómputo

En la barra lateral izquierda de Databricks:

1. Haz clic en **"Compute"** (icono de chip)
2. Haz clic en **"Create compute"**

### Configuración del clúster

| Campo | Valor |
| --- | --- |
| Cluster name | `cluster-logitrack-curso` |
| Policy | Unrestricted |
| Cluster mode | Single node |
| **Databricks runtime version** | **Runtime: 16.4 LTS (Scala 2.13, Spark 3.5.2)** |
| Node type | Standard_DS3_v2 (4 cores, 14 GB RAM) |
| Terminate after | 30 minutes of inactivity |

> ⚠️ **Importante:** En el selector de runtime, busca exactamente **"16.4 LTS (Scala 2.13, Spark 3.5.2)"**. Hay dos variantes de 16.4 LTS (una con Scala 2.12 y otra con Scala 2.13) — selecciona la de **Scala 2.13**.
> 
1. Haz clic en **"Create compute"**
2. Espera 3-5 minutos a que el clúster arranque (el indicador pasará de gris a verde ✅)

---

## 📁 Paso 3 — Crear un Unity Catalog Volume y subir los datos

Unity Catalog Volume es el almacenamiento recomendado por Databricks para ficheros en workspaces modernos (DBFS está deprecado).

### 3.1 Acceder a Catalog Explorer

1. En la barra lateral, haz clic en **"Catalog"** (icono de base de datos)
2. Verás el catálogo `main` por defecto

### 3.2 Crear un schema

1. Haz clic en **"main"** → **"Create schema"**
2. Nombre: `logitrack`
3. Haz clic en **"Create"**

### 3.3 Crear un Volume

1. Haz clic en el schema `logitrack` → **"Create volume"**
2. Nombre del volume: `datos`
3. Tipo: **Managed**
4. Haz clic en **"Create"**

### 3.4 Preparar el CSV de datos en Windows

Antes de subir, crea el fichero CSV con los datos de entrenamiento en tu máquina Windows.

Abre PowerShell y ejecuta:

```powershell
# Crear carpeta local si no existe
New-Item -ItemType Directory -Force -Path "C:\Curso-Scala\datos\logitrack_csv"

# Crear el fichero CSV con los 50 registros de entrenamiento
$contenido = @"
distancia_km,peso_kg,num_paradas,hora_salida,tiempo_entrega_horas
120.0,2.5,1,8,4.2
350.0,8.0,3,14,9.8
80.0,1.2,0,6,2.9
500.0,15.0,4,7,14.1
210.0,4.5,2,10,6.3
430.0,10.0,3,22,13.0
60.0,0.8,0,9,2.1
300.0,6.5,2,16,8.7
150.0,3.0,1,11,4.8
620.0,18.0,5,8,17.2
240.0,5.0,2,13,7.1
90.0,1.5,0,18,3.2
410.0,9.0,3,20,11.9
170.0,3.5,1,15,5.3
540.0,12.0,4,6,15.4
280.0,6.0,2,9,8.0
110.0,2.0,1,7,3.8
390.0,9.5,3,17,11.2
70.0,1.0,0,12,2.4
460.0,11.5,4,21,13.7
200.0,4.0,1,10,5.9
330.0,7.5,2,14,9.3
130.0,2.8,1,8,4.4
570.0,14.0,5,16,16.1
250.0,5.5,2,11,7.4
95.0,1.8,0,19,3.4
420.0,9.8,3,7,12.1
180.0,3.8,1,13,5.6
490.0,13.0,4,9,14.6
310.0,6.8,2,15,8.9
140.0,2.6,1,22,4.9
380.0,8.5,3,18,10.8
75.0,1.1,0,6,2.6
550.0,13.5,4,11,15.8
220.0,4.8,2,8,6.6
450.0,10.5,3,20,12.8
100.0,1.9,0,14,3.5
360.0,8.2,3,16,10.3
190.0,4.2,1,9,5.8
610.0,17.5,5,13,17.0
260.0,5.8,2,10,7.6
115.0,2.1,1,19,4.0
440.0,10.2,3,7,12.4
160.0,3.2,1,12,5.1
530.0,12.5,4,15,15.1
290.0,6.2,2,17,8.4
85.0,1.4,0,21,3.0
400.0,9.2,3,11,11.5
175.0,3.6,1,14,5.5
480.0,11.8,4,18,13.9
"@

$contenido | Out-File -FilePath "C:\Curso-Scala\datos\logitrack_csv\envios_train.csv" -Encoding UTF8
Write-Host "✅ CSV creado: C:\Curso-Scala\datos\logitrack_csv\envios_train.csv"
```

### 3.5 Subir el CSV al Volume desde la UI de Databricks

1. En **Catalog Explorer**, navega a `main` → `logitrack` → `datos`
2. Haz clic en la pestaña **"Files"**
3. Haz clic en el botón **"Upload to this volume"**
4. Arrastra el fichero `envios_train.csv` desde `C:\Curso-Scala\datos\logitrack_csv\` o haz clic para buscarlo
5. Haz clic en **"Upload"**

> ✅ El fichero quedará disponible en la ruta: `/Volumes/main/logitrack/datos/envios_train.csv`
> 

---

## 📓 Paso 4 — Crear el notebook Scala en Databricks

1. En la barra lateral, haz clic en **"Workspace"**
2. Haz clic en tu carpeta personal (tu email)
3. Haz clic en el botón **"+"** → **"Notebook"**
4. Configura:
    - **Name:** `dia23_s1_logitrack_via2`
    - **Default language:** Scala
    - **Cluster:** `cluster-logitrack-curso`
5. Haz clic en **"Create"**

---

# PARTE B — Notebook de desarrollo en Databricks

> En Databricks el notebook funciona igual que en Jupyter pero con diferencias importantes:
> 
> - **No existe `import $ivy`** — las librerías de Spark y MLlib ya están en el runtime
> - **`spark` ya existe** como variable implícita — no hay que crearlo
> - **No se usa `.master()`** — el clúster ya está configurado
> - Las **rutas** apuntan a Unity Catalog Volumes (`/Volumes/...`) no a `C:\`

---

## 🔧 Celda 1 — Verificar el entorno

```scala
// En Databricks no se importa $ivy ni se crea SparkSession
// spark ya está disponible como variable global del notebook

println(s"Spark version: ${spark.version}")
println(s"Scala version: ${scala.util.Properties.versionString}")
// Salida esperada:
// Spark version: 3.5.2
// Scala version: version 2.13.x
```

---

## 📥 Fase 1 — Cargar los datos desde Unity Catalog Volume

### Celda 2

```scala
import spark.implicits._
import org.apache.spark.sql.functions._

// Los datos están en el Volume que creamos y donde subimos el CSV
val rutaDatos = "/Volumes/main/logitrack/datos/envios_train.csv"

val envios = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(rutaDatos)

println(s"Registros cargados: ${envios.count()}")
envios.printSchema()
// Salida esperada: Registros cargados: 50
```

---

## 🔍 Fase 2 — Exploración de datos (EDA)

### Tarea 2.1 — Inspección básica

📌 Muestra el esquema, número de filas y estadísticas descriptivas.

```scala
// ── Tu código aquí ──
```

### Tarea 2.2 — Verificación de nulos

📌 Cuenta los nulos por columna antes de entrenar.

```scala
// ── Tu código aquí ──
```

### Tarea 2.3 — Análisis exploratorio

📌 Muestra el tiempo de entrega medio por `num_paradas`.

```scala
// ── Tu código aquí ──
```

---

## ⚙️ Fase 3 — VectorAssembler

### Tarea 3.1 — Crear el assembler

```scala
import org.apache.spark.ml.feature.VectorAssembler

// ── Tu código aquí ──
```

### Tarea 3.2 — Verificar la transformación

```scala
// ── Tu código aquí ──
```

---

## 🤖 Fase 4 — Pipeline y entrenamiento

### Tarea 4.1 — Dividir datos

```scala
// ── Tu código aquí ──
```

### Tarea 4.2 — Definir el algoritmo

```scala
import org.apache.spark.ml.regression.LinearRegression

// ── Tu código aquí ──
```

### Tarea 4.3 — Pipeline y entrenamiento

```scala
import org.apache.spark.ml.Pipeline

// ── Tu código aquí ──
```

---

## 📊 Fase 5 — Evaluación

### Tarea 5.1 — Predicciones sobre test

```scala
// ── Tu código aquí ──
```

### Tarea 5.2 — RMSE y R²

```scala
import org.apache.spark.ml.evaluation.RegressionEvaluator

// ── Tu código aquí ──
```

### Tarea 5.3 — Comparativa de modelos

```scala
// ── Tu código aquí ──
```

---

## 💾 Fase 6A — Guardar el modelo en Unity Catalog Volume

### Celda — Serializar el modelo

```scala
// En Databricks la ruta es un Volume de Unity Catalog, no una ruta local de Windows
val rutaModelo = "/Volumes/main/logitrack/datos/logitrack_model_v1"

model.save(rutaModelo)

println(s"✅ Modelo guardado en: $rutaModelo")
// Salida esperada: ✅ Modelo guardado en: /Volumes/main/logitrack/datos/logitrack_model_v1
```

> 📁 Spark crea la misma estructura de carpetas que en local:
> 
> 
> ```
> /Volumes/main/logitrack/datos/logitrack_model_v1/
>   metadata/
>   stages/
>     0_VectorAssembler_.../
>     1_LinearRegressionModel_.../
> ```
> 

---

# PARTE C — Despliegue como Databricks Job

> En Databricks la fase de producción NO requiere compilar un JAR ni usar `spark-submit`. El job se ejecuta directamente desde un notebook o un script Python/Scala configurado en la interfaz de Jobs.
> 

---

## 🚀 Paso 1 — Crear el notebook de predicción

Crea un nuevo notebook en tu workspace:

- **Name:** `logitrack_prediccion_job`
- **Default language:** Scala
- **Cluster:** `cluster-logitrack-curso`

### Celda 1 — Datos nuevos de envíos a predecir

```scala
import spark.implicits._
import org.apache.spark.sql.functions._

// En producción real estos datos vendrían de un fichero diario en el Volume
// o de una tabla Delta. Aquí los creamos en memoria para el ejemplo.
val nuevosEnvios = Seq(
  (320.0, 7.0,  2, 10),
  ( 95.0, 1.6,  0, 14),
  (480.0,12.0,  4,  8),
  (200.0, 4.3,  1, 20),
  (560.0,16.0,  5,  6),
  (145.0, 3.1,  1, 11),
  (410.0, 9.4,  3, 17),
  ( 72.0, 1.0,  0,  9),
  (275.0, 6.0,  2, 15),
  (520.0,13.2,  4, 13)
).toDF("distancia_km", "peso_kg", "num_paradas", "hora_salida")

println(s"Envíos a predecir: ${nuevosEnvios.count()}")
nuevosEnvios.show()
```

### Celda 2 — Cargar el modelo y generar predicciones

```scala
import org.apache.spark.ml.PipelineModel

// Cargar el pipeline completo desde el Volume
val rutaModelo = "/Volumes/main/logitrack/datos/logitrack_model_v1"
val modeloCargado = PipelineModel.load(rutaModelo)

println(s"✅ Modelo cargado. Etapas: ${modeloCargado.stages.length}")

// Generar predicciones — el pipeline aplica VectorAssembler + modelo automáticamente
val predicciones = modeloCargado.transform(nuevosEnvios)

// Seleccionar columnas relevantes para el negocio
val resultado = predicciones.select(
  col("distancia_km"),
  col("peso_kg"),
  col("num_paradas"),
  col("hora_salida"),
  round(col("prediction"), 2).alias("tiempo_estimado_horas"),
  current_timestamp().alias("timestamp_prediccion")
)

println("=== Predicciones de tiempo de entrega ===")
resultado.show(truncate = false)
```

**Salida esperada (valores aproximados):**

```
=== Predicciones de tiempo de entrega ===
+------------+-------+-----------+-----------+---------------------+---------------------+
|distancia_km|peso_kg|num_paradas|hora_salida|tiempo_estimado_horas|timestamp_prediccion |
+------------+-------+-----------+-----------+---------------------+---------------------+
|320.0       |7.0    |2          |10         |9.14                 |2026-05-06 ...       |
|95.0        |1.6    |0          |14         |3.31                 |2026-05-06 ...       |
|480.0       |12.0   |4          |8          |13.68                |2026-05-06 ...       |
|200.0       |4.3    |1          |20         |5.94                 |2026-05-06 ...       |
|560.0       |16.0   |5          |6          |15.87                |2026-05-06 ...       |
+------------+-------+-----------+-----------+---------------------+---------------------+
```

### Celda 3 — Escribir resultados en Delta Lake

```scala
// Delta Lake es el formato de almacenamiento estándar en Databricks
// Soporta ACID transactions, versionado y es más eficiente que Parquet puro
val rutaSalida = "/Volumes/main/logitrack/datos/predicciones_delta"

resultado
  .write
  .mode("overwrite")
  .format("delta")        // formato Delta Lake — estándar en Databricks
  .save(rutaSalida)

println(s"✅ Predicciones escritas en Delta Lake: $rutaSalida")

// Verificar
val verificacion = spark.read.format("delta").load(rutaSalida)
println(s"Filas escritas: ${verificacion.count()}")
verificacion.show()
```

---

## 📅 Paso 2 — Configurar el Job programado

Un Databricks Job ejecuta el notebook automáticamente según un calendario, sin que el usuario tenga que pulsar "Run".

### Crear el Job desde la UI

1. En la barra lateral, haz clic en **"Workflows"** (icono de engranaje)
2. Haz clic en **"Create job"**
3. Configura:

| Campo | Valor |
| --- | --- |
| Job name | `logitrack-prediccion-diaria` |
| Task name | `predecir-envios` |
| Type | Notebook |
| Source | Workspace |
| Path | Selecciona tu notebook `logitrack_prediccion_job` |
| Cluster | `cluster-logitrack-curso` |
1. En la sección **"Schedule"**, haz clic en **"Add trigger"**:
    - Trigger type: **Scheduled**
    - Every: **1 day** at **02:00**
    - Timezone: **Europe/Madrid**
2. Haz clic en **"Create job"**

### Ejecutar el job manualmente para verificar

1. Desde la página del job, haz clic en **"Run now"**
2. El job ejecutará el notebook completo automáticamente
3. Verás el resultado en la pestaña **"Runs"**

> ✅ Una vez configurado, el job se ejecutará automáticamente cada noche a las 02:00 sin intervención manual. En producción real, este job procesaría los envíos del día anterior y escribiría las predicciones en una tabla Delta que consumiría el sistema de atención al cliente.
> 

---

## 🔍 Paso 3 — Verificar los resultados en Catalog Explorer

1. Ve a **Catalog** → `main` → `logitrack` → `datos`
2. Verás la tabla Delta `predicciones_delta` creada automáticamente por el job
3. Haz clic sobre ella para ver el esquema y una vista previa de los datos

---

---