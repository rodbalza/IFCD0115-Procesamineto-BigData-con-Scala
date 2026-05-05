# 💻Clase 21 - Lectura y escritura de Ficheros

---

# Agenda:

<aside>
💡

#### 9:00 - 9:50    → Ficheros Parquet , AVRO y ORC

#### 9:50 - 11:20   →  Ejercicios

#### **11:20 - 11:40  →  Descanso**

#### 11:40 - 12:40  → Caso de uso 1

#### 12:40 - 14:00  → Caso de uso 2

</aside>

---

# **Lectura y escritura de datos en Spark**

---

# **1. Celda inicial obligatoria**

Antes de ejecutar cualquier ejemplo de esta sesión, ejecuta esta celda al principio del notebook.

```scala
import $ivy.`org.apache.spark::spark-core:4.1.1`
import $ivy.`org.apache.spark::spark-sql:4.1.1`
import $ivy.`org.apache.spark::spark-avro:4.1.1`

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import java.io.File

val spark = SparkSession.builder()
  .appName("c21_Formatos_Spark")
  .master("local[*]")
  .config("spark.ui.showConsoleProgress", "false")
  .getOrCreate()

import spark.implicits._

spark.sparkContext.setLogLevel("ERROR")

println(s"✅ Spark${spark.version} listo para lectura y escritura de formatos")
```

**Salida esperada:**

```scala
✅ Spark 4.1.1 listo para lectura y escritura de formatos
```

> ⚠️ Para trabajar con **Avro** se añade la dependencia `spark-avro`. Parquet y ORC se pueden usar directamente desde `spark-sql`.
> 

---

# **2. Preparar carpeta y datos de entrada**

Para que todos los ejemplos funcionen en el notebook, primero crearemos un fichero CSV desde Scala. El CSV será solo el **punto de partida** del pipeline.

> ⚠️ Ajusta la ruta si tu carpeta de trabajo es diferente.
> 

```scala
val rutaBase = "C:/Curso-Scala/datos/c21"
Files.createDirectories(Paths.get(rutaBase))

println(s"Carpeta creada o existente:$rutaBase")
```

**Salida esperada:**

```scala
Carpeta creada o existente: C:/Curso-Scala/datos/dia17_s2
```

---

## **2.1 Crear un CSV de ventas**

```scala
val contenidoVentasCSV =
"""id_venta,fecha,anio,mes,cliente,ciudad,categoria,producto,cantidad,precio_unitario
1,2026-01-05,2026,1,Ana,Madrid,Informatica,Portatil,1,850.50
2,2026-01-07,2026,1,Luis,Valencia,Informatica,Raton,3,18.90
3,2026-02-11,2026,2,Marta,Sevilla,Oficina,Silla,2,120.00
4,2026-02-13,2026,2,Carlos,Madrid,Informatica,Monitor,1,199.99
5,2026-03-02,2026,3,Ana,Barcelona,Oficina,Mesa,1,250.00
6,2026-03-15,2026,3,Lucia,Zaragoza,Informatica,Webcam,4,39.90
7,2026-04-01,2026,4,Pedro,Madrid,Informatica,Teclado,2,45.00
8,2026-04-03,2026,4,Sofia,Valencia,Audio,Auriculares,2,59.99
9,2025-12-20,2025,12,Ana,Madrid,Informatica,Portatil,1,799.00
10,2025-12-22,2025,12,Luis,Sevilla,Oficina,Silla,4,110.00
"""

val rutaVentasCSV = s"$rutaBase/ventas.csv"

Files.write(
  Paths.get(rutaVentasCSV),
  contenidoVentasCSV.getBytes(StandardCharsets.UTF_8)
)

println(s"CSV creado en:$rutaVentasCSV")
```

**Salida esperada:**

```scala
CSV creado en: C:/Curso-Scala/datos/dia17_s2/ventas.csv
```

---

## **2.2 Cargar el CSV base con schema manual**

Como CSV ya se explicó en sesiones anteriores, aquí solo lo usamos como fuente inicial.

```scala
val schemaVentas = StructType(List(
  StructField("id_venta",        IntegerType, nullable = false),
  StructField("fecha",           DateType,    nullable = true),
  StructField("anio",            IntegerType, nullable = true),
  StructField("mes",             IntegerType, nullable = true),
  StructField("cliente",         StringType,  nullable = true),
  StructField("ciudad",          StringType,  nullable = true),
  StructField("categoria",       StringType,  nullable = true),
  StructField("producto",        StringType,  nullable = true),
  StructField("cantidad",        IntegerType, nullable = true),
  StructField("precio_unitario", DoubleType,  nullable = true)
))

val dfVentas = spark.read
  .option("header", "true")
  .schema(schemaVentas)
  .csv(rutaVentasCSV)

println("=== Ventas cargadas desde CSV base ===")
dfVentas.show(false)

dfVentas.printSchema()
```

**Salida esperada parcial:**

```scala
=== Ventas cargadas desde CSV base ===
+--------+----------+----+---+-------+---------+-----------+------------+--------+---------------+
|id_venta|fecha     |anio|mes|cliente|ciudad   |categoria  |producto    |cantidad|precio_unitario|
+--------+----------+----+---+-------+---------+-----------+------------+--------+---------------+
|1       |2026-01-05|2026|1  |Ana    |Madrid   |Informatica|Portatil    |1       |850.5          |
|2       |2026-01-07|2026|1  |Luis   |Valencia |Informatica|Raton       |3       |18.9           |
...
+--------+----------+----+---+-------+---------+-----------+------------+--------+---------------+
```

> ⚠️ En algunos lectores de CSV, Spark puede mostrar `nullable = true` aunque en el `StructField` hayamos escrito `nullable = false`. Esto se debe a que el CSV es texto externo y Spark no siempre puede garantizar esa restricción durante la lectura. El punto importante aquí es que el **tipo de dato** sí queda controlado.
> 

---

# **3. Formatos de datos soportados por Spark**

Spark permite leer y escribir muchos formatos. En Big Data, no todos cumplen la misma función.

| **Formato** | **Tipo** | **Lectura** | **Escritura** | **Uso habitual** |
| --- | --- | --- | --- | --- |
| CSV | Texto por filas | ✅ | ✅ | Intercambio simple, Excel, cargas manuales |
| JSON | Texto semiestructurado | ✅ | ✅ | APIs, logs, eventos simples |
| Parquet | Binario columnar | ✅ | ✅ | Data Lakes, analítica, consultas por columnas |
| ORC | Binario columnar | ✅ | ✅ | Hive, Hadoop, entornos analíticos optimizados |
| Avro | Binario por filas | ✅ | ✅ | Eventos, Kafka, serialización, intercambio con schema |
- **CSV y JSON** son formatos cómodos para intercambio, pero no suelen ser los más eficientes para analítica a gran escala.
- **Parquet y ORC** son formatos columnares. Suelen ser mejores para consultas analíticas porque permiten leer solo las columnas necesarias.
- **Avro** es binario por filas. Es muy usado cuando se necesita transportar registros completos con schema, por ejemplo en pipelines de eventos.

---

# **4. Preparar un DataFrame enriquecido para escribir**

Antes de guardar en distintos formatos, creamos una columna calculada `importe_total`.

```scala
val dfVentasPreparadas = dfVentas
  .withColumn("importe_total", col("cantidad") * col("precio_unitario"))

println("=== Ventas preparadas para escritura ===")
dfVentasPreparadas.show(false)
```

**Salida esperada parcial:**

```scala
=== Ventas preparadas para escritura ===
+--------+----------+----+---+-------+---------+-----------+------------+--------+---------------+-------------+
|id_venta|fecha     |anio|mes|cliente|ciudad   |categoria  |producto    |cantidad|precio_unitario|importe_total|
+--------+----------+----+---+-------+---------+-----------+------------+--------+---------------+-------------+
|1       |2026-01-05|2026|1  |Ana    |Madrid   |Informatica|Portatil    |1       |850.5          |850.5        |
|2       |2026-01-07|2026|1  |Luis   |Valencia |Informatica|Raton       |3       |18.9           |56.7         |
...
+--------+----------+----+---+-------+---------+-----------+------------+--------+---------------+-------------+
```

---

# **5. Parquet**

## **5.1 ¿Qué es Parquet?**

**Parquet** es un formato binario columnar. Es uno de los formatos más usados en Data Lakes y Lakehouses.  La idea clave es que Parquet guarda los datos **por columnas**, no solamente por filas.

<aside>

**Almacenamiento por filas (CSV, JSON):**
Los datos se guardan fila por fila, tal como los lees:

```scala
[1, Laptop, Tecnología, 1200, 2] [2, Teclado, Tecnología, 45, 5] [3, Silla, Oficina, 120, 4]
```

**Almacenamiento columnar (Parquet):** Los datos se guardan columna por columna:

```scala
ids:       [1, 2, 3]
productos: [Laptop, Teclado, Silla]
categorias:[Tecnología, Tecnología, Oficina]
precios:   [1200, 45, 120]        ← solo esto se lee para sum(precio)
cantidades:[2, 5, 4]
```

Para calcular la suma de precios, Spark **solo lee la columna `precio`**. El resto ni se toca.

### 3.2 Ventajas de Parquet

- **Lectura selectiva:** Solo se leen las columnas necesarias para la consulta.
- **Compresión superior:** Datos del mismo tipo se comprimen juntos (ej: enteros con enteros). Un CSV de 100 MB suele convertirse en un Parquet de 10–20 MB.
- **Schema embebido:** El fichero lleva su propio esquema de tipos. No hace falta `inferSchema` ni definirlo manualmente.
- **Estadísticas por bloque:** Parquet almacena mínimos y máximos por fragmento. Spark puede saltar bloques enteros que no cumplan un filtro (`WHERE precio > 500`).
- **Particionado físico:** Se puede escribir particionando los datos en carpetas (`partitionBy`), lo que acelera enormemente las consultas con filtros por esa columna.
</aside>

> Si una consulta solo necesita `ciudad` e `importe_total`, Spark puede evitar leer muchas columnas innecesarias.
> 

---

## **5.2 Escribir en Parquet**

```scala
val rutaParquet = s"$rutaBase/salida/ventas_parquet"

dfVentasPreparadas.write
  .mode("overwrite")
  .parquet(rutaParquet)

println(s"Datos escritos en Parquet:$rutaParquet")
```

**Salida esperada:**

```scala
Datos escritos en Parquet: C:/Curso-Scala/datos/dia17_s2/salida/ventas_parquet
```

---

## **5.3 Leer Parquet**

```scala
val dfVentasParquet = spark.read.parquet(rutaParquet)

println("=== Datos leídos desde Parquet ===")
dfVentasParquet.show(5, truncate = false)

dfVentasParquet.printSchema()
```

**Salida esperada parcial:**

```scala
=== Datos leídos desde Parquet ===
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
|id_venta|fecha     |anio|mes|cliente|ciudad   |categoria  |producto|cantidad|precio_unitario|importe_total|
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
|1       |2026-01-05|2026|1  |Ana    |Madrid   |Informatica|Portatil|1       |850.5          |850.5        |
...
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
```

> 💡 Parquet guarda el schema dentro del propio formato. Por eso, al leerlo, no necesitamos indicar `header`, `inferSchema` ni `schema`.
> 

---

# **6. ORC**

## **6.1 ¿Qué es ORC?**

**ORC** significa **Optimized Row Columnar**. Aunque su nombre contiene la palabra `Row`, en la práctica es un formato **columnar** optimizado para analítica. Es muy habitual en ecosistemas basados en **Hive**, **Hadoop** y almacenes analíticos que necesitan compresión y lectura eficiente.

| **Característica** | **ORC** |
| --- | --- |
| Tipo | Binario columnar |
| Guarda schema | Sí |
| Compresión | Sí |
| Uso típico | Hive, Hadoop, Data Warehouse sobre Data Lake |
| Bueno para | Consultas analíticas y lectura de columnas concretas |

<aside>

ORC es **columnar** igual que Parquet, pero con algunas diferencias internas.

**Almacenamiento ORC:** Los datos se guardan columna por columna, organizados en **stripes** (franjas horizontales):

```scala
STRIPE 1 (filas 1-3)
├── columna ids:        [1, 2, 3]
├── columna productos:  [Laptop, Teclado, Silla]
├── columna categorias: [Tecnología, Tecnología, Oficina]
├── columna precios:    [1200, 45, 120]     ← solo esto se lee para sum(precio)
└── columna cantidades: [2, 5, 4]

STRIPE 2 (filas 4-6)
├── columna ids:        [4, 5, 6]
├── columna productos:  [Mesa, Tablet, Ratón]
...

FILE FOOTER (al final del fichero)
├── estadísticas globales: min/max/suma por columna
├── estadísticas por stripe: min/max por columna por franja
└── schema completo
```

En la práctica, para Spark puro se usa casi siempre Parquet. ORC es la elección natural si el ecosistema principal es **Hive** o **Hadoop**, donde ORC nació y está más optimizado.

</aside>

---

## **6.2 Escribir en ORC**

```scala
val rutaORC = s"$rutaBase/salida/ventas_orc"

dfVentasPreparadas.write
  .mode("overwrite")
  .orc(rutaORC)

println(s"Datos escritos en ORC:$rutaORC")
```

**Salida esperada:**

```scala
Datos escritos en ORC: C:/Curso-Scala/datos/dia17_s2/salida/ventas_orc
```

---

## **6.3 Leer ORC**

```scala
val dfVentasORC = spark.read.orc(rutaORC)

println("=== Datos leídos desde ORC ===")
dfVentasORC.show(5, truncate = false)

dfVentasORC.printSchema()
```

**Salida esperada parcial:**

```scala
=== Datos leídos desde ORC ===
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
|id_venta|fecha     |anio|mes|cliente|ciudad   |categoria  |producto|cantidad|precio_unitario|importe_total|
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
|1       |2026-01-05|2026|1  |Ana    |Madrid   |Informatica|Portatil|1       |850.5          |850.5        |
...
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
```

---

# **7. Avro**

## **7.1 ¿Qué es Avro?**

**Avro** es un formato binario orientado a registros. A diferencia de Parquet y ORC, no está pensado principalmente para leer columnas sueltas, sino para transportar o almacenar registros completos con schema.

Es muy usado en pipelines de eventos, integración entre sistemas y ecosistemas como Kafka.

| **Característica** | **Avro** |
| --- | --- |
| Tipo | Binario por filas / registros |
| Guarda schema | Sí |
| Uso típico | Eventos, Kafka, integración de sistemas |
| Bueno para | Intercambiar registros completos |
| Menos ideal para | Consultas analíticas que leen pocas columnas |

<aside>

Avro es un formato de almacenamiento **por filas**, igual que CSV y JSON, no columnar como Parquet u ORC.

Usando la misma lógica de la imagen, un fichero Avro con esos mismos datos se almacenaría así:

```scala
fila 1: {id:1, producto:Laptop,  categoria:Tecnología, precio:1200, cantidad:2}
fila 2: {id:2, producto:Teclado, categoria:Tecnología, precio:45,   cantidad:5}
fila 3: {id:3, producto:Silla,   categoria:Oficina,    precio:120,  cantidad:4}
```

Cada fila se guarda completa y junta, igual que en CSV o JSON. La diferencia con estos dos es que Avro es **binario** (no legible por humanos) y lleva el **schema embebido** en el propio fichero en formato JSON.

La consecuencia práctica es la misma que en CSV: si quieres calcular `sum(precio)`, Spark tiene que leer los cinco campos de cada fila aunque solo necesite `precio`. Por eso Avro **no es eficiente para analítica**, pero sí es muy bueno para **streaming y mensajería** (Kafka, por ejemplo), donde los datos llegan registro a registro y lo que importa es procesar cada mensaje completo lo antes posible, no hacer agregaciones sobre millones de columnas.

</aside>

---

## **7.2 Escribir en Avro**

Con Spark se usa el formato `avro` mediante `.format("avro")`.

```scala
val rutaAvro = s"$rutaBase/salida/ventas_avro"

dfVentasPreparadas.write
  .mode("overwrite")
  .format("avro")
  .save(rutaAvro)

println(s"Datos escritos en Avro:$rutaAvro")
```

**Salida esperada:**

```scala
Datos escritos en Avro: C:/Curso-Scala/datos/dia17_s2/salida/ventas_avro
```

---

## **7.3 Leer Avro**

```scala
val dfVentasAvro = spark.read
  .format("avro")
  .load(rutaAvro)

println("=== Datos leídos desde Avro ===")
dfVentasAvro.show(5, truncate = false)

dfVentasAvro.printSchema()
```

**Salida esperada parcial:**

```scala
=== Datos leídos desde Avro ===
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
|id_venta|fecha     |anio|mes|cliente|ciudad   |categoria  |producto|cantidad|precio_unitario|importe_total|
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
|1       |2026-01-05|2026|1  |Ana    |Madrid   |Informatica|Portatil|1       |850.5          |850.5        |
...
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
```

> 💡 Recuerda: si esta celda falla con un error de formato Avro no encontrado, revisa que hayas ejecutado la celda inicial con `spark-avro`.
> 

---

# **8. Modos de escritura**

Cuando escribimos un DataFrame, Spark necesita saber qué hacer si la ruta de salida ya existe.

| **Modo** | **Qué hace** | **Cuándo usarlo** |
| --- | --- | --- |
| `overwrite` | Borra la salida anterior y escribe de nuevo | Reprocesos completos |
| `append` | Añade nuevos datos a la carpeta existente | Cargas incrementales |
| `ignore` | Si la ruta existe, no hace nada | Evitar errores en procesos repetidos |
| `errorIfExists` | Falla si la ruta ya existe | Evitar sobrescribir por accidente |

---

## **8.1 `overwrite`**

```scala
val rutaOverwrite = s"$rutaBase/salida/modo_overwrite"

dfVentasPreparadas.write
  .mode("overwrite")
  .parquet(rutaOverwrite)

println("Primera escritura con overwrite completada")

dfVentasPreparadas.write
  .mode("overwrite")
  .parquet(rutaOverwrite)

println("Segunda escritura con overwrite completada sin error")
```

**Salida esperada:**

```scala
Primera escritura con overwrite completada
Segunda escritura con overwrite completada sin error
```

---

## **8.2 `append`**

```scala
val rutaAppend = s"$rutaBase/salida/modo_append"

val ventas2026 = dfVentasPreparadas.filter(col("anio") === 2026)
val ventas2025 = dfVentasPreparadas.filter(col("anio") === 2025)

ventas2026.write
  .mode("overwrite")
  .parquet(rutaAppend)

ventas2025.write
  .mode("append")
  .parquet(rutaAppend)

val dfAppend = spark.read.parquet(rutaAppend)
println(s"Total después de append:${dfAppend.count()}")
```

**Salida esperada:**

```scala
Total después de append: 10
```

---

## **8.3 `ignore`**

```scala
val rutaIgnore = s"$rutaBase/salida/modo_ignore"

dfVentasPreparadas.write
  .mode("overwrite")
  .parquet(rutaIgnore)

val conteoAntes = spark.read.parquet(rutaIgnore).count()

// Como la ruta ya existe, ignore no escribirá de nuevo ni dará error
dfVentasPreparadas.limit(2).write
  .mode("ignore")
  .parquet(rutaIgnore)

val conteoDespues = spark.read.parquet(rutaIgnore).count()

println(s"Registros antes:$conteoAntes")
println(s"Registros después de ignore:$conteoDespues")
```

**Salida esperada:**

```scala
Registros antes: 10
Registros después de ignore: 10
```

---

## **8.4 `errorIfExists`**

Este modo es el comportamiento por defecto. Si la ruta ya existe, Spark lanza un error.

```scala
val rutaError = s"$rutaBase/salida/modo_error"

dfVentasPreparadas.write
  .mode("overwrite")
  .parquet(rutaError)

// Esta segunda escritura fallará porque la ruta ya existe
// dfVentasPreparadas.write
//   .mode("errorIfExists")
//   .parquet(rutaError)
```

**Salida esperada si se descomenta la segunda escritura:**

```scala
org.apache.spark.sql.AnalysisException: [PATH_ALREADY_EXISTS] Path file:/C:/Curso-Scala/datos/dia17_s2/salida/modo_error already exists.
```

> 💡 Para principiantes, conviene dejar esta parte comentada y explicarla en clase. Así evitamos detener el notebook con un error intencionado.
> 

# **9. Escritura particionada con `partitionBy`**

`partitionBy` permite guardar los datos organizados en carpetas según el valor de una o varias columnas.

Por ejemplo, si escribimos por `anio`, Spark crea una estructura parecida a esta:

```
ventas_por_anio/
  anio=2025/
    part-0000...
  anio=2026/
    part-0000...
```

Esto es útil cuando después queremos leer solo un año concreto.

---

## **9.1 Escribir Parquet particionado por año y mes**

```scala
val rutaParquetParticionado = s"$rutaBase/salida/ventas_parquet_particionado"

dfVentasPreparadas.write
  .mode("overwrite")
  .partitionBy("anio", "mes")
  .parquet(rutaParquetParticionado)

println(s"Parquet particionado creado en:$rutaParquetParticionado")
```

**Salida esperada:**

```scala
Parquet particionado creado en: C:/Curso-Scala/datos/dia17_s2/salida/ventas_parquet_particionado
```

---

## **9.2 Escribir ORC particionado por año y mes**

```scala
val rutaORCParticionado = s"$rutaBase/salida/ventas_orc_particionado"

dfVentasPreparadas.write
  .mode("overwrite")
  .partitionBy("anio", "mes")
  .orc(rutaORCParticionado)

println(s"ORC particionado creado en:$rutaORCParticionado")
```

**Salida esperada:**

```scala
ORC particionado creado en: C:/Curso-Scala/datos/dia17_s2/salida/ventas_orc_particionado
```

---

## **9.3 Escribir Avro particionado por año y mes**

```scala
val rutaAvroParticionado = s"$rutaBase/salida/ventas_avro_particionado"

dfVentasPreparadas.write
  .mode("overwrite")
  .partitionBy("anio", "mes")
  .format("avro")
  .save(rutaAvroParticionado)

println(s"Avro particionado creado en:$rutaAvroParticionado")
```

**Salida esperada:**

```scala
Avro particionado creado en: C:/Curso-Scala/datos/dia17_s2/salida/ventas_avro_particionado
```

---

## **9.4 Leer solo ventas de 2026 desde una salida particionada**

```scala
val dfSolo2026 = spark.read
  .parquet(rutaParquetParticionado)
  .filter(col("anio") === 2026)

println("=== Ventas del año 2026 ===")
dfSolo2026.show(false)

println(s"Total ventas 2026:${dfSolo2026.count()}")
```

**Salida esperada:**

```scala
Total ventas 2026: 8
```

> 💡 Cuando escribimos con `partitionBy("anio", "mes")`, esas columnas pueden aparecer al final al leer de nuevo. Esto es normal: se convierten en columnas de partición.
> 

---

# **10. Comparar tamaño de CSV, JSON, Parquet, ORC y Avro**

Para comparar tamaños de forma sencilla, creamos una función que calcule el tamaño total de una carpeta.

```scala
def tamanoCarpetaBytes(path: String): Long = {
  def total(file: File): Long = {
    if (file.isFile) file.length()
    else file.listFiles().map(total).sum
  }
  total(new File(path))
}
```

Primero exportamos también una salida CSV y una salida JSON para usarlas como comparación. No repetimos la teoría de estos formatos: aquí se usan solo como formatos base para medir peso y rendimiento.

```scala
val rutaCSVSalida = s"$rutaBase/salida/ventas_csv_exportado"
val rutaJSONSalida = s"$rutaBase/salida/ventas_json_exportado"

dfVentasPreparadas.write
  .mode("overwrite")
  .option("header", "true")
  .csv(rutaCSVSalida)

dfVentasPreparadas.write
  .mode("overwrite")
  .json(rutaJSONSalida)

println(s"CSV exportado en:$rutaCSVSalida")
println(s"JSON exportado en:$rutaJSONSalida")
```

Ahora medimos los tamaños.

```scala
val sizeCSV = tamanoCarpetaBytes(rutaCSVSalida)
val sizeJSON = tamanoCarpetaBytes(rutaJSONSalida)
val sizeParquet = tamanoCarpetaBytes(rutaParquet)
val sizeORC = tamanoCarpetaBytes(rutaORC)
val sizeAvro = tamanoCarpetaBytes(rutaAvro)

println(s"Tamaño CSV:$sizeCSV bytes")
println(s"Tamaño JSON:$sizeJSON bytes")
println(s"Tamaño Parquet:$sizeParquet bytes")
println(s"Tamaño ORC:$sizeORC bytes")
println(s"Tamaño Avro:$sizeAvro bytes")
```

**Salida esperada aproximada:**

```scala
Tamaño CSV:     2500 bytes
Tamaño JSON:    3200 bytes
Tamaño Parquet: 6000 bytes
Tamaño ORC:     5000 bytes
Tamaño Avro:    4500 bytes
```

> ⚠️ Con datasets muy pequeños, los formatos binarios pueden ocupar más que CSV o JSON porque guardan metadatos y schema. En datasets grandes, Parquet y ORC suelen ser más eficientes por compresión, lectura columnar y estadísticas internas. JSON normalmente ocupa más que CSV porque repite los nombres de campo en cada registro.
> 

---

# **11. Comparar tiempo de escritura y lectura entre formatos**

Con datos pequeños, la diferencia puede no ser estable. Aun así, este ejercicio enseña cómo medir de forma básica tanto la escritura como la lectura.

```scala
def medirTiempo[T](descripcion: String)(bloque: => T): (T, Double) = {
  val inicio = System.nanoTime()
  val resultado = bloque
  val fin = System.nanoTime()
  val ms = (fin - inicio) / 1000000.0
  println(f"$descripcion →$ms%.2f ms")
  (resultado, ms)
}
```

## **11.1 Medir escritura completa**

Para comparar escritura de forma limpia, escribimos en rutas nuevas de benchmark.

```scala
val rutaBenchCSV = s"$rutaBase/benchmark/ventas_csv"
val rutaBenchJSON = s"$rutaBase/benchmark/ventas_json"
val rutaBenchParquet = s"$rutaBase/benchmark/ventas_parquet"
val rutaBenchORC = s"$rutaBase/benchmark/ventas_orc"
val rutaBenchAvro = s"$rutaBase/benchmark/ventas_avro"

val (_, escrituraCSV) = medirTiempo("Escritura CSV") {
  dfVentasPreparadas.write
    .mode("overwrite")
    .option("header", "true")
    .csv(rutaBenchCSV)
}

val (_, escrituraJSON) = medirTiempo("Escritura JSON") {
  dfVentasPreparadas.write
    .mode("overwrite")
    .json(rutaBenchJSON)
}

val (_, escrituraParquet) = medirTiempo("Escritura Parquet") {
  dfVentasPreparadas.write
    .mode("overwrite")
    .parquet(rutaBenchParquet)
}

val (_, escrituraORC) = medirTiempo("Escritura ORC") {
  dfVentasPreparadas.write
    .mode("overwrite")
    .orc(rutaBenchORC)
}

val (_, escrituraAvro) = medirTiempo("Escritura Avro") {
  dfVentasPreparadas.write
    .mode("overwrite")
    .format("avro")
    .save(rutaBenchAvro)
}
```

**Salida esperada aproximada:**

```scala
Escritura CSV → 300.00 ms
Escritura JSON → 330.00 ms
Escritura Parquet → 180.00 ms
Escritura ORC → 190.00 ms
Escritura Avro → 220.00 ms
```

---

## **11.2 Medir lectura completa con `count()`**

```scala
val (_, lecturaCSV) = medirTiempo("Lectura CSV + count") {
  spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(rutaBenchCSV)
    .count()
}

val (_, lecturaJSON) = medirTiempo("Lectura JSON + count") {
  spark.read
    .json(rutaBenchJSON)
    .count()
}

val (_, lecturaParquet) = medirTiempo("Lectura Parquet + count") {
  spark.read
    .parquet(rutaBenchParquet)
    .count()
}

val (_, lecturaORC) = medirTiempo("Lectura ORC + count") {
  spark.read
    .orc(rutaBenchORC)
    .count()
}

val (_, lecturaAvro) = medirTiempo("Lectura Avro + count") {
  spark.read
    .format("avro")
    .load(rutaBenchAvro)
    .count()
}
```

**Salida esperada aproximada:**

```scala
Lectura CSV + count → 250.00 ms
Lectura JSON + count → 280.00 ms
Lectura Parquet + count → 90.00 ms
Lectura ORC + count → 95.00 ms
Lectura Avro + count → 120.00 ms
```

---

## **11.3 Crear una tabla resumen de tiempos**

```scala
val comparativaTiempos = Seq(
  ("CSV", escrituraCSV, lecturaCSV),
  ("JSON", escrituraJSON, lecturaJSON),
  ("Parquet", escrituraParquet, lecturaParquet),
  ("ORC", escrituraORC, lecturaORC),
  ("Avro", escrituraAvro, lecturaAvro)
).toDF("formato", "tiempo_escritura_ms", "tiempo_lectura_ms")

comparativaTiempos.show(false)
```

**Salida esperada aproximada:**

```scala
+-------+-------------------+-----------------+
|formato|tiempo_escritura_ms|tiempo_lectura_ms|
+-------+-------------------+-----------------+
|CSV    |300.0              |250.0            |
|JSON   |330.0              |280.0            |
|Parquet|180.0              |90.0             |
|ORC    |190.0              |95.0             |
|Avro   |220.0              |120.0            |
+-------+-------------------+-----------------+
```

> 💡 Los tiempos pueden cambiar en cada ordenador. Lo importante es comparar el procedimiento, no memorizar los números.
> 

---

## **11.4 Medir lectura de pocas columnas**

Esta prueba ayuda a entender por qué Parquet y ORC son interesantes para analítica.

```scala
medirTiempo("CSV seleccionando 2 columnas") {
  spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(rutaBenchCSV)
    .select("ciudad", "importe_total")
    .count()
}

medirTiempo("JSON seleccionando 2 columnas") {
  spark.read
    .json(rutaBenchJSON)
    .select("ciudad", "importe_total")
    .count()
}

medirTiempo("Parquet seleccionando 2 columnas") {
  spark.read
    .parquet(rutaBenchParquet)
    .select("ciudad", "importe_total")
    .count()
}

medirTiempo("ORC seleccionando 2 columnas") {
  spark.read
    .orc(rutaBenchORC)
    .select("ciudad", "importe_total")
    .count()
}

medirTiempo("Avro seleccionando 2 columnas") {
  spark.read
    .format("avro")
    .load(rutaBenchAvro)
    .select("ciudad", "importe_total")
    .count()
}
```

**Salida esperada aproximada:**

```scala
CSV seleccionando 2 columnas → 230.00 ms
JSON seleccionando 2 columnas → 260.00 ms
Parquet seleccionando 2 columnas → 70.00 ms
ORC seleccionando 2 columnas → 75.00 ms
Avro seleccionando 2 columnas → 115.00 ms
```

> 💡 En formatos columnares, seleccionar pocas columnas suele ser más eficiente porque Spark puede evitar leer columnas que no necesita.
> 

---

# **12. Resumen comparativo de formatos**

| **Formato** | **Tipo** | **Ventaja principal** | **Desventaja principal** | **Cuándo usarlo** |
| --- | --- | --- | --- | --- |
| CSV | Texto por filas | Muy simple y universal | Sin schema interno, pesado para analítica | Intercambio manual o entrada inicial |
| JSON | Texto semiestructurado | Flexible para datos con estructura variable | Puede ser pesado y lento | APIs, logs, documentos simples |
| Parquet | Binario columnar | Muy bueno para analítica y Data Lakes | No es cómodo de abrir manualmente | Capa silver/gold, consultas BI, lakehouse |
| ORC | Binario columnar | Muy optimizado en ecosistema Hive/Hadoop | Menos común fuera de ese ecosistema | Data Warehouse sobre Hadoop/Hive |
| Avro | Binario por registros | Bueno para eventos y transporte con schema | Menos eficiente para leer pocas columnas | Kafka, eventos, integración entre sistemas |

# **💻 Práctica guiada**

---

## **Ejercicio 1 — Crear DataFrame base y columna calculada**

### **Enunciado**

Usa el CSV `ventas.csv` creado al inicio de la sesión, cárgalo con schema manual y crea una columna `importe_total`.

### **Código solución**

```scala
val dfEj1 = spark.read
  .option("header", "true")
  .schema(schemaVentas)
  .csv(rutaVentasCSV)

val dfEj1Preparado = dfEj1
  .withColumn("importe_total", col("cantidad") * col("precio_unitario"))

println("=== DataFrame preparado ===")
dfEj1Preparado.show(false)
```

### **Salida esperada parcial**

```scala
+--------+----------+----+---+-------+---------+-----------+------------+--------+---------------+-------------+
|id_venta|fecha     |anio|mes|cliente|ciudad   |categoria  |producto    |cantidad|precio_unitario|importe_total|
+--------+----------+----+---+-------+---------+-----------+------------+--------+---------------+-------------+
|1       |2026-01-05|2026|1  |Ana    |Madrid   |Informatica|Portatil    |1       |850.5          |850.5        |
...
+--------+----------+----+---+-------+---------+-----------+------------+--------+---------------+-------------+
```

---

## **Ejercicio 2 — Escribir y leer Parquet**

### **Enunciado**

Guarda `dfEj1Preparado` en Parquet y vuelve a leerlo.

### **Código solución**

```scala
val rutaEj2Parquet = s"$rutaBase/practica/ventas_parquet_ej2"

dfEj1Preparado.write
  .mode("overwrite")
  .parquet(rutaEj2Parquet)

val dfEj2Parquet = spark.read.parquet(rutaEj2Parquet)

println("=== Parquet leído ===")
dfEj2Parquet.show(5, truncate = false)
dfEj2Parquet.printSchema()
```

### **Salida esperada parcial**

```scala
=== Parquet leído ===
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
|id_venta|fecha     |anio|mes|cliente|ciudad   |categoria  |producto|cantidad|precio_unitario|importe_total|
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
|1       |2026-01-05|2026|1  |Ana    |Madrid   |Informatica|Portatil|1       |850.5          |850.5        |
...
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
```

---

## **Ejercicio 3 — Escribir y leer ORC**

### **Enunciado**

Guarda `dfEj1Preparado` en ORC y vuelve a leerlo.

### **Código solución**

```scala
val rutaEj3ORC = s"$rutaBase/practica/ventas_orc_ej3"

dfEj1Preparado.write
  .mode("overwrite")
  .orc(rutaEj3ORC)

val dfEj3ORC = spark.read.orc(rutaEj3ORC)

println("=== ORC leído ===")
dfEj3ORC.show(5, truncate = false)
dfEj3ORC.printSchema()
```

### **Salida esperada parcial**

```scala
=== ORC leído ===
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
|id_venta|fecha     |anio|mes|cliente|ciudad   |categoria  |producto|cantidad|precio_unitario|importe_total|
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
|1       |2026-01-05|2026|1  |Ana    |Madrid   |Informatica|Portatil|1       |850.5          |850.5        |
...
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
```

---

## **Ejercicio 4 — Escribir y leer Avro**

### **Enunciado**

Guarda `dfEj1Preparado` en Avro y vuelve a leerlo.

### **Código solución**

```scala
val rutaEj4Avro = s"$rutaBase/practica/ventas_avro_ej4"

dfEj1Preparado.write
  .mode("overwrite")
  .format("avro")
  .save(rutaEj4Avro)

val dfEj4Avro = spark.read
  .format("avro")
  .load(rutaEj4Avro)

println("=== Avro leído ===")
dfEj4Avro.show(5, truncate = false)
dfEj4Avro.printSchema()
```

### **Salida esperada parcial**

```scala
=== Avro leído ===
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
|id_venta|fecha     |anio|mes|cliente|ciudad   |categoria  |producto|cantidad|precio_unitario|importe_total|
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
|1       |2026-01-05|2026|1  |Ana    |Madrid   |Informatica|Portatil|1       |850.5          |850.5        |
...
+--------+----------+----+---+-------+---------+-----------+--------+--------+---------------+-------------+
```

---

## **Ejercicio 5 — Comparar tamaños**

### **Enunciado**

Exporta el mismo DataFrame en CSV, JSON, Parquet, ORC y Avro. Después compara el peso de cada salida.

### **Código solución**

```scala
val rutaEj5CSV = s"$rutaBase/practica/comparacion_csv"
val rutaEj5JSON = s"$rutaBase/practica/comparacion_json"
val rutaEj5Parquet = s"$rutaBase/practica/comparacion_parquet"
val rutaEj5ORC = s"$rutaBase/practica/comparacion_orc"
val rutaEj5Avro = s"$rutaBase/practica/comparacion_avro"

dfEj1Preparado.write.mode("overwrite").option("header", "true").csv(rutaEj5CSV)
dfEj1Preparado.write.mode("overwrite").json(rutaEj5JSON)
dfEj1Preparado.write.mode("overwrite").parquet(rutaEj5Parquet)
dfEj1Preparado.write.mode("overwrite").orc(rutaEj5ORC)
dfEj1Preparado.write.mode("overwrite").format("avro").save(rutaEj5Avro)

println(s"CSV:${tamanoCarpetaBytes(rutaEj5CSV)} bytes")
println(s"JSON:${tamanoCarpetaBytes(rutaEj5JSON)} bytes")
println(s"Parquet:${tamanoCarpetaBytes(rutaEj5Parquet)} bytes")
println(s"ORC:${tamanoCarpetaBytes(rutaEj5ORC)} bytes")
println(s"Avro:${tamanoCarpetaBytes(rutaEj5Avro)} bytes")
```

### **Salida esperada aproximada**

```scala
CSV:     2500 bytes
JSON:    3200 bytes
Parquet: 6000 bytes
ORC:     5000 bytes
Avro:    4500 bytes
```

---

## **Ejercicio 6 — Comparar tiempos de lectura**

### **Enunciado**

Mide el tiempo de lectura y conteo de cada formato, incluyendo JSON como formato de texto semiestructurado.

### **Código solución**

```scala
medirTiempo("CSV + count") {
  spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(rutaEj5CSV)
    .count()
}

medirTiempo("JSON + count") {
  spark.read.json(rutaEj5JSON).count()
}

medirTiempo("Parquet + count") {
  spark.read.parquet(rutaEj5Parquet).count()
}

medirTiempo("ORC + count") {
  spark.read.orc(rutaEj5ORC).count()
}

medirTiempo("Avro + count") {
  spark.read.format("avro").load(rutaEj5Avro).count()
}
```

### **Salida esperada aproximada**

```scala
CSV + count → 250.00 ms
JSON + count → 280.00 ms
Parquet + count → 90.00 ms
ORC + count → 95.00 ms
Avro + count → 120.00 ms
```

---

## **Ejercicio 7 — Escribir formatos particionados**

### **Enunciado**

Guarda el mismo DataFrame particionado por `anio` y `mes` en Parquet, ORC y Avro.

### **Código solución**

```scala
val rutaEj7ParquetPart = s"$rutaBase/practica/particionado_parquet"
val rutaEj7ORCPart = s"$rutaBase/practica/particionado_orc"
val rutaEj7AvroPart = s"$rutaBase/practica/particionado_avro"

dfEj1Preparado.write
  .mode("overwrite")
  .partitionBy("anio", "mes")
  .parquet(rutaEj7ParquetPart)

dfEj1Preparado.write
  .mode("overwrite")
  .partitionBy("anio", "mes")
  .orc(rutaEj7ORCPart)

dfEj1Preparado.write
  .mode("overwrite")
  .partitionBy("anio", "mes")
  .format("avro")
  .save(rutaEj7AvroPart)

println("Formatos particionados creados correctamente")
```

### **Salida esperada**

```scala
Formatos particionados creados correctamente
```

---

# **🏢 Caso de estudio propuesto: RetailLake Analytics**

---

## **Contexto empresarial**

**RetailLake Analytics** es una empresa de comercio electrónico que está construyendo su primer Data Lake. Hasta ahora, las ventas diarias se recibían en CSV y se procesaban manualmente. El equipo de Data Engineering quiere evaluar distintos formatos de almacenamiento para decidir cuál usar en las capas analíticas.

La empresa necesita comparar:

- CSV como formato de entrada original.
- JSON como formato semiestructurado de intercambio.
- Parquet como formato analítico general.
- ORC como alternativa columnar compatible con ecosistemas Hive/Hadoop.
- Avro como formato de intercambio de registros con schema.

Tu tarea será construir un pequeño pipeline que lea ventas, prepare datos, escriba en varios formatos, compare peso y tiempo de lectura, y genere una recomendación técnica.

Este caso usa únicamente conocimientos de esta sesión: lectura, escritura, formatos, modos de escritura, particionado, comparación de tamaño y medición básica de tiempo.

---

## **📦 Datos del caso**

Crea el fichero `retail_ventas.csv` en la carpeta `C:/Curso-Scala/datos/dia17_s2/caso/`.

```scala
val rutaCaso = s"$rutaBase/caso"
Files.createDirectories(Paths.get(rutaCaso))

val contenidoCasoCSV =
"""id_venta,fecha,anio,mes,canal,ciudad,categoria,producto,cantidad,precio_unitario
1001,2026-01-03,2026,1,Web,Madrid,Informatica,Portatil,2,820.00
1002,2026-01-04,2026,1,App,Valencia,Audio,Auriculares,3,55.00
1003,2026-01-10,2026,1,Web,Sevilla,Oficina,Silla,5,115.00
1004,2026-02-02,2026,2,App,Madrid,Informatica,Monitor,2,210.00
1005,2026-02-09,2026,2,Web,Barcelona,Oficina,Mesa,1,260.00
1006,2026-02-12,2026,2,Web,Zaragoza,Informatica,Webcam,6,42.00
1007,2026-03-01,2026,3,App,Madrid,Informatica,Teclado,4,48.00
1008,2026-03-04,2026,3,Web,Valencia,Audio,Auriculares,2,60.00
1009,2026-03-08,2026,3,App,Sevilla,Oficina,Silla,3,118.00
1010,2025-12-15,2025,12,Web,Madrid,Informatica,Portatil,1,790.00
1011,2025-12-16,2025,12,App,Barcelona,Audio,Auriculares,2,52.00
1012,2025-12-20,2025,12,Web,Valencia,Oficina,Mesa,1,240.00
"""

val rutaCasoCSV = s"$rutaCaso/retail_ventas.csv"

Files.write(
  Paths.get(rutaCasoCSV),
  contenidoCasoCSV.getBytes(StandardCharsets.UTF_8)
)

println(s"Fichero del caso creado en:$rutaCasoCSV")
```

**Salida esperada:**

```scala
Fichero del caso creado en: C:/Curso-Scala/datos/dia17_s2/caso/retail_ventas.csv
```

---

## **Tarea 1 — Cargar el CSV de entrada con schema manual**

### **Enunciado**

Carga el CSV de RetailLake con un schema manual para controlar los tipos.

### **Solución**

```scala
val schemaRetail = StructType(List(
  StructField("id_venta",        IntegerType, nullable = false),
  StructField("fecha",           DateType,    nullable = true),
  StructField("anio",            IntegerType, nullable = true),
  StructField("mes",             IntegerType, nullable = true),
  StructField("canal",           StringType,  nullable = true),
  StructField("ciudad",          StringType,  nullable = true),
  StructField("categoria",       StringType,  nullable = true),
  StructField("producto",        StringType,  nullable = true),
  StructField("cantidad",        IntegerType, nullable = true),
  StructField("precio_unitario", DoubleType,  nullable = true)
))

val dfRetail = spark.read
  .option("header", "true")
  .schema(schemaRetail)
  .csv(rutaCasoCSV)

println("=== Ventas RetailLake ===")
dfRetail.show(false)

dfRetail.printSchema()
println(s"Total registros:${dfRetail.count()}")
```

**Salida esperada:**

```scala
Total registros: 12
```

---

## **Tarea 2 — Preparar el DataFrame para escritura**

### **Enunciado**

Crea una columna `importe_total` multiplicando `cantidad` por `precio_unitario`. Guarda el resultado en la variable `dfRetailPreparado`.

### **Solución**

```scala
val dfRetailPreparado = dfRetail
  .withColumn("importe_total", col("cantidad") * col("precio_unitario"))

println("=== Dataset preparado ===")
dfRetailPreparado.show(false)
```

**Salida esperada parcial:**

```scala
+--------+----------+----+---+-----+---------+-----------+------------+--------+---------------+-------------+
|id_venta|fecha     |anio|mes|canal|ciudad   |categoria  |producto    |cantidad|precio_unitario|importe_total|
+--------+----------+----+---+-----+---------+-----------+------------+--------+---------------+-------------+
|1001    |2026-01-03|2026|1  |Web  |Madrid   |Informatica|Portatil    |2       |820.0          |1640.0       |
...
+--------+----------+----+---+-----+---------+-----------+------------+--------+---------------+-------------+
```

---

## **Tarea 3 — Guardar en JSON, Parquet, ORC y Avro**

### **Enunciado**

Guarda `dfRetailPreparado` en cuatro formatos diferentes usando `overwrite`:

- JSON
- Parquet
- ORC
- Avro

### **Solución**

```scala
val rutaRetailJSON = s"$rutaCaso/salida/retail_json"
val rutaRetailParquet = s"$rutaCaso/salida/retail_parquet"
val rutaRetailORC = s"$rutaCaso/salida/retail_orc"
val rutaRetailAvro = s"$rutaCaso/salida/retail_avro"

dfRetailPreparado.write
  .mode("overwrite")
  .json(rutaRetailJSON)

dfRetailPreparado.write
  .mode("overwrite")
  .parquet(rutaRetailParquet)

dfRetailPreparado.write
  .mode("overwrite")
  .orc(rutaRetailORC)

dfRetailPreparado.write
  .mode("overwrite")
  .format("avro")
  .save(rutaRetailAvro)

println("Formatos generados correctamente")
```

**Salida esperada:**

```scala
Formatos generados correctamente
```

---

## **Tarea 4 — Leer cada formato y validar conteos**

### **Enunciado**

Lee de nuevo las cuatro salidas y comprueba que todas conservan los 12 registros.

### **Solución**

```scala
val dfRetailJSON = spark.read.json(rutaRetailJSON)
val dfRetailParquet = spark.read.parquet(rutaRetailParquet)
val dfRetailORC = spark.read.orc(rutaRetailORC)
val dfRetailAvro = spark.read.format("avro").load(rutaRetailAvro)

println(s"Registros JSON:${dfRetailJSON.count()}")
println(s"Registros Parquet:${dfRetailParquet.count()}")
println(s"Registros ORC:${dfRetailORC.count()}")
println(s"Registros Avro:${dfRetailAvro.count()}")
```

**Salida esperada:**

```scala
Registros JSON:    12
Registros Parquet: 12
Registros ORC:     12
Registros Avro:    12
```

---

## **Tarea 5 — Guardar versiones particionadas por año y mes**

### **Enunciado**

Guarda el dataset preparado particionado por `anio` y `mes` en Parquet, ORC y Avro.

### **Solución**

```scala
val rutaRetailParquetPart = s"$rutaCaso/salida/retail_parquet_particionado"
val rutaRetailORCPart = s"$rutaCaso/salida/retail_orc_particionado"
val rutaRetailAvroPart = s"$rutaCaso/salida/retail_avro_particionado"

dfRetailPreparado.write
  .mode("overwrite")
  .partitionBy("anio", "mes")
  .parquet(rutaRetailParquetPart)

dfRetailPreparado.write
  .mode("overwrite")
  .partitionBy("anio", "mes")
  .orc(rutaRetailORCPart)

dfRetailPreparado.write
  .mode("overwrite")
  .partitionBy("anio", "mes")
  .format("avro")
  .save(rutaRetailAvroPart)

println("Versiones particionadas creadas correctamente")
```

**Salida esperada:**

```scala
Versiones particionadas creadas correctamente
```

---

## **Tarea 6 — Leer solo ventas del año 2026 desde Parquet particionado**

### **Enunciado**

Lee el Parquet particionado y filtra únicamente las ventas del año 2026.

### **Solución**

```scala
val dfRetail2026 = spark.read
  .parquet(rutaRetailParquetPart)
  .filter(col("anio") === 2026)

println("=== Ventas RetailLake 2026 ===")
dfRetail2026.show(false)

println(s"Total ventas 2026:${dfRetail2026.count()}")
```

**Salida esperada:**

```scala
Total ventas 2026: 9
```

---

## **Tarea 7 — Comparar peso en disco**

### **Enunciado**

Compara el tamaño del CSV original y de las salidas JSON, Parquet, ORC y Avro.

### **Solución**

```scala
val sizeRetailCSV = tamanoCarpetaBytes(rutaCasoCSV)
val sizeRetailJSON = tamanoCarpetaBytes(rutaRetailJSON)
val sizeRetailParquet = tamanoCarpetaBytes(rutaRetailParquet)
val sizeRetailORC = tamanoCarpetaBytes(rutaRetailORC)
val sizeRetailAvro = tamanoCarpetaBytes(rutaRetailAvro)

println(s"Tamaño CSV:$sizeRetailCSV bytes")
println(s"Tamaño JSON:$sizeRetailJSON bytes")
println(s"Tamaño Parquet:$sizeRetailParquet bytes")
println(s"Tamaño ORC:$sizeRetailORC bytes")
println(s"Tamaño Avro:$sizeRetailAvro bytes")
```

**Salida esperada aproximada:**

```scala
Tamaño CSV:     3000 bytes
Tamaño JSON:    3800 bytes
Tamaño Parquet: 7000 bytes
Tamaño ORC:     6000 bytes
Tamaño Avro:    5000 bytes
```

> 💡 El tamaño exacto puede cambiar según la versión de Spark, el sistema operativo, el número de particiones y la compresión aplicada.
> 

---

## **Tarea 8 — Comparar tiempo de lectura**

### **Enunciado**

Mide cuánto tarda Spark en leer y contar registros desde CSV, JSON, Parquet, ORC y Avro.

### **Solución**

```scala
medirTiempo("Retail CSV + count") {
  spark.read
    .option("header", "true")
    .schema(schemaRetail)
    .csv(rutaCasoCSV)
    .count()
}

medirTiempo("Retail JSON + count") {
  spark.read.json(rutaRetailJSON).count()
}

medirTiempo("Retail Parquet + count") {
  spark.read.parquet(rutaRetailParquet).count()
}

medirTiempo("Retail ORC + count") {
  spark.read.orc(rutaRetailORC).count()
}

medirTiempo("Retail Avro + count") {
  spark.read.format("avro").load(rutaRetailAvro).count()
}
```

**Salida esperada aproximada:**

```scala
Retail CSV + count → 250.00 ms
Retail JSON + count → 280.00 ms
Retail Parquet + count → 90.00 ms
Retail ORC + count → 95.00 ms
Retail Avro + count → 120.00 ms
```

---

## **Tarea 9 — Crear una recomendación técnica**

### **Enunciado**

Según los resultados obtenidos, escribe una recomendación para RetailLake.

Debe responder:

1. ¿Qué formato usarías para la capa analítica principal?
2. ¿Qué formato usarías si el sistema se integra con Kafka o eventos?
3. ¿Por qué no usarías CSV o JSON como formato principal del Data Lake analítico?

---

## Caso practico propuesto

# **1. Contexto empresarial**

## **🏢 Empresa ficticia: RetailNova**

**RetailNova** es una empresa de comercio electrónico que vende productos tecnológicos en España. Cada año exporta sus ventas en un fichero CSV independiente. El equipo de datos quiere construir una pequeña capa analítica optimizada para que los analistas puedan consultar los resultados con **Spark SQL**.

Actualmente la empresa dispone de varios CSV anuales:

```
ventas_2022.csv
ventas_2023.csv
ventas_2024.csv
```

Cada fichero contiene ventas con información de cliente, producto, canal, fecha, unidades, precio, descuento y país. El objetivo del caso práctico es construir este flujo:

![image.png](image.png)

---

---

# **3. Inicialización del entorno**

Ejecuta esta celda al principio del notebook.

```scala
import $ivy.`org.apache.spark::spark-sql:4.1.1`
import $ivy.`org.apache.spark::spark-avro:4.1.1`

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

val spark = SparkSession.builder()
  .appName("RetailNova_Dataset_DataFrame_Parquet_SQL")
  .master("local[*]")
  .config("spark.ui.showConsoleProgress", "false")
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

import spark.implicits._

println(s"✅ Spark${spark.version} listo")
println(s"✅ Scala${scala.util.Properties.versionNumberString} listo")
```

**Salida esperada:**

```
✅ Spark 4.1.1 listo
✅ Scala 2.13.18 listo
```

---

# **4. Crear los CSV sintéticos de la empresa**

## **4.1 Crear carpeta de trabajo**

```scala
val rutaBase = "C:/Curso-Scala/datos/retailnova"
Files.createDirectories(Paths.get(rutaBase))

println(s"Carpeta creada o existente:$rutaBase")
```

---

## **4.2 Crear `ventas_2022.csv`**

```scala
val ventas2022 =
"""id_venta,fecha,id_cliente,cliente,pais,canal,categoria,producto,unidades,precio_unitario,descuento_pct
V2022-001,2022-01-15,C001,Ana García,España,web,Informática,Portátil Pro,1,950.00,5
V2022-002,2022-02-03,C002,Luis Martín,España,tienda,Periféricos,Teclado Mecánico,2,75.00,0
V2022-003,2022-03-18,C003,Marta López,España,web,Periféricos,Ratón Inalámbrico,3,29.90,10
V2022-004,2022-04-22,C004,Carlos Ruiz,Portugal,marketplace,Audio,Auriculares USB,2,59.99,5
V2022-005,2022-05-11,C005,Elena Vega,España,web,Monitores,Monitor 27,1,220.00,15
V2022-006,2022-06-30,C006,Jorge Díaz,Francia,tienda,Informática,Tablet 10,1,310.00,0
V2022-007,2022-08-09,C007,Laura Prieto,España,web,Almacenamiento,SSD 1TB,2,115.00,5
V2022-008,2022-09-25,C008,Pedro Santos,España,marketplace,Audio,Webcam HD,1,79.00,0
V2022-009,2022-10-14,C009,Sofía Ramos,Portugal,web,Informática,Portátil Air,1,780.00,8
V2022-010,2022-12-02,C010,Andrés Mora,España,tienda,Periféricos,Hub USB-C,4,35.00,0
"""

Files.write(
  Paths.get(s"$rutaBase/ventas_2022.csv"),
  ventas2022.getBytes(StandardCharsets.UTF_8)
)

println("✅ ventas_2022.csv creado")
```

---

## **4.3 Crear `ventas_2023.csv`**

```scala
val ventas2023 =
"""id_venta,fecha,id_cliente,cliente,pais,canal,categoria,producto,unidades,precio_unitario,descuento_pct
V2023-001,2023-01-09,C001,Ana García,España,web,Informática,Portátil Pro,1,970.00,4
V2023-002,2023-01-21,C011,Nuria Castro,España,marketplace,Audio,Auriculares Bluetooth,2,89.90,10
V2023-003,2023-02-15,C012,Raúl Gómez,Portugal,web,Monitores,Monitor 32,1,310.00,12
V2023-004,2023-03-05,C003,Marta López,España,tienda,Periféricos,Teclado Mecánico,1,79.00,0
V2023-005,2023-04-17,C013,Clara Soler,Francia,web,Informática,Tablet 10,2,299.00,5
V2023-006,2023-06-10,C014,David León,España,marketplace,Almacenamiento,Disco Externo 2TB,1,95.00,0
V2023-007,2023-07-19,C015,Lucía Torres,España,web,Audio,Micrófono USB,1,120.00,15
V2023-008,2023-09-01,C016,Iván Navarro,Portugal,tienda,Periféricos,Ratón Inalámbrico,2,31.00,5
V2023-009,2023-10-28,C017,Paula Marín,España,web,Informática,Portátil Air,1,810.00,7
V2023-010,2023-11-30,C018,Marcos Vidal,España,marketplace,Monitores,Monitor 27,2,215.00,10
"""

Files.write(
  Paths.get(s"$rutaBase/ventas_2023.csv"),
  ventas2023.getBytes(StandardCharsets.UTF_8)
)

println("✅ ventas_2023.csv creado")
```

---

## **4.4 Crear `ventas_2024.csv`**

```scala
val ventas2024 =
"""id_venta,fecha,id_cliente,cliente,pais,canal,categoria,producto,unidades,precio_unitario,descuento_pct
V2024-001,2024-01-12,C019,Isabel Romero,España,web,Informática,Portátil Pro,1,990.00,3
V2024-002,2024-02-08,C020,Hugo Molina,España,marketplace,Audio,Auriculares Bluetooth,1,94.90,5
V2024-003,2024-03-23,C021,Teresa Cano,Portugal,web,Almacenamiento,SSD 1TB,3,109.00,8
V2024-004,2024-04-04,C022,Álvaro Peña,España,tienda,Monitores,Monitor 32,1,330.00,10
V2024-005,2024-05-16,C023,Noelia Gil,Francia,web,Informática,Tablet 10,1,289.00,0
V2024-006,2024-06-27,C024,Rubén Flores,España,marketplace,Periféricos,Hub USB-C,2,39.00,0
V2024-007,2024-07-13,C025,Beatriz León,España,web,Audio,Micrófono USB,2,115.00,12
V2024-008,2024-08-29,C026,Sergio Vega,Portugal,tienda,Periféricos,Teclado Mecánico,1,82.00,0
V2024-009,2024-10-03,C027,Celia Robles,España,web,Informática,Portátil Air,1,835.00,6
V2024-010,2024-11-18,C028,Miguel Arias,España,marketplace,Monitores,Monitor 27,1,225.00,5
"""

Files.write(
  Paths.get(s"$rutaBase/ventas_2024.csv"),
  ventas2024.getBytes(StandardCharsets.UTF_8)
)

println("✅ ventas_2024.csv creado")
```

---

# **5. Parte 1 — Lectura de varios CSV como DataFrame**

## **5.1 Leer todos los CSV anuales**

```scala
val rutaCSV = s"$rutaBase/ventas_*.csv"

val dfRaw = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(rutaCSV)

println("=== Datos cargados desde varios CSV ===")
dfRaw.show(10, truncate = false)

println("=== Schema inferido ===")
dfRaw.printSchema()

println(s"Total de ventas cargadas:${dfRaw.count()}")
```

**Salida esperada aproximada:**

```
Total de ventas cargadas: 30
```

---

## **5.2 Preguntas:**

1. ¿Cuántos CSV se han leído realmente?
2. ¿Spark ha unido los tres ficheros en un único DataFrame?
3. ¿Qué tipos ha inferido Spark para `fecha`, `unidades`, `precio_unitario` y `descuento_pct`?
4. ¿Por qué no conviene depender siempre de `inferSchema` en producción?

---

# **6. Parte 2 — Normalización de tipos y columnas**

Antes de convertir a Dataset, conviene controlar los tipos.

```scala
val dfNormalizado = dfRaw
  .withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))
  .withColumn("unidades", col("unidades").cast(IntegerType))
  .withColumn("precio_unitario", col("precio_unitario").cast(DoubleType))
  .withColumn("descuento_pct", col("descuento_pct").cast(DoubleType))
  .withColumn("pais", trim(col("pais")))
  .withColumn("canal", lower(trim(col("canal"))))
  .withColumn("categoria", trim(col("categoria")))
  .withColumn("producto", trim(col("producto")))

println("=== DataFrame normalizado ===")
dfNormalizado.show(10, truncate = false)

dfNormalizado.printSchema()
```

**Concepto clave:**

```
Primero limpiamos y normalizamos con DataFrame porque las funciones nativas de Spark son eficientes y optimizables.
```

---

# **7. Parte 3 — Selección de columnas útiles para Dataset**

No siempre interesa convertir todas las columnas a Dataset. Normalmente seleccionamos las columnas relevantes para la lógica de negocio.

```scala
val dfVentasSeleccionadas = dfNormalizado.select(
  col("id_venta"),
  col("fecha"),
  col("id_cliente"),
  col("cliente"),
  col("pais"),
  col("canal"),
  col("categoria"),
  col("producto"),
  col("unidades"),
  col("precio_unitario"),
  col("descuento_pct")
)

println("=== Columnas seleccionadas ===")
dfVentasSeleccionadas.show(5, truncate = false)
```

---

# **8. Parte 4 — Convertir DataFrame a Dataset tipado**

## **8.1 Definir la case class de entrada**

```scala
import java.sql.Date

case class VentaRaw(
  id_venta: String,
  fecha: Date,
  id_cliente: String,
  cliente: String,
  pais: String,
  canal: String,
  categoria: String,
  producto: String,
  unidades: Int,
  precio_unitario: Double,
  descuento_pct: Double
)
```

---

## **8.2 Convertir a Dataset**

```scala

```

---

# **9. Parte 5 — Lógica de negocio segura con Scala**

## **9.1 Definir case class enriquecida**

Ahora crearemos una segunda case class con nuevas columnas de negocio.

```scala
case class VentaEnriquecida(
  id_venta: String,
  fecha: Date,
  id_cliente: String,
  cliente: String,
  pais: String,
  canal: String,
  categoria: String,
  producto: String,
  unidades: Int,
  precio_unitario: Double,
  descuento_pct: Double,
  importe_bruto: Double,
  importe_descuento: Double,
  importe_neto: Double,
  segmento_venta: String,
  requiere_revision: Boolean
)
```

---

## **9.2 Crear función Scala de negocio**

La empresa define estas reglas:

| **Regla** | **Resultado** |
| --- | --- |
| Importe neto >= 900 | Venta estratégica |
| Importe neto >= 300 | Venta media |
| Importe neto < 300 | Venta pequeña |
| Descuento > 12% y venta neta > 200 | Requiere revisión |

```scala

```

---

## **9.3 Aplicar lógica de negocio sobre Dataset**

```scala
val dsEnriquecido = dsVentas.map { v =>
  val importeBruto = v.unidades * v.precio_unitario
  val importeDescuento = importeBruto * (v.descuento_pct / 100.0)
  val importeNeto = importeBruto - importeDescuento

  VentaEnriquecida(
    id_venta = v.id_venta,
    fecha = v.fecha,
    id_cliente = v.id_cliente,
    cliente = v.cliente,
    pais = v.pais,
    canal = v.canal,
    categoria = v.categoria,
    producto = v.producto,
    unidades = v.unidades,
    precio_unitario = v.precio_unitario,
    descuento_pct = v.descuento_pct,
    importe_bruto = BigDecimal(importeBruto).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
    importe_descuento = BigDecimal(importeDescuento).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
    importe_neto = BigDecimal(importeNeto).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
    segmento_venta = clasificarVenta(importeNeto),
    requiere_revision = necesitaRevision(v.descuento_pct, importeNeto)
  )
}

println("=== Dataset enriquecido con lógica Scala ===")
dsEnriquecido.show(10, truncate = false)
```

---

## **9.4 Preguntas:**

1. ¿Por qué esta parte se ha hecho con Dataset y no directamente con DataFrame?
2. ¿Qué ventaja aporta la `case class VentaRaw`?
3. ¿Qué pasaría si escribimos mal `v.precio_unitario` dentro del `map`?

---

# **10. Parte 6 — Convertir Dataset enriquecido a DataFrame**

Para guardar, particionar, consultar y usar Spark SQL, volvemos a DataFrame.

```scala
// Convierte el datset a dataframe
```

---

# **11. Parte 7 — UDF para lógica no expresada fácilmente con funciones nativas**

## **11.1 Caso de negocio**

RetailNova quiere crear un código interno de riesgo comercial. La regla no es simplemente un `when`: combina país, canal, categoría, descuento e importe neto con una lógica propia.

Reglas:

| **Condición** | **Código de riesgo** |
| --- | --- |
| Marketplace + descuento >= 10 + importe neto > 300 | `RIESGO_MARKETPLACE_DESCUENTO` |
| País diferente de España + importe neto > 500 | `RIESGO_INTERNACIONAL_ALTO` |
| Categoría Informática + importe neto > 800 | `VENTA_TECNOLOGICA_CLAVE` |
| Resto de casos | `NORMAL` |

---

## **11.2 Crear la UDF**

```scala

```

---

## **11.3 Aplicar la UDF**

```scala

```

---

## **11.4 Nota didáctica sobre UDFs**

```
Usamos UDF solo cuando la lógica de negocio no se expresa cómodamente con funciones nativas.
Si puede hacerse con when, col, concat, year, month, lower, regexp_replace, etc., es mejor usar funciones nativas de Spark.
```

---

# **12. Parte 8 — Añadir columnas con funciones nativas de Spark SQL**

Ahora añadimos columnas derivadas con funciones nativas. Esta parte sí conviene hacerla con DataFrame.

```scala
val dfFinal = dfConRiesgo
  .withColumn("anio", year(col("fecha")))
  .withColumn("mes", month(col("fecha")))
  .withColumn("trimestre", quarter(col("fecha")))
  .withColumn("importe_neto_redondeado", round(col("importe_neto"), 2))
  .withColumn(
    "tipo_cliente",
    when(col("importe_neto") >= 900, "premium")
      .when(col("importe_neto") >= 300, "estandar")
      .otherwise("ocasional")
  )
  .withColumn(
    "venta_internacional",
    when(col("pais") =!= "España", true).otherwise(false)
  )

println("=== DataFrame final con columnas nativas Spark SQL ===")
dfFinal.show(10, truncate = false)
```

---

# **13. Parte 9 — Crear DataFrame final con menos columnas**

En una capa final no siempre se guardan todas las columnas. Se seleccionan las necesarias para análisis.

```scala
val dfCapaAnalitica = dfFinal.select(
  col("id_venta"),
  col("fecha"),
  col("anio"),
  col("mes"),
  col("trimestre"),
  col("id_cliente"),
  col("pais"),
  col("canal"),
  col("categoria"),
  col("producto"),
  col("unidades"),
  col("importe_bruto"),
  col("importe_descuento"),
  col("importe_neto_redondeado").as("importe_neto"),
  col("segmento_venta"),
  col("tipo_cliente"),
  col("venta_internacional"),
  col("requiere_revision"),
  col("riesgo_comercial")
)

println("=== Capa analítica final ===")
dfCapaAnalitica.show(10, truncate = false)

dfCapaAnalitica.printSchema()
```

---

# **14. Parte 10 — Guardar en Parquet particionado**

La empresa quiere guardar la capa final particionada por:

```
anio
mes
```

Esto facilitará consultas por periodos.

```scala

```

La estructura generada será parecida a:

```
parquet_final_ventas/
  anio=2022/
    mes=1/
    mes=2/
    ...
  anio=2023/
    mes=1/
    mes=2/
    ...
  anio=2024/
    mes=1/
    mes=2/
    ...
```

---

# **15. Parte 11 — Leer el Parquet final**

```scala
val dfParquetFinal = spark.read.parquet(rutaParquetFinal)

println("=== Parquet final leído ===")
dfParquetFinal.show(10, truncate = false)

dfParquetFinal.printSchema()
```

---

# **16. Parte 12 — Consultar con Spark SQL**

## **16.1 Crear vista temporal**

```scala

```

---

## **16.2 Consulta 1 — Ventas por año y mes**

```scala
spark.sql("""
  SELECT
    anio,
    mes,
    COUNT(*) AS total_ventas,
    ROUND(SUM(importe_neto), 2) AS facturacion_neta
  FROM ventas_retailnova
  GROUP BY anio, mes
  ORDER BY anio, mes
""").show(100, truncate = false)
```

---

## **16.3 Consulta 2 — Facturación por país**

```scala
spark.sql("""
  SELECT
    pais,
    COUNT(*) AS total_ventas,
    ROUND(SUM(importe_neto), 2) AS facturacion_neta,
    ROUND(AVG(importe_neto), 2) AS ticket_medio
  FROM ventas_retailnova
  GROUP BY pais
  ORDER BY facturacion_neta DESC
""").show(false)
```

---

## **16.4 Consulta 3 — Ventas que requieren revisión**

```scala
spark.sql("""
  SELECT
    id_venta,
    fecha,
    pais,
    canal,
    categoria,
    producto,
    importe_neto,
    descuento_pct,
    requiere_revision,
    riesgo_comercial
  FROM ventas_retailnova
  WHERE requiere_revision = true
     OR riesgo_comercial <> 'NORMAL'
  ORDER BY importe_neto DESC
""").show(100, truncate = false)
```

---

## **16.5 Consulta 4 — Ranking de categorías**

```scala
spark.sql("""
  SELECT
    categoria,
    COUNT(*) AS total_ventas,
    ROUND(SUM(importe_neto), 2) AS facturacion_neta
  FROM ventas_retailnova
  GROUP BY categoria
  ORDER BY facturacion_neta DESC
""").show(false)
```

---

# **17.  Comparar tamaño y tiempos de escritura**

Esta parte permite conectar el caso con la sesión de formatos.

## **17.1 Función para medir tiempo**

```scala
def medirTiempo[T](bloque: => T): (T, Long) = {
  val inicio = System.nanoTime()
  val resultado = bloque
  val fin = System.nanoTime()
  val tiempoMs = (fin - inicio) / 1000000
  (resultado, tiempoMs)
}
```

---

## **17.2 Función para calcular tamaño de carpeta**

```scala
def calcularTamanoBytes(path: String): Long = {
  val p = Paths.get(path)
  if (!Files.exists(p)) 0L
  else {
    Files.walk(p)
      .filter(Files.isRegularFile(_))
      .mapToLong(Files.size(_))
      .sum()
  }
}
```

---

## **17.3 Escribir en distintos formatos: AVRO, ORC y Parquet**

```scala

```

---

## **17.4 Medir tiempos de lectura**

```scala

```

---

## **17.5 Crear tabla comparativa**

```scala

```

---

# **18. Preguntas finales**

## **Preguntas técnicas**

1. ¿Por qué se leen los CSV primero como DataFrame?
2. ¿Por qué normalizamos tipos antes de convertir a Dataset?
3. ¿Qué aporta `case class VentaRaw`?
4. ¿Qué aporta `case class VentaEnriquecida`?
5. ¿Por qué volvemos de Dataset a DataFrame?
6. ¿Por qué guardamos el resultado final en Parquet y no en CSV?
7. ¿Qué ventaja tiene particionar por `anio` y `mes`?
8. ¿Qué ocurre si una consulta Spark SQL filtra por `anio = 2024`?

## **Preguntas de negocio**

1. ¿Qué país genera más facturación?
2. ¿Qué categoría tiene mayor facturación neta?
3. ¿Qué ventas deberían ser revisadas por el equipo comercial?
4. ¿Qué canal genera más ventas estratégicas?
5. ¿Qué meses tienen mayor volumen de ventas?

---

# **19. Flujo completo**

```
1. CSV anuales
   - ventas_2022.csv
   - ventas_2023.csv
   - ventas_2024.csv

2. DataFrame raw
   - lectura conjunta
   - inspección inicial

3. DataFrame normalizado
   - casteo de tipos
   - limpieza de texto
   - fecha como DateType

4. Dataset[VentaRaw]
   - seguridad de tipos
   - acceso a campos como propiedades Scala

5. Dataset[VentaEnriquecida]
   - cálculo de importe bruto
   - cálculo de descuento
   - cálculo de importe neto
   - clasificación de venta
   - revisión comercial

6. DataFrame enriquecido
   - uso de UDF
   - uso de funciones nativas Spark SQL

7. DataFrame final reducido
   - solo columnas analíticas necesarias

8. Parquet particionado
   - optimizado para lectura analítica
   - particionado por año y mes

9. Spark SQL
   - consultas de negocio
   - agregaciones
   - filtros
```

---

# **20. Para recordar**

> En un flujo real no usamos Dataset porque sí. Usamos Dataset cuando necesitamos lógica de negocio tipada y segura con Scala. Después volvemos a DataFrame porque Spark SQL, las escrituras particionadas y las consultas analíticas trabajan de forma más natural sobre DataFrames y tablas.
>