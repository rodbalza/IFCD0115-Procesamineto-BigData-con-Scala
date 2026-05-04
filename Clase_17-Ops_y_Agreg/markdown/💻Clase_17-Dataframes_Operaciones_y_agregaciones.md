# 💻Clase 17 - Dataframes: Operaciones y agregaciones.

---

# Agenda:

<aside>
💡

#### 9:00 - 9:50    →  Sesión 1  - DataFrames: Operaciones básicas

#### 9:50 - 11:20   → Ejercicios y caso de uso

#### **11:20 - 11:40 → Descanso**

#### 11:40 - 12:40  → Sesión 2 - Agregaciones, agrupaciones, ventanas, rollup, cube y pivot

#### 12:40 - 14:00  → Ejercicios y caso de uso

</aside>

# Sesión 1 : Dataframes: Operaciones básicas

---

# 🧠 Parte 1 — Teoría

---

## 🔹 Operaciones clave

| Operación | Método | Descripción |
| --- | --- | --- |
| Selección | `select()` | Permite elegir qué columnas del DataFrame queremos conservar |
| Filtrado | `filter()` | Permite seleccionar filas que cumplen una condición |
| Nueva columna | `withColumn()` | Crea o modifica una columna a partir de una expresión |
| Renombrar | `withColumnRenamed()` | Cambia el nombre de una columna existente |
| Eliminar | `drop()` | Elimina una o varias columnas del DataFrame |
| Ordenar | `orderBy()` | Ordena los datos según una o varias columnas |
| Duplicados | `dropDuplicates()` | Elimina filas duplicadas (todas o por columnas específicas) |

---

## 🔍 Exploración

| Método | Uso |
| --- | --- |
| `show()` | Ver datos |
| `printSchema()` | Ver estructura |
| `describe()` | Estadísticas |
| `columns` | Nombres |
| `dtypes` | Tipos |

---

# 💻  Práctica

---

## 🔧 Inicialización

```scala
import $ivy.`org.apache.spark::spark-core:4.1.1`
import $ivy.`org.apache.spark::spark-sql:4.1.1`

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("Clase17_Sesion1_DataFrames")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._
spark.sparkContext.setLogLevel("ERROR")
```

---

## 📂 Dataset de práctica

```scala
val dfVentas = Seq(
(1,"Laptop","Tecnología",1200.0,2,"Madrid"),
(2,"Teclado","Tecnología",45.0,5,"Valencia"),
(3,"Monitor","Tecnología",350.0,1,"Madrid"),
(4,"Silla","Oficina",120.0,4,"Barcelona"),
(5,"Mesa","Oficina",250.0,2,"Sevilla"),
(6,"Laptop","Tecnología",1200.0,1,"Madrid"),
(7,"Tablet","Tecnología",600.0,2,"Bilbao"),
(8,"Ratón","Tecnología",25.0,10,"Madrid"),
(9,"Impresora","Tecnología",200.0,1,"Sevilla"),
(10,"Silla","Oficina",120.0,3,"Madrid"),
(11,"Mesa","Oficina",250.0,1,"Barcelona"),
(12,"Laptop","Tecnología",1200.0,1,"Valencia"),
(13,"Monitor","Tecnología",350.0,2,"Madrid"),
(14,"Tablet","Tecnología",600.0,1,"Sevilla"),
(15,"Ratón","Tecnología",25.0,8,"Bilbao"),
(16,"Teclado","Tecnología",45.0,6,"Madrid"),
(17,"Silla","Oficina",120.0,2,"Valencia"),
(18,"Mesa","Oficina",250.0,3,"Madrid"),
(19,"Laptop","Tecnología",1200.0,1,"Barcelona"),
(20,"Monitor","Tecnología",350.0,1,"Sevilla"),
(21,"Tablet","Tecnología",600.0,2,"Madrid"),
(22,"Ratón","Tecnología",25.0,12,"Madrid"),
(23,"Teclado","Tecnología",45.0,4,"Bilbao"),
(24,"Silla","Oficina",120.0,1,"Sevilla"),
(25,"Mesa","Oficina",250.0,2,"Madrid"),
(26,"Laptop","Tecnología",1200.0,3,"Madrid"),
(27,"Monitor","Tecnología",350.0,2,"Barcelona"),
(28,"Tablet","Tecnología",600.0,1,"Valencia"),
(29,"Ratón","Tecnología",25.0,15,"Madrid"),
(30,"Teclado","Tecnología",45.0,7,"Sevilla"),
(31,"Silla","Oficina",120.0,5,"Madrid"),
(32,"Mesa","Oficina",250.0,1,"Bilbao"),
(33,"Laptop","Tecnología",1200.0,2,"Sevilla"),
(34,"Monitor","Tecnología",350.0,3,"Madrid"),
(35,"Tablet","Tecnología",600.0,2,"Barcelona"),
(36,"Ratón","Tecnología",25.0,9,"Valencia"),
(37,"Teclado","Tecnología",45.0,6,"Madrid"),
(38,"Silla","Oficina",120.0,2,"Bilbao"),
(39,"Mesa","Oficina",250.0,2,"Madrid"),
(40,"Laptop","Tecnología",1200.0,1,"Madrid")
).toDF("id","producto","categoria","precio","cantidad","ciudad")
```

---

# 🧪 BLOQUE 1 — Exploración

```scala
dfVentas.show(5)
```

```scala
dfVentas.printSchema()
```

```scala
dfVentas.describe("precio","cantidad").show()
```

```scala
dfVentas.columns.foreach(println)
```

---

# 🧪 BLOQUE 2 — Operaciones básicas

### 1. Selección

```scala
dfVentas.select("producto","precio").show()
```

### 2. Filtrado

```scala
dfVentas.filter($"precio" > 500).show()
```

### 3. Nueva columna

```scala
val dfTotal = dfVentas.withColumn("total", $"precio" * $"cantidad")
```

### 4. Ordenación

```scala
dfTotal.orderBy($"total".desc).show()
```

---

# 🧪 Ejercicios basados en preguntas de negocio.

---

## 🔹 Ejercicio 1

👉 Mostrar solo ventas de Madrid

```scala
dfVentas.filter($"ciudad" === "Madrid").show()
```

---

## 🔹 Ejercicio 2

👉 Crear columna IVA (21%)

```scala
dfVentas.withColumn("precio_iva", $"precio" * 1.21).show()
```

---

## 🔹 Ejercicio 3

👉 Eliminar columna categoría

```scala
dfVentas.drop("categoria").show()
```

---

## 🔹 Ejercicio 4

👉 Renombrar precio

```scala
dfVentas.withColumnRenamed("precio","precio_unitario").show()
```

---

## 🔹 Ejercicio 5

👉 Quitar duplicados por producto

```scala
dfVentas.dropDuplicates("producto").show()
```

---

---

## 🔹 Ejercicio 6

👉 ¿Cuántas filas hay?

```scala
dfVentas.count()
```

---

## 🔹 Ejercicio 7

👉 ¿Qué columnas existen?

```scala
dfVentas.columns
```

---

## 🔹 Ejercicio 8

👉 Ver tipos

```scala
dfVentas.dtypes.foreach(println)
```

---

## 🔹 Ejercicio 9

👉 Estadísticas de precio

```scala
dfVentas.describe("precio").show()
```

---

# 🧪  PIPELINES

---

---

## 🔹 Ejercicio 10

👉 **Enunciado:** Queremos analizar las ventas de mayor valor económico. Construye un pipeline que:

1. Filtre solo los productos cuyo **precio sea mayor a 100**
2. Cree una nueva columna llamada **`total`** (precio × cantidad)
3. Seleccione únicamente las columnas:
    - producto
    - total
    - ciudad
4. Ordene los resultados de **mayor a menor total**

```scala
val ventasMayorValor = dfVentas
  .filter($"precio" > 100)
  .withColumn("total", $"precio" * $"cantidad")
  .select("producto", "total", "ciudad")
  .orderBy($"total".desc)

ventasMayorValor.show()
```

---

## 🔹 Ejercicio 11

👉 **Enunciado:** Queremos obtener una lista de productos únicos junto con su precio más representativo. Construye un pipeline que:

1. Elimine los duplicados basándose en la columna **producto**
2. Seleccione únicamente:
    - producto
    - precio
3. Ordene los resultados de **mayor a menor precio**

```scala
val productosUnicos = dfVentas
  .dropDuplicates("producto")
  .select("producto", "precio")
  .orderBy($"precio".desc)

productosUnicos.show()
```

---

# 🏢 Caso de Estudio — MobilityData Analytics

---

## Enunciado

Una empresa de movilidad urbana ha recopilado información de todos los viajes realizados durante el mes de **mayo** en distintas ciudades. El equipo de datos necesita preparar un análisis inicial que permita:

- Identificar los viajes que generan **mayor ingreso**
- Entender en qué ciudades se concentran estos viajes más rentables
- Generar un dataset limpio y ordenado para futuros análisis

---

A partir del dataset de viajes, construye un pipeline de datos que:

1. Calcule el **ingreso de cada viaje:**  (distancia × precio)
2. Filtre únicamente los viajes con **ingresos altos** (> 150)
3. Seleccione solo las columnas relevantes:
    - ciudad
    - ingreso_viaje
4. Ordene los resultados de **mayor a menor ingreso**

---

## 📥 Dataset

```scala
val viajes = Seq(
(1,"2026-05-01","Madrid",12.5,10.0),
(2,"2026-05-01","Barcelona",8.0,7.0),
(3,"2026-05-02","Madrid",15.0,12.0),
(4,"2026-05-02","Valencia",6.0,6.0),
(5,"2026-05-03","Sevilla",20.0,15.0),
(6,"2026-05-03","Madrid",5.0,5.0),
(7,"2026-05-04","Bilbao",18.0,13.0),
(8,"2026-05-04","Madrid",22.0,16.0),
(9,"2026-05-05","Barcelona",10.0,9.0),
(10,"2026-05-05","Madrid",14.0,11.0),
(11,"2026-05-06","Valencia",7.0,6.5),
(12,"2026-05-06","Madrid",16.0,13.0),
(13,"2026-05-07","Sevilla",25.0,18.0),
(14,"2026-05-07","Madrid",9.0,8.0),
(15,"2026-05-08","Barcelona",11.0,9.5),
(16,"2026-05-08","Bilbao",13.0,10.0),
(17,"2026-05-09","Madrid",17.0,14.0),
(18,"2026-05-09","Valencia",8.0,7.5),
(19,"2026-05-10","Sevilla",19.0,14.0),
(20,"2026-05-10","Madrid",21.0,17.0),
(21,"2026-05-11","Barcelona",9.0,8.0),
(22,"2026-05-11","Madrid",18.0,15.0),
(23,"2026-05-12","Valencia",6.5,6.0),
(24,"2026-05-12","Madrid",20.0,16.0),
(25,"2026-05-13","Bilbao",14.0,11.0),
(26,"2026-05-13","Madrid",23.0,18.0),
(27,"2026-05-14","Sevilla",18.0,13.0),
(28,"2026-05-14","Madrid",7.0,6.0),
(29,"2026-05-15","Barcelona",12.0,10.0),
(30,"2026-05-15","Madrid",19.0,15.0)
).toDF("id","fecha","ciudad","distancia_km","precio")
```

---

Queremos construir un nuevo dataset que muestre:

- La **ciudad**
- El **ingreso generado por cada viaje**

👉 Es decir, una tabla con esta estructura: ciudad | ingreso_viaje

---

Vamos a transformar el DataFrame paso a paso:

1. Crear una nueva columna llamada `ingreso_viaje`
    - Se calcula multiplicando la distancia por el precio
2. Filtrar solo los viajes con ingresos altos (> 150)
3. Quedarnos únicamente con las columnas relevantes:
    - ciudad
    - ingreso_viaje
4. Ordenar los resultados de mayor a menor ingreso

---

<aside>

Estamos construyendo un "pipeline de datos": cada operación transforma el DataFrame paso a paso.

</aside>

```scala
val viajesIngresosAltos = viajes
  .withColumn("ingreso_viaje", $"distancia_km" * $"precio")
  .filter($"ingreso_viaje" > 150)
  .select("ciudad", "ingreso_viaje")
  .orderBy($"ingreso_viaje".desc)

viajesIngresosAltos.show()
```

---

# 💡 Preguntas

### 🔹 Exploración

1. ¿Cuántos viajes hay?
2. ¿Qué columnas existen?
3. ¿Qué tipos tienen?

---

### 🔹 Transformación

1. ¿Cuántos viajes cumplen el filtro?
2. ¿Cuál tiene mayor ingreso?
3. Muestra solo viajes de Madrid

---

### 🔹 Manipulación

1. Elimina columna distancia
2. Renombra precio
3. Quita duplicados por ciudad

---

### 🔹 Análisis

1. Ordena por precio
2. Cambia filtro a >100
3. ¿Qué ciudad aparece más?

---

### 🔹 Guardar resultados

Guarda el resultado del pipeline anterior en un fichero `.csv`.

```scala
val resultadoViajes = viajes
  .withColumn("ingreso_viaje", $"distancia_km" * $"precio")
  .filter($"ingreso_viaje" > 150)
  .select("ciudad", "ingreso_viaje")
  .orderBy($"ingreso_viaje".desc)

resultadoViajes.write
  .mode("overwrite")
  .option("header", "true")
  .csv("C:/Curso-Scala/salidas/viajes_ingresos_altos")
```

### 🔹 Crear y guardar otro pipeline:

Crea un nuevo pipeline que haga lo siguiente:

1. Crear una columna llamada `precio_por_km`
    - Fórmula: `precio / distancia_km`
2. Filtrar solo los viajes donde `precio_por_km` sea mayor que `0.8`
3. Seleccionar solo las columnas:
    - `ciudad`
    - `fecha`
    - `precio_por_km`
4. Ordenar los resultados de mayor a menor `precio_por_km`
5. Guardar el resultado en formato CSV en la ruta:

```
C:/Curso-Scala/salidas/viajes_precio_por_km // Usa tu ruta
```

---

# Sesión 2 : Agregaciones, agrupaciones, ventanas, rollup, cube y pivot

---

---

Hasta ahora hemos trabajado con operaciones fila a fila:

- `select()`
- `filter()`
- `withColumn()`
- `drop()`
- `orderBy()`
- `dropDuplicates()`

> En esta clase damos un paso más: aprenderemos a **agrupar datos y calcular métricas.**
> 

---

---

# 🧠 Parte 1 — Teoría

```scala
import $ivy.`org.apache.spark::spark-core:4.1.1`
import $ivy.`org.apache.spark::spark-sql:4.1.1`

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val spark = SparkSession.builder()
  .appName("Teoria_Agregaciones_Dia15")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._
spark.sparkContext.setLogLevel("ERROR")
```

> En cada ejemplo se incluye un dataset pequeño para que puedas probar el código de forma independiente.
> 

---

## 1. ¿Qué es una agregación?

> Una **agregación** es una operación que resume varias filas en un resultado más pequeño. Por ejemplo, si tenemos varias ventas, podemos calcular:
> 
> - Cuántas ventas hubo por ciudad
> - Cuánto se vendió por categoría
> - Cuál fue la venta máxima
> - Cuál fue el promedio de venta

### Datos de entrada

```scala
val dfEjemplo = Seq(
  (1, "Madrid", "Tecnología", "Laptop", 2400.0),
  (2, "Madrid", "Tecnología", "Monitor", 700.0),
  (3, "Valencia", "Oficina", "Silla", 480.0),
  (4, "Valencia", "Tecnología", "Teclado", 270.0)
).toDF("id", "ciudad", "categoria", "producto", "total")

dfEjemplo.show()
```

### Salida esperada

```
+---+--------+----------+--------+------+
| id|  ciudad| categoria|producto| total|
+---+--------+----------+--------+------+
|  1|  Madrid|Tecnología|  Laptop|2400.0|
|  2|  Madrid|Tecnología| Monitor| 700.0|
|  3|Valencia|   Oficina|   Silla| 480.0|
|  4|Valencia|Tecnología| Teclado| 270.0|
+---+--------+----------+--------+------+
```

### Ejemplo de agregación conceptual

Queremos sumar el total vendido por ciudad:

```scala
dfEjemplo
  .groupBy("ciudad")
  .agg(sum("total").as("total_ciudad"))
  .show()
```

### Salida esperada

```
+--------+------------+
|  ciudad|total_ciudad|
+--------+------------+
|  Madrid|      3100.0|
|Valencia|       750.0|
+--------+------------+
```

---

## 2. `groupBy()`

El método `groupBy()` permite agrupar filas que tienen el mismo valor en una o varias columnas, `groupBy()` por sí solo no muestra resultados. Hay que añadir una agregación como `count()`, `sum()`, `avg()`, etc.

### Datos de entrada

```scala
val dfGroupBy = Seq(
  (1, "Madrid", "Laptop"),
  (2, "Madrid", "Monitor"),
  (3, "Valencia", "Silla"),
  (4, "Madrid", "Teclado"),
  (5, "Valencia", "Mesa")
).toDF("id", "ciudad", "producto")

dfGroupBy.show()
```

### Salida esperada del dataset

```
+---+--------+--------+
| id|  ciudad|producto|
+---+--------+--------+
|  1|  Madrid|  Laptop|
|  2|  Madrid| Monitor|
|  3|Valencia|   Silla|
|  4|  Madrid| Teclado|
|  5|Valencia|    Mesa|
+---+--------+--------+
```

### Ejemplo

```scala
dfGroupBy
  .groupBy("ciudad")
  .count()
  .show()
```

### Salida esperada

```
+--------+-----+
|  ciudad|count|
+--------+-----+
|  Madrid|    3|
|Valencia|    2|
+--------+-----+
```

---

## 3. Funciones de agregación principales

| Función | Qué hace | Ejemplo de uso |
| --- | --- | --- |
| `count()` | Cuenta filas | Número de ventas por ciudad |
| `sum()` | Suma valores | Total facturado |
| `avg()` | Calcula promedio | Ticket medio |
| `min()` | Obtiene el valor mínimo | Venta más baja |
| `max()` | Obtiene el valor máximo | Venta más alta |

Para usarlas necesitamos importar:

```scala
import org.apache.spark.sql.functions._
```

### Datos de entrada

```scala
val dfAgregaciones = Seq(
  (1, "Madrid", "Tecnología", 2400.0),
  (2, "Madrid", "Tecnología", 700.0),
  (3, "Madrid", "Oficina", 250.0),
  (4, "Valencia", "Oficina", 480.0),
  (5, "Valencia", "Tecnología", 270.0)
).toDF("id", "ciudad", "categoria", "total")

dfAgregaciones.show()
```

### Salida esperada del dataset

```
+---+--------+----------+------+
| id|  ciudad| categoria| total|
+---+--------+----------+------+
|  1|  Madrid|Tecnología|2400.0|
|  2|  Madrid|Tecnología| 700.0|
|  3|  Madrid|   Oficina| 250.0|
|  4|Valencia|   Oficina| 480.0|
|  5|Valencia|Tecnología| 270.0|
+---+--------+----------+------+
```

---

## 4. Agregaciones simples

### Ejemplo 1 — Contar registros por ciudad

```scala
dfAgregaciones
  .groupBy("ciudad")
  .count()
  .show()
```

### Salida esperada

```
+--------+-----+
|  ciudad|count|
+--------+-----+
|  Madrid|    3|
|Valencia|    2|
+--------+-----+
```

### Ejemplo 2 — Sumar ventas por ciudad

```scala
dfAgregaciones
  .groupBy("ciudad")
  .sum("total")
  .show()
```

### Salida esperada

```
+--------+----------+
|  ciudad|sum(total)|
+--------+----------+
|  Madrid|    3350.0|
|Valencia|     750.0|
+--------+----------+
```

### Ejemplo 3 — Calcular promedio por categoría

```scala
dfAgregaciones
  .groupBy("categoria")
  .avg("total")
  .show()
```

### Salida esperada

```
+----------+----------+
| categoria|avg(total)|
+----------+----------+
|Tecnología|    1123.3|
|   Oficina|     365.0|
+----------+----------+
```

> El promedio de Tecnología es aproximado: `(2400 + 700 + 270) / 3 = 1123.33...`.
> 

---

## 5. `agg()` — varias métricas a la vez

`agg()` permite calcular varias métricas dentro del mismo `groupBy()`.

### Datos de entrada

```scala
val dfAgg = Seq(
  (1, "Madrid", 2400.0),
  (2, "Madrid", 700.0),
  (3, "Madrid", 250.0),
  (4, "Valencia", 480.0),
  (5, "Valencia", 270.0)
).toDF("id", "ciudad", "total")

dfAgg.show()
```

### Salida esperada del dataset

```
+---+--------+------+
| id|  ciudad| total|
+---+--------+------+
|  1|  Madrid|2400.0|
|  2|  Madrid| 700.0|
|  3|  Madrid| 250.0|
|  4|Valencia| 480.0|
|  5|Valencia| 270.0|
+---+--------+------+
```

### Ejemplo

```scala
dfAgg
  .groupBy("ciudad")
  .agg(
    count("id").as("num_ventas"),
    sum("total").as("total_facturado"),
    avg("total").as("ticket_medio"),
    max("total").as("venta_maxima")
  )
  .show()
```

### Salida esperada

```
+--------+----------+---------------+------------------+------------+
|  ciudad|num_ventas|total_facturado|      ticket_medio|venta_maxima|
+--------+----------+---------------+------------------+------------+
|  Madrid|         3|         3350.0|1116.6666666666667|      2400.0|
|Valencia|         2|          750.0|             375.0|       480.0|
+--------+----------+---------------+------------------+------------+
```

### ¿Por qué es útil `agg()`?

Porque en un solo paso podemos construir una tabla resumen con varias métricas.

---

## 6. Alias con `.as()`

Cuando Spark calcula una agregación, genera nombres automáticos como:

```markdown
sum(total)
avg(total)
max(total)
```

Para que el resultado sea más claro, usamos `.as()`.

### Datos de entrada

```scala
val dfAlias = Seq(
  (1, "Madrid", 2400.0),
  (2, "Madrid", 700.0),
  (3, "Valencia", 480.0)
).toDF("id", "ciudad", "total")

dfAlias.show()
```

### Salida esperada del dataset

```
+---+--------+------+
| id|  ciudad| total|
+---+--------+------+
|  1|  Madrid|2400.0|
|  2|  Madrid| 700.0|
|  3|Valencia| 480.0|
+---+--------+------+
```

### Ejemplo sin alias

```scala
dfAlias
  .groupBy("ciudad")
  .sum("total")
  .show()
```

### Salida esperada

```
+--------+----------+
|  ciudad|sum(total)|
+--------+----------+
|  Madrid|    3100.0|
|Valencia|     480.0|
+--------+----------+
```

### Ejemplo con alias

```scala
dfAlias
  .groupBy("ciudad")
  .agg(sum("total").as("total_facturado"))
  .show()
```

### Salida esperada

```
+--------+---------------+
|  ciudad|total_facturado|
+--------+---------------+
|  Madrid|         3100.0|
|Valencia|          480.0|
+--------+---------------+
```

---

## 7. Funciones de ventana — Introducción

> Las funciones de ventana permiten calcular métricas **dentro de grupos**, pero sin reducir todas las filas a una sola fila.  Son muy útiles para rankings.
> 

Ejemplo:

> Queremos saber cuál es la venta más alta dentro de cada ciudad.
> 

Para esto usamos:

```scala
import org.apache.spark.sql.expressions.Window
```

Y funciones como:

| Función | Uso |
| --- | --- |
| `row_number()` | Asigna un número de fila dentro de cada grupo |
| `rank()` | Crea ranking con saltos si hay empates |
| `dense_rank()` | Crea ranking sin saltos |

---

## 8. Ventana con `partitionBy` y `orderBy`

### Datos de entrada para probar el ejemplo

```scala
val dfVentana = Seq(
  (1, "Madrid", "Laptop", 2400.0),
  (2, "Madrid", "Monitor", 700.0),
  (3, "Madrid", "Mesa", 250.0),
  (4, "Valencia", "Silla", 480.0),
  (5, "Valencia", "Teclado", 270.0)
).toDF("id", "ciudad", "producto", "total")

dfVentana.show()
```

### Salida esperada del dataset

```
+---+--------+--------+------+
| id|  ciudad|producto| total|
+---+--------+--------+------+
|  1|  Madrid|  Laptop|2400.0|
|  2|  Madrid| Monitor| 700.0|
|  3|  Madrid|    Mesa| 250.0|
|  4|Valencia|   Silla| 480.0|
|  5|Valencia| Teclado| 270.0|
+---+--------+--------+------+
```

### Ejemplo

```scala
val ventanaCiudad = Window
  .partitionBy("ciudad")
  .orderBy($"total".desc)

val rankingCiudad = dfVentana
  .withColumn("ranking_ciudad", row_number().over(ventanaCiudad))
  .orderBy($"ciudad", $"ranking_ciudad")

rankingCiudad.show()
```

### Salida esperada

```
+---+--------+--------+------+--------------+
| id|  ciudad|producto| total|ranking_ciudad|
+---+--------+--------+------+--------------+
|  1|  Madrid|  Laptop|2400.0|             1|
|  2|  Madrid| Monitor| 700.0|             2|
|  3|  Madrid|    Mesa| 250.0|             3|
|  4|Valencia|   Silla| 480.0|             1|
|  5|Valencia| Teclado| 270.0|             2|
+---+--------+--------+------+--------------+
```

### Explicación

- `partitionBy("ciudad")`: crea grupos por ciudad.
- `orderBy($"total".desc)`: ordena dentro de cada ciudad de mayor a menor total.
- `row_number()`: asigna el ranking dentro de cada ciudad.

---

<aside>

### Diferencia entre groupBy y partitionBy:

En Spark, `groupBy` y `partitionBy` pueden parecer parecidos porque ambos “agrupan” datos por una columna, pero **no hacen lo mismo**. 

#### 1. `groupBy`: agrupa y reduce filas

`groupBy` se usa cuando quieres **agrupar filas y calcular una agregación**, por ejemplo:

- sumar ventas por ciudad
- contar clientes por país
- calcular promedio de nota por estudiante
- obtener máximo o mínimo por categoría

Ejemplo:

Si tenemos estos datos:

| ciudad | vendedor | venta |
| --- | --- | --- |
| Madrid | Ana | 500 |
| Madrid | Luis | 400 |
| Madrid | Marta | 300 |
| Valencia | Pedro | 600 |
| Valencia | Laura | 200 |

```scala
val ventasPorCiudad = df
  .groupBy("ciudad")
  .sum("venta")

ventasPorCiudad.show()
```

El resultado de `groupBy("ciudad").sum("venta")` sería:

| ciudad | sum(venta) |
| --- | --- |
| Madrid | 1200 |
| Valencia | 800 |

Observa algo importante: **se pierden las filas individuales**. Ya no aparecen Ana, Luis, Marta, Pedro o Laura. El resultado queda reducido a una fila por ciudad. Por eso decimos que:

> groupBy agrupa filas y normalmente reduce el número de filas.
> 

#### 2. `partitionBy`: define grupos lógicos para funciones de ventana

`partitionBy` se usa dentro de una **ventana** para decirle a Spark:

> “Haz el cálculo por separado dentro de cada grupo, pero conserva todas las filas originales”.
> 

Ejemplo:

```scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val ventana = Window
  .partitionBy("ciudad")
  .orderBy(col("venta").desc)

val ranking = df
  .withColumn("ranking", rank().over(ventana))

ranking.show()
```

Resultado:

| ciudad | vendedor | venta | ranking |
| --- | --- | --- | --- |
| Madrid | Ana | 500 | 1 |
| Madrid | Luis | 400 | 2 |
| Madrid | Marta | 300 | 3 |
| Valencia | Pedro | 600 | 1 |
| Valencia | Laura | 200 | 2 |
</aside>

## 9. `rollup()` — subtotales jerárquicos

`rollup()` permite generar subtotales por niveles.

### Datos de entrada

```scala
val dfRollup = Seq(
  (1, "Madrid", "Tecnología", 2400.0),
  (2, "Madrid", "Tecnología", 700.0),
  (3, "Madrid", "Oficina", 250.0),
  (4, "Valencia", "Oficina", 480.0),
  (5, "Valencia", "Tecnología", 270.0)
).toDF("id", "ciudad", "categoria", "total")

dfRollup.show()
```

### Salida esperada del dataset

```
+---+--------+----------+------+
| id|  ciudad| categoria| total|
+---+--------+----------+------+
|  1|  Madrid|Tecnología|2400.0|
|  2|  Madrid|Tecnología| 700.0|
|  3|  Madrid|   Oficina| 250.0|
|  4|Valencia|   Oficina| 480.0|
|  5|Valencia|Tecnología| 270.0|
+---+--------+----------+------+
```

### Ejemplo

<aside>

Una empresa registra sus ventas por **ciudad** y **categoría de producto**.

Se desea analizar la facturación de forma jerárquica para obtener:

1. **El total facturado por cada combinación de ciudad y categoría**.
2. **El subtotal de facturación por ciudad**, sin distinguir categorías.
3. **El total general de facturación** considerando todos los registros.

Para ello, utiliza la función **`rollup("ciudad", "categoria")`** junto con una agregación sobre la columna **`total`**, de modo que el resultado muestre tanto los detalles como los subtotales y el total general.

</aside>

```scala
dfRollup
  .rollup("ciudad", "categoria")
  .agg(sum("total").as("total_facturado"))
  .orderBy($"ciudad", $"categoria")
  .show()
```

### Salida esperada

```
+--------+----------+---------------+
|  ciudad| categoria|total_facturado|
+--------+----------+---------------+
|    null|      null|         4100.0|
|  Madrid|      null|         3350.0|
|  Madrid|   Oficina|          250.0|
|  Madrid|Tecnología|         3100.0|
|Valencia|      null|          750.0|
|Valencia|   Oficina|          480.0|
|Valencia|Tecnología|          270.0|
+--------+----------+---------------+
```

| ciudad | categoria | total_facturado | Interpretación |
| --- | --- | --- | --- |
| `NULL` | `NULL` | `4100.0` | Total general de todas las ventas |
| `Madrid` | `NULL` | `3350.0` | Subtotal de todas las ventas de Madrid |
| `Madrid` | `Oficina` | `250.0` | Total de Madrid en la categoría Oficina |
| `Madrid` | `Tecnología` | `3100.0` | Total de Madrid en la categoría Tecnología |
| `Valencia` | `NULL` | `750.0` | Subtotal de todas las ventas de Valencia |
| `Valencia` | `Oficina` | `480.0` | Total de Valencia en la categoría Oficina |
| `Valencia` | `Tecnología` | `270.0` | Total de Valencia en la categoría Tecnología |

---

## 10. `cube()` — combinaciones multidimensionales

`cube()` genera combinaciones de agregaciones entre varias columnas.

### Datos de entrada

```scala
val dfCube = Seq(
  (1, "Madrid", "Tecnología", 2400.0),
  (2, "Madrid", "Tecnología", 700.0),
  (3, "Madrid", "Oficina", 250.0),
  (4, "Valencia", "Oficina", 480.0),
  (5, "Valencia", "Tecnología", 270.0)
).toDF("id", "ciudad", "categoria", "total")

dfCube.show()
```

### Salida esperada del dataset

```
+---+--------+----------+------+
| id|  ciudad| categoria| total|
+---+--------+----------+------+
|  1|  Madrid|Tecnología|2400.0|
|  2|  Madrid|Tecnología| 700.0|
|  3|  Madrid|   Oficina| 250.0|
|  4|Valencia|   Oficina| 480.0|
|  5|Valencia|Tecnología| 270.0|
+---+--------+----------+------+
```

### Ejemplo

<aside>

Una empresa registra sus ventas por **ciudad** y **categoría de producto**.

La dirección desea analizar la facturación desde diferentes perspectivas, no solo por la combinación ciudad-categoría, sino también por cada dimensión de forma independiente.

Se necesita calcular:

1. El **total facturado por cada combinación de ciudad y categoría**.
2. El **subtotal facturado por cada ciudad**, sin distinguir la categoría.
3. El **subtotal facturado por cada categoría**, sin distinguir la ciudad.
4. El **total general facturado** considerando todas las ciudades y todas las categorías.

Para resolver este análisis multidimensional, utiliza la función **`cube("ciudad", "categoria")`** junto con una agregación sobre la columna **`total`**. Esta función permitirá obtener todas las combinaciones posibles de agrupación entre las columnas seleccionadas.

</aside>

```scala
dfCube
  .cube("ciudad", "categoria")
  .agg(sum("total").as("total_facturado"))
  .orderBy($"ciudad", $"categoria")
  .show()
```

### Salida esperada

```
+--------+----------+---------------+
|  ciudad| categoria|total_facturado|
+--------+----------+---------------+
|    null|      null|         4100.0|
|    null|   Oficina|          730.0|
|    null|Tecnología|         3370.0|
|  Madrid|      null|         3350.0|
|  Madrid|   Oficina|          250.0|
|  Madrid|Tecnología|         3100.0|
|Valencia|      null|          750.0|
|Valencia|   Oficina|          480.0|
|Valencia|Tecnología|          270.0|
+--------+----------+---------------+
```

| ciudad | categoria | total_facturado | Interpretación |
| --- | --- | --- | --- |
| `NULL` | `NULL` | `4100.0` | Total general de todas las ventas |
| `NULL` | `Oficina` | `730.0` | Subtotal de la categoría Oficina en todas las ciudades |
| `NULL` | `Tecnología` | `3370.0` | Subtotal de la categoría Tecnología en todas las ciudades |
| `Madrid` | `NULL` | `3350.0` | Subtotal de todas las categorías en Madrid |
| `Madrid` | `Oficina` | `250.0` | Total de ventas de Oficina en Madrid |
| `Madrid` | `Tecnología` | `3100.0` | Total de ventas de Tecnología en Madrid |
| `Valencia` | `NULL` | `750.0` | Subtotal de todas las categorías en Valencia |
| `Valencia` | `Oficina` | `480.0` | Total de ventas de Oficina en Valencia |
| `Valencia` | `Tecnología` | `270.0` | Total de ventas de Tecnología en Valencia |

<aside>

## ¿Por qué aparecen valores `NULL`?

En esta salida, los valores `NULL` **no** indican datos faltantes en el DataFrame original. En operaciones como `cube()`, Spark usa `NULL` para representar que esa dimensión no se está usando en ese nivel de agregación.

Por ejemplo:

```
Madrid | NULL | 3350.0
```

significa:

```
Total facturado en Madrid, sin separar por categoría.
```

Es decir, Spark agrupa por `ciudad`, pero ignora temporalmente la columna `categoria`.

Otro ejemplo:

```
NULL | Oficina | 730.0
```

significa:

```
Total facturado en la categoría Oficina, sin separar por ciudad.
```

Aquí Spark agrupa por `categoria`, pero ignora temporalmente la columna `ciudad`.

Y esta fila:

```
NULL | NULL | 4100.0
```

significa:

```
Total general de todas las ventas.
```

Aquí Spark no agrupa ni por `ciudad` ni por `categoria`.

</aside>

<aside>

### Diferencia con `rollup()`

`rollup()` genera subtotales siguiendo una **jerarquía**.

Por ejemplo:

```
.rollup("ciudad","categoria")
```

calcula:

```
ciudad + categoria
ciudad
total general
```

En cambio, `cube()` calcula **todas las combinaciones posibles**:

```
ciudad + categoria
ciudad
categoria
total general
```

Por eso en `cube()` aparecen estas filas adicionales:

```
NULL | Oficina    | 730.0
NULL | Tecnología | 3370.0
```

Estas filas no aparecían en `rollup()` porque `rollup()` no calcula automáticamente el subtotal por `categoria` si la jerarquía definida es primero `ciudad` y luego `categoria`.

## Resumen

Con `cube("ciudad", "categoria")`, Spark responde varias preguntas al mismo tiempo:

```
¿Cuánto se facturó por cada ciudad y categoría?
¿Cuánto se facturó por cada ciudad?
¿Cuánto se facturó por cada categoría?
¿Cuánto se facturó en total?
```

Por tanto, `cube()` es útil cuando queremos hacer un análisis multidimensional parecido a una tabla dinámica o a un cubo OLAP.

</aside>

---

## 11. `pivot()` — tabla dinámica

`pivot()` convierte valores de una columna en columnas nuevas.

### Datos de entrada

```scala
val dfPivot = Seq(
  (1, "Madrid", "Tecnología", 2400.0),
  (2, "Madrid", "Tecnología", 700.0),
  (3, "Madrid", "Oficina", 250.0),
  (4, "Valencia", "Oficina", 480.0),
  (5, "Valencia", "Tecnología", 270.0)
).toDF("id", "ciudad", "categoria", "total")

dfPivot.show()
```

### Salida esperada del dataset

```
+---+--------+----------+------+
| id|  ciudad| categoria| total|
+---+--------+----------+------+
|  1|  Madrid|Tecnología|2400.0|
|  2|  Madrid|Tecnología| 700.0|
|  3|  Madrid|   Oficina| 250.0|
|  4|Valencia|   Oficina| 480.0|
|  5|Valencia|Tecnología| 270.0|
+---+--------+----------+------+
```

### Ejemplo

<aside>

> 
> 
> 
> Una empresa registra sus ventas indicando la **ciudad**, la **categoría del producto** y el **importe total de cada venta**. La dirección quiere transformar estos datos en una estructura similar a una **tabla dinámica**, donde cada ciudad aparezca en una fila y cada categoría de producto se convierta en una columna independiente.
> 
> Se necesita calcular:
> 
> 1. El **total facturado por ciudad**.
> 2. El **total facturado por cada categoría dentro de cada ciudad**.
> 3. Una tabla final donde las categorías, como `Oficina` y `Tecnología`, aparezcan como columnas.
> 
> Para resolverlo, utiliza `groupBy("ciudad")` para agrupar las ventas por ciudad y `pivot("categoria")` para convertir los valores de la columna `categoria` en nuevas columnas. Después, aplica `sum("total")` para calcular el total facturado en cada combinación ciudad-categoría.
> 
</aside>

```scala
dfPivot
  .groupBy("ciudad")
  .pivot("categoria")
  .agg(sum("total"))
  .orderBy("ciudad")
  .show()
```

### Salida esperada

```
+--------+-------+----------+
|  ciudad|Oficina|Tecnología|
+--------+-------+----------+
|  Madrid|  250.0|    3100.0|
|Valencia|  480.0|     270.0|
+--------+-------+----------+
```

### ¿Para qué sirve?

`pivot()` es útil cuando queremos construir una tabla parecida a una hoja de cálculo o una tabla dinámica de BI.

---

---

# 💻 Parte 2 — Práctica

---

## 🔧 Inicialización obligatoria

Ejecuta esta celda al inicio del notebook.

```scala
import $ivy.`org.apache.spark::spark-core:4.1.1`
import $ivy.`org.apache.spark::spark-sql:4.1.1`

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val spark = SparkSession.builder()
  .appName("Dia15_Sesion1_Agregaciones")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

spark.sparkContext.setLogLevel("ERROR")

println(s"✅ Spark ${spark.version} listo")
```

---

## 📦 Dataset de práctica — Ventas de mayo

Usaremos un dataset de ventas de una empresa de material tecnológico y oficina durante el mes de mayo.

```scala
val ventasMayo = Seq(
  (1, "2026-05-01", "Madrid", "Tecnología", "Laptop", 1200.0, 2),
  (2, "2026-05-01", "Barcelona", "Tecnología", "Monitor", 350.0, 1),
  (3, "2026-05-01", "Valencia", "Oficina", "Silla", 120.0, 4),
  (4, "2026-05-02", "Madrid", "Tecnología", "Teclado", 45.0, 6),
  (5, "2026-05-02", "Sevilla", "Oficina", "Mesa", 250.0, 2),
  (6, "2026-05-02", "Bilbao", "Tecnología", "Tablet", 600.0, 1),
  (7, "2026-05-03", "Madrid", "Tecnología", "Monitor", 350.0, 2),
  (8, "2026-05-03", "Barcelona", "Oficina", "Silla", 120.0, 3),
  (9, "2026-05-03", "Valencia", "Tecnología", "Ratón", 25.0, 10),
  (10, "2026-05-04", "Madrid", "Oficina", "Mesa", 250.0, 1),
  (11, "2026-05-04", "Sevilla", "Tecnología", "Laptop", 1200.0, 1),
  (12, "2026-05-04", "Bilbao", "Tecnología", "Teclado", 45.0, 5),
  (13, "2026-05-05", "Madrid", "Tecnología", "Tablet", 600.0, 2),
  (14, "2026-05-05", "Barcelona", "Tecnología", "Laptop", 1200.0, 1),
  (15, "2026-05-05", "Valencia", "Oficina", "Archivador", 80.0, 4),
  (16, "2026-05-06", "Madrid", "Tecnología", "Ratón", 25.0, 12),
  (17, "2026-05-06", "Sevilla", "Tecnología", "Monitor", 350.0, 1),
  (18, "2026-05-06", "Bilbao", "Oficina", "Silla", 120.0, 2),
  (19, "2026-05-07", "Madrid", "Oficina", "Silla", 120.0, 5),
  (20, "2026-05-07", "Barcelona", "Tecnología", "Tablet", 600.0, 2),
  (21, "2026-05-07", "Valencia", "Tecnología", "Teclado", 45.0, 7),
  (22, "2026-05-08", "Madrid", "Tecnología", "Laptop", 1200.0, 1),
  (23, "2026-05-08", "Sevilla", "Oficina", "Mesa", 250.0, 1),
  (24, "2026-05-08", "Bilbao", "Tecnología", "Monitor", 350.0, 2),
  (25, "2026-05-09", "Madrid", "Tecnología", "Monitor", 350.0, 3),
  (26, "2026-05-09", "Barcelona", "Tecnología", "Ratón", 25.0, 15),
  (27, "2026-05-09", "Valencia", "Oficina", "Mesa", 250.0, 2),
  (28, "2026-05-10", "Madrid", "Oficina", "Archivador", 80.0, 6),
  (29, "2026-05-10", "Sevilla", "Tecnología", "Tablet", 600.0, 1),
  (30, "2026-05-10", "Bilbao", "Tecnología", "Laptop", 1200.0, 1),
  (31, "2026-05-11", "Madrid", "Tecnología", "Teclado", 45.0, 8),
  (32, "2026-05-11", "Barcelona", "Oficina", "Mesa", 250.0, 1),
  (33, "2026-05-11", "Valencia", "Tecnología", "Monitor", 350.0, 1),
  (34, "2026-05-12", "Madrid", "Tecnología", "Laptop", 1200.0, 3),
  (35, "2026-05-12", "Sevilla", "Oficina", "Silla", 120.0, 4),
  (36, "2026-05-12", "Bilbao", "Tecnología", "Ratón", 25.0, 9),
  (37, "2026-05-13", "Madrid", "Oficina", "Mesa", 250.0, 2),
  (38, "2026-05-13", "Barcelona", "Tecnología", "Monitor", 350.0, 2),
  (39, "2026-05-13", "Valencia", "Tecnología", "Tablet", 600.0, 1),
  (40, "2026-05-14", "Madrid", "Tecnología", "Laptop", 1200.0, 2),
  (41, "2026-05-14", "Sevilla", "Tecnología", "Teclado", 45.0, 6),
  (42, "2026-05-14", "Bilbao", "Oficina", "Archivador", 80.0, 3),
  (43, "2026-05-15", "Madrid", "Tecnología", "Tablet", 600.0, 1),
  (44, "2026-05-15", "Barcelona", "Oficina", "Silla", 120.0, 2),
  (45, "2026-05-15", "Valencia", "Tecnología", "Laptop", 1200.0, 1)
).toDF("id", "fecha", "ciudad", "categoria", "producto", "precio_unitario", "cantidad")

val ventas = ventasMayo
  .withColumn("total", $"precio_unitario" * $"cantidad")

ventas.show(10)
```

---

## 🧪 Bloque 1 — Exploración inicial

### Ejercicio 1 — Ver primeras filas

```scala
ventas.show(5)
```

### Ejercicio 2 — Revisar schema

```scala
ventas.printSchema()
```

### Ejercicio 3 — Contar registros

```scala
ventas.count()
```

### Ejercicio 4 — Estadísticas básicas

```scala
ventas.describe("precio_unitario", "cantidad", "total").show()
```

### Preguntas rápidas

1. ¿Cuántas filas tiene el DataFrame?
2. ¿Qué tipo de dato tiene la columna `total`?
3. ¿Cuál es el valor máximo de `cantidad`?
4. ¿Cuál es el valor máximo de `total`?

---

## 🧪 Bloque 2 - `groupBy()` y `count()`

### Ejercicio 5 — Número de ventas por ciudad

```scala
val ventasPorCiudad = ventas
  .groupBy("ciudad")
  .count()
  .orderBy($"count".desc)

ventasPorCiudad.show()
```

### Ejercicio 6 — Número de ventas por categoría

```scala
val ventasPorCategoria = ventas
  .groupBy("categoria")
  .count()
  .orderBy($"count".desc)

ventasPorCategoria.show()
```

### Ejercicio 7 — Número de ventas por producto

```scala
val ventasPorProducto = ventas
  .groupBy("producto")
  .count()
  .orderBy($"count".desc)

ventasPorProducto.show()
```

### Preguntas

1. ¿Qué ciudad tiene más registros de venta?
2. ¿Qué categoría aparece más veces?
3. ¿Qué producto se vendió en más operaciones?

---

## 🧪 Bloque 3 — `sum`, `avg`, `min`, `max`

### Ejercicio 8 — Total facturado por ciudad

```scala
val facturacionPorCiudad = ventas
  .groupBy("ciudad")
  .agg(
    sum("total").as("total_facturado")
  )
  .orderBy($"total_facturado".desc)

facturacionPorCiudad.show()
```

### Ejercicio 9 — Total facturado por categoría

```scala
val facturacionPorCategoria = ventas
  .groupBy("categoria")
  .agg(
    sum("total").as("total_facturado")
  )
  .orderBy($"total_facturado".desc)

facturacionPorCategoria.show()
```

### Ejercicio 10 — Ticket medio por ciudad

```scala
val ticketMedioPorCiudad = ventas
  .groupBy("ciudad")
  .agg(
    avg("total").as("ticket_medio")
  )
  .orderBy($"ticket_medio".desc)

ticketMedioPorCiudad.show()
```

### Ejercicio 11 — Venta mínima y máxima por ciudad

```scala
val minMaxPorCiudad = ventas
  .groupBy("ciudad")
  .agg(
    min("total").as("venta_minima"),
    max("total").as("venta_maxima")
  )
  .orderBy($"venta_maxima".desc)

minMaxPorCiudad.show()
```

### Preguntas

1. ¿Qué ciudad factura más?
2. ¿Qué categoría tiene mayor facturación total?
3. ¿Qué ciudad tiene el ticket medio más alto?
4. ¿En qué ciudad se encuentra la venta máxima?

---

## 🧪 Bloque 4 — `agg()` con varias métricas

### Ejercicio 12 — Resumen completo por ciudad

```scala
val resumenCiudad = ventas
  .groupBy("ciudad")
  .agg(
    count("id").as("num_ventas"),
    sum("cantidad").as("unidades_vendidas"),
    sum("total").as("total_facturado"),
    avg("total").as("ticket_medio"),
    max("total").as("venta_maxima")
  )
  .orderBy($"total_facturado".desc)

resumenCiudad.show()
```

### Ejercicio 13 — Resumen completo por producto

```scala
val resumenProducto = ventas
  .groupBy("producto")
  .agg(
    count("id").as("num_ventas"),
    sum("cantidad").as("unidades_vendidas"),
    sum("total").as("total_facturado"),
    avg("total").as("ticket_medio"),
    max("total").as("venta_maxima")
  )
  .orderBy($"total_facturado".desc)

resumenProducto.show()
```

### Ejercicio 14 — Resumen por ciudad y categoría

```scala
val resumenCiudadCategoria = ventas
  .groupBy("ciudad", "categoria")
  .agg(
    count("id").as("num_ventas"),
    sum("total").as("total_facturado")
  )
  .orderBy($"ciudad", $"total_facturado".desc)

resumenCiudadCategoria.show()
```

### Preguntas

1. ¿Qué producto genera más facturación?
2. ¿Qué producto tiene más unidades vendidas?
3. ¿Qué combinación ciudad-categoría factura más?
4. ¿Por qué `agg()` es más práctico que hacer varias consultas separadas?

---

## 🧪 Bloque 5 — Ranking con funciones de ventana

### Ejercicio 15 — Ranking de ventas dentro de cada ciudad

```scala
val ventanaCiudad = Window
  .partitionBy("ciudad")
  .orderBy($"total".desc)

val rankingVentasCiudad = ventas
  .withColumn("ranking_ciudad", row_number().over(ventanaCiudad))
  .orderBy($"ciudad", $"ranking_ciudad")

rankingVentasCiudad.show(50)
```

### Ejercicio 16 — Top 1 venta de cada ciudad

```scala
val topVentaPorCiudad = rankingVentasCiudad
  .filter($"ranking_ciudad" === 1)
  .select("ciudad", "producto", "total", "ranking_ciudad")
  .orderBy($"total".desc)

topVentaPorCiudad.show()
```

### Ejercicio 17 — Top 3 ventas de cada ciudad

```scala
val top3VentasPorCiudad = rankingVentasCiudad
  .filter($"ranking_ciudad" <= 3)
  .select("ciudad", "producto", "total", "ranking_ciudad")
  .orderBy($"ciudad", $"ranking_ciudad")

top3VentasPorCiudad.show(50)
```

### Preguntas

1. ¿Qué venta ocupa el primer puesto en Madrid?
2. ¿Qué producto aparece más veces en los primeros puestos?
3. ¿Qué diferencia hay entre ordenar todo el DataFrame y ordenar dentro de cada ciudad?

---

## 🧪 Bloque 6 — `rollup()`

### Ejercicio 18 — Subtotales por ciudad y categoría

```scala
val rollupCiudadCategoria = ventas
  .rollup("ciudad", "categoria")
  .agg(
    sum("total").as("total_facturado")
  )
  .orderBy($"ciudad", $"categoria")

rollupCiudadCategoria.show(50)
```

### Preguntas

1. ¿Qué representan las filas donde `categoria` aparece como `null`?
2. ¿Qué representa la fila donde `ciudad` y `categoria` aparecen como `null`?
3. ¿Por qué puede ser útil `rollup()` para crear informes?

---

## 🧪 Bloque 7 — `cube()`

### Ejercicio 19 — Análisis multidimensional por ciudad y categoría

```scala
val cubeCiudadCategoria = ventas
  .cube("ciudad", "categoria")
  .agg(
    sum("total").as("total_facturado")
  )
  .orderBy($"ciudad", $"categoria")

cubeCiudadCategoria.show(50)
```

### Preguntas

1. ¿Qué diferencia observas entre `rollup()` y `cube()`?
2. ¿Qué representan las filas donde `ciudad` es `null` pero `categoria` tiene valor?
3. ¿Qué representa la fila donde ambas columnas son `null`?

---

## 🧪 Bloque 8 — `pivot()`

### Ejercicio 20 — Tabla dinámica de facturación por ciudad y categoría

```scala
val pivotCiudadCategoria = ventas
  .groupBy("ciudad")
  .pivot("categoria")
  .agg(sum("total"))
  .orderBy("ciudad")

pivotCiudadCategoria.show()
```

### Ejercicio 21 — Pivot por ciudad y producto

```scala
val pivotCiudadProducto = ventas
  .groupBy("ciudad")
  .pivot("producto")
  .agg(sum("total"))
  .orderBy("ciudad")

pivotCiudadProducto.show()
```

### Preguntas

1. ¿Qué ciudad factura más en Tecnología?
2. ¿Qué ciudad factura más en Oficina?
3. ¿Para qué tipo de informe sería útil un `pivot()`?
4. ¿Qué problema podría aparecer si una columna tiene demasiados valores distintos?

---

# 🏢 Caso de Estudio Propuesto — MarketNova Analytics

---

## 👉 Enunciado

**MarketNova** es una cadena de tiendas online que vende productos de distintas categorías. Durante el mes de mayo ha registrado ventas en varias ciudades españolas. El equipo directivo quiere un primer informe para responder preguntas como:

- Qué ciudad está generando más ingresos
- Qué categoría es más rentable
- Qué productos lideran las ventas
- Cuáles son las mejores ventas dentro de cada ciudad
- Cómo se distribuye la facturación por ciudad y categoría

Tu tarea como analista de datos es construir un análisis con DataFrames usando únicamente las herramientas vistas en esta sesión.

---

## 🔧 Inicialización

```scala
import $ivy.`org.apache.spark::spark-core:4.1.1`
import $ivy.`org.apache.spark::spark-sql:4.1.1`

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val spark = SparkSession.builder()
  .appName("MarketNova_Analytics")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._
spark.sparkContext.setLogLevel("ERROR")
```

---

## 📥 Dataset del caso de estudio

```scala
val marketNovaRaw = Seq(
  (1, "2026-05-01", "Madrid", "Electrónica", "Portátil", 950.0, 2),
  (2, "2026-05-01", "Madrid", "Hogar", "Aspiradora", 180.0, 1),
  (3, "2026-05-02", "Barcelona", "Electrónica", "Tablet", 420.0, 3),
  (4, "2026-05-02", "Valencia", "Deporte", "Bicicleta", 300.0, 2),
  (5, "2026-05-03", "Sevilla", "Hogar", "Cafetera", 90.0, 4),
  (6, "2026-05-03", "Bilbao", "Electrónica", "Auriculares", 75.0, 6),
  (7, "2026-05-04", "Madrid", "Deporte", "Cinta correr", 650.0, 1),
  (8, "2026-05-04", "Barcelona", "Hogar", "Microondas", 140.0, 2),
  (9, "2026-05-05", "Valencia", "Electrónica", "Monitor", 280.0, 2),
  (10, "2026-05-05", "Sevilla", "Deporte", "Mancuernas", 45.0, 8),
  (11, "2026-05-06", "Bilbao", "Hogar", "Batidora", 60.0, 5),
  (12, "2026-05-06", "Madrid", "Electrónica", "Smartphone", 700.0, 2),
  (13, "2026-05-07", "Barcelona", "Deporte", "Bicicleta", 300.0, 1),
  (14, "2026-05-07", "Valencia", "Hogar", "Cafetera", 90.0, 3),
  (15, "2026-05-08", "Sevilla", "Electrónica", "Tablet", 420.0, 2),
  (16, "2026-05-08", "Bilbao", "Deporte", "Balón", 25.0, 10),
  (17, "2026-05-09", "Madrid", "Hogar", "Microondas", 140.0, 3),
  (18, "2026-05-09", "Barcelona", "Electrónica", "Portátil", 950.0, 1),
  (19, "2026-05-10", "Valencia", "Deporte", "Cinta correr", 650.0, 1),
  (20, "2026-05-10", "Sevilla", "Hogar", "Aspiradora", 180.0, 2),
  (21, "2026-05-11", "Bilbao", "Electrónica", "Smartphone", 700.0, 1),
  (22, "2026-05-11", "Madrid", "Deporte", "Bicicleta", 300.0, 2),
  (23, "2026-05-12", "Barcelona", "Hogar", "Cafetera", 90.0, 5),
  (24, "2026-05-12", "Valencia", "Electrónica", "Auriculares", 75.0, 7),
  (25, "2026-05-13", "Sevilla", "Deporte", "Cinta correr", 650.0, 1),
  (26, "2026-05-13", "Bilbao", "Hogar", "Microondas", 140.0, 2),
  (27, "2026-05-14", "Madrid", "Electrónica", "Monitor", 280.0, 4),
  (28, "2026-05-14", "Barcelona", "Deporte", "Mancuernas", 45.0, 6),
  (29, "2026-05-15", "Valencia", "Hogar", "Aspiradora", 180.0, 2),
  (30, "2026-05-15", "Sevilla", "Electrónica", "Smartphone", 700.0, 1),
  (31, "2026-05-16", "Bilbao", "Deporte", "Bicicleta", 300.0, 2),
  (32, "2026-05-16", "Madrid", "Hogar", "Batidora", 60.0, 5),
  (33, "2026-05-17", "Barcelona", "Electrónica", "Monitor", 280.0, 3),
  (34, "2026-05-17", "Valencia", "Deporte", "Balón", 25.0, 12),
  (35, "2026-05-18", "Sevilla", "Hogar", "Microondas", 140.0, 2),
  (36, "2026-05-18", "Bilbao", "Electrónica", "Portátil", 950.0, 1)
).toDF("id", "fecha", "ciudad", "categoria", "producto", "precio_unitario", "cantidad")

val marketNova = marketNovaRaw
  .withColumn("total", $"precio_unitario" * $"cantidad")

marketNova.show(10)
```

---

## 🎯 Tareas del caso de estudio

---

### Pregunta 1 — Exploración inicial

Muestra las primeras 10 filas, el schema y las estadísticas de `precio_unitario`, `cantidad` y `total`.

```scala

```

---

### Pregunta 2 — Ventas por ciudad

Calcula cuántas ventas hay por ciudad y ordena de mayor a menor.

```scala

```

---

### Pregunta 3 — Facturación por ciudad

Calcula la facturación total por ciudad.

```scala

```

---

### Pregunta 4 — Facturación por categoría

Calcula la facturación total por categoría.

```scala

```

---

### Pregunta 5 — Resumen por producto

Para cada producto calcula:

- Número de ventas
- Unidades vendidas
- Total facturado
- Ticket medio
- Venta máxima

```scala

```

---

### Pregunta 6 — Resumen por ciudad y categoría

Calcula el total facturado por cada combinación de ciudad y categoría.

```scala

```

---

### Pregunta 7 — Ranking de ventas por ciudad

Crea un ranking de ventas dentro de cada ciudad, ordenando por `total` de mayor a menor.

```scala

```

---

### Pregunta 8 — Mejor venta de cada ciudad

A partir del ranking anterior, muestra solo la venta número 1 de cada ciudad.

```scala

```

---

### Pregunta 9 — Subtotales con rollup

Usa `rollup()` para obtener subtotales por ciudad y categoría.

```scala

```

---

### Pregunta 10 — Análisis multidimensional con cube

Usa `cube()` para obtener totales por ciudad, por categoría, por combinación ciudad-categoría y total general.

```scala

```

---

### Pregunta 11 — Tabla dinámica con pivot

Crea una tabla dinámica donde las filas sean ciudades y las columnas sean categorías.

```scala

```

---

### Pregunta 12 — Guardar resultados en CSV

Guarda el resumen por ciudad en formato CSV.

```scala

```

> Spark guarda el resultado como una carpeta que contiene varios archivos internos. La carpeta será:
> 
> 
> `C:/Curso-Scala/salidas/marketnova_facturacion_ciudad`
> 

---

### Pregunta 13 —

Crea un pipeline que:

1. Calcule el resumen por `categoria`
2. Incluya:
    - número de ventas
    - unidades vendidas
    - facturación total
    - ticket medio
3. Ordene por facturación total de mayor a menor
4. Guarde el resultado en CSV en la ruta:

```scala
C:/Curso-Scala/salidas/marketnova_resumen_categoria // Utiliza tu propia ruta
```

---