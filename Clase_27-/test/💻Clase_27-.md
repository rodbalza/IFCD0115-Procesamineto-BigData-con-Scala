# 💻Clase 27 -

---

# Agenda:

<aside>
💡

#### 9:00 - 9:50    →

#### 9:50 - 11:20   →

#### **11:20 - 11:40  →  Descanso**

#### 11:40 - 12:40  →

#### 12:40 - 14:00  →

</aside>

---

---

## 🗺️ ¿Qué vía seguir hoy?

Esta sesión introduce conceptos que viven de forma natural en **proyectos sbt estructurados**: organización de código, patrones de diseño, testing y logging. Las dos vías trabajan los mismos conceptos pero en entornos distintos.

|  | Vía 1 — IntelliJ + sbt | Vía 2 — Almond / Jupyter |
| --- | --- | --- |
| **Fichero de trabajo** | Proyecto `ventas-spark/` en sbt | `c27.ipynb` |
| **Cómo se ejecuta** | `sbt run` / `sbt test` en la sbt Shell | Celda a celda en el notebook |
| **Qué aporta** | Experiencia real de proyecto profesional | Exploración rápida de los patrones |
| **Entrega** | Carpeta del proyecto comprimida en `.zip` | `.ipynb` |

---

## 1. Estructura de proyectos Spark con sbt

---

Hasta ahora hemos trabajado en notebooks de Almond: ideales para exploración, análisis y aprendizaje. En un entorno profesional, las aplicaciones Spark se empaquetan como **proyectos sbt** que se compilan, testean y despliegan de forma reproducible.

La estructura estándar de un proyecto Spark con sbt es la siguiente:

```
ventas-spark/
│
├── build.sbt                          ← configuración del proyecto
├── project/
│   └── build.properties               ← versión de sbt
│
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   └── com/empresa/
│   │   │       ├── Main.scala                    ← punto de entrada
│   │   │       ├── jobs/
│   │   │       │   └── VentasJob.scala            ← orquesta el flujo completo
│   │   │       ├── transformations/
│   │   │       │   └── LimpiezaDatos.scala        ← lógica de transformación
│   │   │       └── utils/
│   │   │           └── SparkUtils.scala           ← helpers reutilizables
│   │   └── resources/
│   │       └── log4j2.properties                 ← configuración de logs
│   └── test/
│       └── scala/
│           └── com/empresa/
│               └── transformations/
│                   └── LimpiezaDatosSpec.scala    ← tests unitarios
│
└── data/
    └── ventas_test.csv                            ← datos de prueba
```

**¿Para qué sirve cada carpeta?**

| Carpeta | Propósito |
| --- | --- |
| `jobs/` | Clases que orquestan el flujo completo: leer → transformar → escribir |
| `transformations/` | Funciones puras que transforman DataFrames; las más fáciles de testear |
| `utils/` | Helpers reutilizables: creación de `SparkSession`, configuración, rutas |
| `resources/` | Ficheros de configuración incluidos en el JAR final |
| `test/` | Tests unitarios que verifican cada transformación de forma aislada |

> 💡 **Analogía:** piensa en la estructura como una cocina industrial. `Main` es el chef que coordina, `jobs` son los platos del menú, `transformations` son las recetas concretas, y `utils` son los utensilios compartidos.
> 

---

### 2. Gestión de dependencias en `build.sbt`

El archivo `build.sbt` es el corazón del proyecto. Declara versiones, dependencias y configuración de compilación.

```scala
// build.sbt — proyecto Spark 4.1.1 con Scala 2.13.18

ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version      := "1.0.0"
ThisBuild / organization := "com.empresa"

lazy val root = (project in file("."))
  .settings(
    name := "ventas-spark",

    libraryDependencies ++= Seq(
      // Spark: el operador %% resuelve automáticamente el sufijo _2.13
      "org.apache.spark" %% "spark-core" % "4.1.1" % "provided",
      "org.apache.spark" %% "spark-sql"  % "4.1.1" % "provided",

      // Logging
      "org.slf4j"                % "slf4j-api"         % "2.0.9",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0" % "runtime",

      // Testing — solo en el classpath de tests
      "org.scalatest"    %% "scalatest"  % "3.2.18" % Test,
      "org.apache.spark" %% "spark-core" % "4.1.1"  % Test,
      "org.apache.spark" %% "spark-sql"  % "4.1.1"  % Test
    )
  )
```

**Puntos clave:**

| Elemento | Significado |
| --- | --- |
| `%%` | sbt añade automáticamente el sufijo `_2.13`; nunca usar `%` con artefactos Spark |
| `"provided"` | Spark ya estará en el classpath del clúster; no se incluye en el JAR empaquetado |
| `% Test` | Dependencia disponible solo durante la ejecución de tests |
| `project/build.properties` | Contiene `sbt.version=1.10.1` |

> ⚠️ **Error frecuente:** usar `%` en lugar de `%%` para las dependencias de Spark devuelve `unresolved dependency`. El operador `%%` es el correcto para artefactos Scala que publican variantes por versión del lenguaje.
> 

---

### 3. Patrones de diseño en Spark con Scala 2.13

Los tres patrones más importantes en aplicaciones Spark son:

### 3.1 Objeto `SparkUtils` — creación centralizada de la sesión

En lugar de crear `SparkSession` en cada clase, se centraliza en un `object`:

```scala
package com.empresa.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def crearSesion(appName: String, master: String = "local[*]"): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

  def detenerSesion(spark: SparkSession): Unit = {
    spark.stop()
    println(s"Sesión '${spark.conf.get("spark.app.name")}' detenida.")
  }
}
```

> 💡 **¿Por qué un `object`?** En Scala, `object` es un singleton. Es el lugar natural para utilidades sin estado, equivalente a una clase de métodos estáticos en otros lenguajes.
> 

### 3.2 Patrón Pipeline — transformaciones encadenadas

Las transformaciones se definen como funciones `DataFrame => DataFrame`. Esto las hace **componibles** y **testeables** de forma independiente:

```scala
package com.empresa.transformations

import org.apache.spark.sql.{DataFrame, functions => F}

object LimpiezaDatos {

  def eliminarNulos(df: DataFrame): DataFrame =
    df.na.drop()

  def normalizarNombres(df: DataFrame): DataFrame =
    df.withColumn("nombre", F.trim(F.upper(F.col("nombre"))))

  def filtrarPrecioPositivo(df: DataFrame): DataFrame =
    df.filter(F.col("precio") > 0)

  // Pipeline: encadena las tres transformaciones
  def aplicarPipeline(df: DataFrame): DataFrame =
    filtrarPrecioPositivo(normalizarNombres(eliminarNulos(df)))
}
```

> 💡 **Analogía con la línea de montaje:** cada función es una estación. El DataFrame entra, se transforma y sale hacia la siguiente. Si una estación falla, sabemos exactamente cuál es.
> 

### 3.3 Patrón `Job` — orquestación del flujo completo

```scala
package com.empresa.jobs

import org.apache.spark.sql.SparkSession
import com.empresa.transformations.LimpiezaDatos
import org.slf4j.LoggerFactory

object VentasJob {

  private val logger = LoggerFactory.getLogger(getClass)

  def ejecutar(spark: SparkSession, rutaEntrada: String, rutaSalida: String): Unit = {
    logger.info(s"Leyendo datos de: $rutaEntrada")

    val dfRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(rutaEntrada)

    val dfLimpio = LimpiezaDatos.aplicarPipeline(dfRaw)

    logger.info(s"Registros tras limpieza: ${dfLimpio.count()}")
    dfLimpio.write.mode("overwrite").parquet(rutaSalida)
    logger.info(s"Resultados escritos en: $rutaSalida")
  }
}
```

---

### 4. Testing en Spark con ScalaTest

Los tests permiten verificar que las transformaciones funcionan correctamente antes de ejecutar el pipeline sobre datos reales:

```scala
package com.empresa.transformations

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class LimpiezaDatosSpec extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Tests-LimpiezaDatos")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  "eliminarNulos" should "reducir el número de filas cuando hay valores nulos" in {
    import spark.implicits._

    val dfEntrada = Seq(
      ("Laptop", 1200.0),
      (null,      300.0),
      ("Tablet",  null.asInstanceOf[Double])
    ).toDF("producto", "precio")

    val resultado = LimpiezaDatos.eliminarNulos(dfEntrada)

    resultado.count() shouldBe 1
    resultado.collect().head.getString(0) shouldBe "Laptop"
  }

  "filtrarPrecioPositivo" should "eliminar filas con precio menor o igual a cero" in {
    import spark.implicits._

    val dfEntrada = Seq(
      ("A", 100.0),
      ("B",   0.0),
      ("C",  -5.0)
    ).toDF("producto", "precio")

    LimpiezaDatos.filtrarPrecioPositivo(dfEntrada).count() shouldBe 1
  }
}
```

**Ejecutar los tests desde la sbt Shell de IntelliJ:**

```
sbt test
sbt "testOnly com.empresa.transformations.LimpiezaDatosSpec"
```

**Salida esperada:**

```
[info] LimpiezaDatosSpec:
[info]   eliminarNulos
[info]   - should reducir el número de filas cuando hay valores nulos
[info]   filtrarPrecioPositivo
[info]   - should eliminar filas con precio menor o igual a cero
[info] All tests passed.
```

---

### 5. Logging con slf4j y log4j2

Spark 4.x usa **log4j2**. El patrón estándar usa la fachada **slf4j**, que permite cambiar la implementación sin modificar el código de la aplicación.

```scala
import org.slf4j.LoggerFactory

object VentasJob {
  private val logger = LoggerFactory.getLogger(getClass)

  def ejecutar(...): Unit = {
    logger.info("Iniciando VentasJob")
    logger.debug(s"Ruta de entrada: $rutaEntrada")
    try {
      val df = spark.read.csv(rutaEntrada)
      logger.info(s"Datos cargados: ${df.count()} filas")
    } catch {
      case e: Exception =>
        logger.error(s"Error al leer datos: ${e.getMessage}", e)
        throw e
    }
  }
}
```

Fichero `src/main/resources/log4j2.properties`:

```
# Silenciar el ruido interno de Spark
rootLogger.level = WARN
rootLogger.appenderRef.stdout.ref = STDOUT

# Nuestra aplicación: nivel INFO
logger.app.name = com.empresa
logger.app.level = INFO
logger.app.additivity = false
logger.app.appenderRef.stdout.ref = STDOUT

appender.console.type = Console
appender.console.name = STDOUT
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{HH:mm:ss} [%-5level] %logger{36} - %msg%n
```

**Niveles de log:**

| Nivel | Cuándo usarlo |
| --- | --- |
| `DEBUG` | Detalle técnico para desarrollo; desactivar en producción |
| `INFO` | Progreso normal: inicio, fin, conteos |
| `WARN` | Situación inesperada pero recuperable |
| `ERROR` | Fallo que debe investigarse |

---

## 💻 Práctica

---

## 🟠 Vía 2 — Almond / Jupyter

> **Notebook:** `C:\Curso-Scala\notebooks\dia23_sesion2_buenas_practicas.ipynb`
Kernel: **Scala (almond · scala 2.13.18)**
> 

---

### 🔧 Celda 0 — Inicialización

> Ejecuta esta celda **antes que cualquier otra**. Espera el mensaje `✅`.
> 

```scala
// CELDA 0 — Inicialización
import $ivy.`org.apache.spark::spark-core:4.1.1`
import $ivy.`org.apache.spark::spark-sql:4.1.1`

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

val spark = SparkSession.builder()
  .appName("Dia23S2-BuenasPracticas")
  .master("local[*]")
  .config("spark.sql.shuffle.partitions", "4")
  .getOrCreate()

import spark.implicits._
spark.sparkContext.setLogLevel("ERROR")

println(s"✅ Spark ${spark.version} listo | Scala ${scala.util.Properties.versionString}")
```

**Salida esperada:**

```
✅ Spark 4.1.1 listo | Scala version 2.13.18
```

---

### 📂 Celda 1 — Crear datos de práctica

```scala
// CELDA 1 — Crear ficheros CSV
val carpeta = "C:/Curso-Scala/datos/dia23"
Files.createDirectories(Paths.get(carpeta))

val ventasCSV =
  """id,producto,categoria,precio,cantidad,ciudad
    |1,Laptop,Tecnología,1200.0,2,Madrid
    |2,Teclado,Tecnología,45.0,5,Valencia
    |3,Monitor,Tecnología,350.0,1,Madrid
    |4,Silla,Oficina,120.0,4,Barcelona
    |5,Mesa,Oficina,250.0,2,Sevilla
    |6,Laptop,Tecnología,1200.0,1,Madrid
    |7,Tablet,Tecnología,600.0,2,Bilbao
    |8,Ratón,Tecnología,25.0,10,Madrid
    |9,Impresora,Tecnología,200.0,1,Sevilla
    |10,Silla,Oficina,120.0,3,Madrid
    |11,Mesa,Oficina,250.0,1,Barcelona
    |12,Laptop,Tecnología,1200.0,1,Valencia
    |13,Monitor,Tecnología,350.0,2,Madrid
    |14,Tablet,Tecnología,600.0,1,Sevilla
    |15,Ratón,Tecnología,25.0,8,Bilbao""".stripMargin

val clientesCSV =
  """id,nombre,ciudad,segmento
    |,Ana García,Madrid,Premium
    |2,Pedro López,Valencia,Estándar
    |3,María Ruiz,Barcelona,Premium
    |4,,Sevilla,Estándar
    |5,Carlos Díaz,Bilbao,Premium
    |6,Laura Sanz,Madrid,Estándar
    |7,José Martín,Valencia,
    |8,Isabel Pérez,Madrid,Premium""".stripMargin

Files.write(Paths.get(s"$carpeta/ventas.csv"),   ventasCSV.getBytes(StandardCharsets.UTF_8))
Files.write(Paths.get(s"$carpeta/clientes.csv"), clientesCSV.getBytes(StandardCharsets.UTF_8))

println(s"✅ Ficheros creados en $carpeta")
```

**Salida esperada:**

```
✅ Ficheros creados en C:/Curso-Scala/datos/dia23
```

---

### 📝 P1 — Funciones de transformación reutilizables

> **Objetivo:** practicar el patrón `DataFrame => DataFrame` para crear transformaciones componibles.
> 

**Celda Markdown:**

```
## P1 — Funciones de transformación reutilizables
Cada función hace UNA sola cosa y recibe/devuelve un DataFrame.
```

**Celda de código:**

```scala
// P1 — Transformaciones como funciones DataFrame => DataFrame
val dfVentas = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("C:/Curso-Scala/datos/dia23/ventas.csv")

println("=== DataFrame original ===")
dfVentas.show(5)

def agregarImporte(df: DataFrame): DataFrame =
  df.withColumn("importe", col("precio") * col("cantidad"))

def soloTecnologia(df: DataFrame): DataFrame =
  df.filter(col("categoria") === "Tecnología")

def seleccionarColumnas(df: DataFrame): DataFrame =
  df.select("producto", "ciudad", "importe")

// Pipeline: encadenar las tres funciones
val dfResultado = seleccionarColumnas(soloTecnologia(agregarImporte(dfVentas)))

println("=== Resultado del pipeline ===")
dfResultado.show()
```

**Salida esperada:**

```
=== Resultado del pipeline ===
+----------+---------+-------+
|  producto|   ciudad|importe|
+----------+---------+-------+
|    Laptop|   Madrid| 2400.0|
|   Teclado| Valencia|  225.0|
|   Monitor|   Madrid|  350.0|
|    Laptop|   Madrid| 1200.0|
|    Tablet|   Bilbao| 1200.0|
|     Ratón|   Madrid|  250.0|
| Impresora|  Sevilla|  200.0|
|    Laptop| Valencia| 1200.0|
|   Monitor|   Madrid|  700.0|
|    Tablet|  Sevilla|  600.0|
|     Ratón|   Bilbao|  200.0|
+----------+---------+-------+
```

---

### 📝 P2 — Agrupar transformaciones en un `object`

> **Objetivo:** simular la estructura de un fichero `.scala` real agrupando funciones relacionadas en un `object`.
> 

**Celda Markdown:**

```
## P2 — Agrupando transformaciones en un object
En un proyecto sbt, estas funciones vivirían en LimpiezaDatos.scala.
En Almond, las agrupamos en un object en su propia celda.
```

**Celda de código — definición del object (celda separada):**

```scala
// P2a — Definir el object (celda propia)
object TransformacionesVentas {

  def agregarImporte(df: DataFrame): DataFrame =
    df.withColumn("importe", col("precio") * col("cantidad"))

  def filtrarCategoria(df: DataFrame, categoria: String): DataFrame =
    df.filter(col("categoria") === categoria)

  def agruparPorCiudad(df: DataFrame): DataFrame =
    df.groupBy("ciudad")
      .agg(
        sum("importe").as("total_ventas"),
        count("*").as("num_pedidos")
      )
      .orderBy(desc("total_ventas"))

  def pipeline(df: DataFrame, categoria: String): DataFrame =
    agruparPorCiudad(filtrarCategoria(agregarImporte(df), categoria))
}

println("✅ Object TransformacionesVentas definido")
```

**Celda de código — usar el object:**

```scala
// P2b — Usar el object
val dfVentas2 = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("C:/Curso-Scala/datos/dia23/ventas.csv")

println("=== Ventas de Tecnología por ciudad ===")
TransformacionesVentas.pipeline(dfVentas2, "Tecnología").show()

println("=== Ventas de Oficina por ciudad ===")
TransformacionesVentas.pipeline(dfVentas2, "Oficina").show()
```

**Salida esperada:**

```
=== Ventas de Tecnología por ciudad ===
+---------+------------+-----------+
|   ciudad|total_ventas|num_pedidos|
+---------+------------+-----------+
|   Madrid|      6475.0|          6|
| Valencia|      1425.0|          2|
|  Sevilla|       800.0|          2|
|   Bilbao|       800.0|          2|
+---------+------------+-----------+

=== Ventas de Oficina por ciudad ===
+---------+------------+-----------+
|   ciudad|total_ventas|num_pedidos|
+---------+------------+-----------+
|   Madrid|       720.0|          2|
|Barcelona|       500.0|          2|
|  Sevilla|       500.0|          1|
+---------+------------+-----------+
```

---

### 📝 P3 — Limpieza de datos: manejo de nulos

> **Objetivo:** explorar el dataset de clientes con nulos y aplicar las dos estrategias principales de limpieza.
> 

**Celda Markdown:**

```
## P3 — Limpieza de datos: manejo de nulos
El DataFrame de clientes tiene filas con valores nulos.
Exploraremos el problema y aplicaremos dos estrategias: eliminar y rellenar.
```

**Celda de código:**

```scala
// P3 — Detección y limpieza de nulos
val dfClientes = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("C:/Curso-Scala/datos/dia23/clientes.csv")

println("=== Clientes (con nulos) ===")
dfClientes.show()

println("=== Nulos por columna ===")
dfClientes.columns.foreach { nombreCol =>
  val nulos = dfClientes.filter(col(nombreCol).isNull).count()
  println(f"  $nombreCol%-12s → $nulos nulos")
}

// Estrategia 1: eliminar filas con cualquier nulo
val dfSinNulos = dfClientes.na.drop()
println(s"\nFilas tras eliminar nulos: ${dfSinNulos.count()} (de ${dfClientes.count()})")

// Estrategia 2: rellenar nulos con valores por defecto
val dfRelleno = dfClientes.na.fill(Map(
  "nombre"   -> "Desconocido",
  "segmento" -> "Estándar"
))
println("\n=== Clientes con nulos rellenados ===")
dfRelleno.show()
```

**Salida esperada:**

```
=== Nulos por columna ===
  id           → 1 nulos
  nombre       → 1 nulos
  ciudad       → 0 nulos
  segmento     → 1 nulos

Filas tras eliminar nulos: 5 (de 8)
```

---

### 📝 P4 — Simular logging con mensajes de progreso

> **Objetivo:** practicar el patrón de trazabilidad de un pipeline completo con mensajes de progreso estructurados.
> 

**Celda Markdown:**

```
## P4 — Logging manual con mensajes de progreso
En Almond simulamos el patrón de logging con funciones de ayuda.
En un proyecto sbt real usaríamos slf4j (ver Vía 1).
```

**Celda de código:**

```scala
// P4 — Simular logging de un pipeline completo
def logInfo(msg: String): Unit =
  println(s"[${java.time.LocalTime.now().toString.take(8)}] [INFO ] $msg")
def logWarn(msg: String): Unit =
  println(s"[${java.time.LocalTime.now().toString.take(8)}] [WARN ] $msg")

def ejecutarPipelineConLogs(rutaEntrada: String): Unit = {
  logInfo("=== Iniciando pipeline de ventas ===")
  logInfo(s"Leyendo datos de: $rutaEntrada")

  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(rutaEntrada)

  val totalOriginal = df.count()
  logInfo(s"Registros leídos: $totalOriginal")

  val nulos = df.filter(col("precio").isNull || col("cantidad").isNull).count()
  if (nulos > 0) logWarn(s"$nulos registros con precio o cantidad nulos")

  val dfLimpio = df.na.drop(Seq("precio", "cantidad"))
  logInfo(s"Registros tras limpieza: ${dfLimpio.count()} (eliminados: ${totalOriginal - dfLimpio.count()})")

  val dfConImporte = dfLimpio.withColumn("importe", col("precio") * col("cantidad"))
  val totalImporte = dfConImporte.agg(sum("importe")).collect()(0).getDouble(0)
  logInfo(f"Importe total: $totalImporte%.2f €")
  logInfo("=== Pipeline completado ===")

  dfConImporte.show(5)
}

ejecutarPipelineConLogs("C:/Curso-Scala/datos/dia23/ventas.csv")
```

**Salida esperada:**

```
[HH:MM:SS] [INFO ] === Iniciando pipeline de ventas ===
[HH:MM:SS] [INFO ] Leyendo datos de: C:/Curso-Scala/datos/dia23/ventas.csv
[HH:MM:SS] [INFO ] Registros leídos: 15
[HH:MM:SS] [INFO ] Registros tras limpieza: 15 (eliminados: 0)
[HH:MM:SS] [INFO ] Importe total: 14575.00 €
[HH:MM:SS] [INFO ] === Pipeline completado ===
```

---

### 📝 P5 — Función genérica de resumen estadístico

> **Objetivo:** demostrar que una función bien definida es reutilizable con cualquier DataFrame que tenga las columnas esperadas.
> 

**Celda Markdown:**

```
## P5 — Reutilización: el mismo pipeline para distintos datos
Una transformación no depende de los datos concretos, solo de la estructura.
```

**Celda de código:**

```scala
// P5 — Función genérica de resumen estadístico
def resumenPorGrupo(df: DataFrame, colValor: String, colGrupo: String): DataFrame =
  df.groupBy(colGrupo)
    .agg(
      count("*").as("n"),
      round(avg(colValor), 2).as("media"),
      round(min(colValor), 2).as("minimo"),
      round(max(colValor), 2).as("maximo"),
      round(sum(colValor), 2).as("total")
    )
    .orderBy(desc("total"))

val dfV = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("C:/Curso-Scala/datos/dia23/ventas.csv")
  .withColumn("importe", col("precio") * col("cantidad"))

println("=== Importes por ciudad ===")
resumenPorGrupo(dfV, "importe", "ciudad").show()

println("=== Precios por categoría ===")
resumenPorGrupo(dfV, "precio", "categoria").show()
```

**Salida esperada:**

```
=== Importes por ciudad ===
+---------+---+--------+------+-------+-------+
|   ciudad|  n|   media|minimo| maximo|  total|
+---------+---+--------+------+-------+-------+
|   Madrid|  7| 1210.71|  75.0| 2400.0| 8475.0|
| Valencia|  2|  712.50| 225.0| 1200.0| 1425.0|
|  Sevilla|  3|  533.33| 200.0|  600.0| 1600.0|
|   Bilbao|  2|  700.00| 200.0| 1200.0| 1400.0|
|Barcelona|  1|  500.00| 500.0|  500.0|  500.0|
+---------+---+--------+------+-------+-------+
```

---

### 📝 P6 — Ciclo completo: leer, transformar y escribir en Parquet

> **Objetivo:** completar el ciclo lectura → transformación → escritura en el formato estándar de producción.
> 

**Celda Markdown:**

```
## P6 — Ciclo completo con escritura en Parquet
Parquet es el formato de salida estándar en pipelines Spark:
columnar, comprimido y con tipos preservados.
```

**Celda de código:**

```scala
// P6 — Pipeline completo con escritura en Parquet
val rutaSalida = "C:/Curso-Scala/datos/dia23/resultado_ventas"

val dfFinal = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("C:/Curso-Scala/datos/dia23/ventas.csv")
  .withColumn("importe", col("precio") * col("cantidad"))
  .na.drop()

dfFinal.coalesce(1).write.mode("overwrite").parquet(rutaSalida)
println(s"✅ Resultado escrito en: $rutaSalida")

// Verificar la lectura del Parquet generado
val dfVerif = spark.read.parquet(rutaSalida)
println(s"Registros en Parquet: ${dfVerif.count()}")
dfVerif.printSchema()
dfVerif.show(5)
```

**Salida esperada:**

```
✅ Resultado escrito en: C:/Curso-Scala/datos/dia23/resultado_ventas
Registros en Parquet: 15
root
 |-- id: integer (nullable = true)
 |-- producto: string (nullable = true)
 |-- categoria: string (nullable = true)
 |-- precio: double (nullable = true)
 |-- cantidad: integer (nullable = true)
 |-- ciudad: string (nullable = true)
 |-- importe: double (nullable = true)
```

---

---

## 🔵 Vía 1 — IntelliJ IDEA + sbt

> **Proyecto:** `C:\Curso-Scala\proyectos\ventas-spark\`
Se trabaja en la **terminal integrada de IntelliJ** (`View → Tool Windows → Terminal`) y en el editor de código.
> 
> 
> ⚠️ **Antes de empezar:** IntelliJ debe tener instalado el plugin Scala y sbt disponible en el PATH. Comprueba con `sbt --version` en la terminal integrada.
> 

---

### 📝 P1 — Crear la estructura del proyecto

> **Objetivo:** generar el árbol de directorios estándar de un proyecto Spark con sbt.
> 

```powershell
# En la terminal integrada de IntelliJ
$base = "C:\Curso-Scala\proyectos\ventas-spark"

@(
  "src\main\scala\com\empresa\jobs",
  "src\main\scala\com\empresa\transformations",
  "src\main\scala\com\empresa\utils",
  "src\main\resources",
  "src\test\scala\com\empresa\transformations",
  "project",
  "data"
) | ForEach-Object {
  New-Item -ItemType Directory -Force -Path "$base\$_" | Out-Null
}

Write-Host "✅ Estructura creada en $base"
```

**Salida esperada:**

```
✅ Estructura creada en C:\Curso-Scala\proyectos\ventas-spark
```

---

### 📝 P2 — Crear `build.sbt` y `build.properties`

> **Objetivo:** configurar correctamente las dependencias con las versiones exactas del curso.
> 

```powershell
# build.sbt
@"
ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version      := "1.0.0"
ThisBuild / organization := "com.empresa"

lazy val root = (project in file("."))
  .settings(
    name := "ventas-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "4.1.1" % "provided",
      "org.apache.spark" %% "spark-sql"  % "4.1.1" % "provided",
      "org.slf4j"                % "slf4j-api"         % "2.0.9",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0" % "runtime",
      "org.scalatest"    %% "scalatest"  % "3.2.18" % Test,
      "org.apache.spark" %% "spark-core" % "4.1.1"  % Test,
      "org.apache.spark" %% "spark-sql"  % "4.1.1"  % Test
    )
  )
"@ | Set-Content "$base\build.sbt" -Encoding UTF8

# project/build.properties
"sbt.version=1.10.1" | Set-Content "$base\project\build.properties" -Encoding UTF8

Write-Host "✅ build.sbt y build.properties creados"
```

Una vez creados los ficheros, abre el proyecto en IntelliJ:

```
File → Open → selecciona la carpeta C:\Curso-Scala\proyectos\ventas-spark
→ Trust Project → importar como proyecto sbt
```

IntelliJ detectará `build.sbt` y sincronizará las dependencias automáticamente.

---

### 📝 P3 — Crear los datos de prueba

> **Objetivo:** generar el CSV que usarán tanto el job principal como los tests.
> 

```powershell
$csv = @"
id,producto,categoria,precio,cantidad,ciudad
1,Laptop,Tecnologia,1200.0,2,Madrid
2,Teclado,Tecnologia,45.0,5,Valencia
3,Monitor,Tecnologia,350.0,1,Madrid
4,Silla,Oficina,120.0,4,Barcelona
5,Mesa,Oficina,250.0,2,Sevilla
6,Laptop,Tecnologia,1200.0,1,Madrid
7,Tablet,Tecnologia,600.0,2,Bilbao
8,Raton,Tecnologia,25.0,10,Madrid
"@

$csv | Set-Content "$base\data\ventas_test.csv" -Encoding UTF8
Write-Host "✅ Datos de prueba creados en $base\data\ventas_test.csv"
```

> 💡 Los nombres sin tildes ni caracteres especiales evitan problemas de codificación en Windows.
> 

---

### 📝 P4 — Crear `LimpiezaDatos.scala`

> **Objetivo:** escribir el primer fichero `.scala` del proyecto con las transformaciones reutilizables.
> 

En IntelliJ, navega a `src/main/scala/com/empresa/transformations/`. Haz clic derecho → **New → Scala Class** → selecciona **Object** → nómbralo `LimpiezaDatos`. Escribe:

```scala
// src/main/scala/com/empresa/transformations/LimpiezaDatos.scala
package com.empresa.transformations

import org.apache.spark.sql.{DataFrame, functions => F}

object LimpiezaDatos {

  def eliminarNulos(df: DataFrame): DataFrame =
    df.na.drop()

  def agregarImporte(df: DataFrame): DataFrame =
    df.withColumn("importe", F.col("precio") * F.col("cantidad"))

  def filtrarCategoria(df: DataFrame, categoria: String): DataFrame =
    df.filter(F.col("categoria") === categoria)

  def pipeline(df: DataFrame): DataFrame =
    agregarImporte(eliminarNulos(df))
}
```

Compila para verificar que no hay errores. En la **sbt Shell** (`View → Tool Windows → sbt Shell`):

```
sbt compile
```

**Salida esperada:**

```
[info] Compiling 1 Scala source...
[success] Total time: XX s
```

> ⏳ La primera compilación descarga las dependencias de Spark (~400 MB). Las siguientes son inmediatas.
> 

---

### 📝 P5 — Crear `SparkUtils.scala`

> **Objetivo:** centralizar la creación de la `SparkSession` en un objeto singleton reutilizable.
> 

Crea `src/main/scala/com/empresa/utils/SparkUtils.scala`:

```scala
// src/main/scala/com/empresa/utils/SparkUtils.scala
package com.empresa.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def crearSesion(appName: String, master: String = "local[*]"): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
}
```

```
sbt compile
```

---

### 📝 P6 — Crear `VentasJob.scala` con logging real

> **Objetivo:** escribir el job principal usando `slf4j` para logging profesional.
> 

Crea `src/main/scala/com/empresa/jobs/VentasJob.scala`:

```scala
// src/main/scala/com/empresa/jobs/VentasJob.scala
package com.empresa.jobs

import org.apache.spark.sql.SparkSession
import com.empresa.transformations.LimpiezaDatos
import org.slf4j.LoggerFactory

object VentasJob {

  private val logger = LoggerFactory.getLogger(getClass)

  def ejecutar(spark: SparkSession, rutaEntrada: String, rutaSalida: String): Unit = {
    logger.info("=== Iniciando VentasJob ===")
    logger.info(s"Entrada: $rutaEntrada")

    val dfRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(rutaEntrada)

    logger.info(s"Registros leídos: ${dfRaw.count()}")

    val dfLimpio = LimpiezaDatos.pipeline(dfRaw)
    logger.info(s"Registros tras limpieza: ${dfLimpio.count()}")

    dfLimpio.write.mode("overwrite").parquet(rutaSalida)
    logger.info(s"Resultado escrito en: $rutaSalida")
    logger.info("=== VentasJob completado ===")
  }
}
```

---

### 📝 P7 — Crear `Main.scala` y ejecutar con `sbt run`

> **Objetivo:** crear el punto de entrada de la aplicación y ejecutarla de principio a fin.
> 

Crea `src/main/scala/com/empresa/Main.scala`:

```scala
// src/main/scala/com/empresa/Main.scala
package com.empresa

import com.empresa.jobs.VentasJob
import com.empresa.utils.SparkUtils

object Main extends App {

  val spark = SparkUtils.crearSesion("VentasApp")

  VentasJob.ejecutar(
    spark,
    rutaEntrada = "data/ventas_test.csv",
    rutaSalida  = "data/resultado_ventas"
  )

  spark.stop()
  println("✅ Aplicación finalizada correctamente")
}
```

En la sbt Shell:

```
sbt run
```

**Salida esperada:**

```
[HH:MM:SS] [INFO ] com.empresa.jobs.VentasJob - === Iniciando VentasJob ===
[HH:MM:SS] [INFO ] com.empresa.jobs.VentasJob - Entrada: data/ventas_test.csv
[HH:MM:SS] [INFO ] com.empresa.jobs.VentasJob - Registros leídos: 8
[HH:MM:SS] [INFO ] com.empresa.jobs.VentasJob - Registros tras limpieza: 8
[HH:MM:SS] [INFO ] com.empresa.jobs.VentasJob - Resultado escrito en: data/resultado_ventas
[HH:MM:SS] [INFO ] com.empresa.jobs.VentasJob - === VentasJob completado ===
✅ Aplicación finalizada correctamente
[success] Total time: XX s
```

---

### 📝 P8 — Escribir y ejecutar tests con ScalaTest

> **Objetivo:** escribir dos tests unitarios para las transformaciones y verificarlos con `sbt test`.
> 

Crea `src/test/scala/com/empresa/transformations/LimpiezaDatosSpec.scala`:

```scala
// src/test/scala/com/empresa/transformations/LimpiezaDatosSpec.scala
package com.empresa.transformations

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class LimpiezaDatosSpec extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Tests-LimpiezaDatos")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  "eliminarNulos" should "reducir el número de filas cuando hay valores nulos" in {
    import spark.implicits._

    val dfEntrada = Seq(
      ("Laptop", 1200.0),
      (null,      300.0),
      ("Tablet",  null.asInstanceOf[Double])
    ).toDF("producto", "precio")

    val resultado = LimpiezaDatos.eliminarNulos(dfEntrada)

    resultado.count() shouldBe 1
    resultado.collect().head.getString(0) shouldBe "Laptop"
  }

  "agregarImporte" should "calcular precio multiplicado por cantidad" in {
    import spark.implicits._

    val dfEntrada = Seq(
      ("Laptop", 1200.0, 2),
      ("Raton",    25.0, 4)
    ).toDF("producto", "precio", "cantidad")

    val resultado = LimpiezaDatos.agregarImporte(dfEntrada)
    val importes  = resultado.select("importe").collect().map(_.getDouble(0))

    importes shouldBe Array(2400.0, 100.0)
  }
}
```

En la sbt Shell:

```
sbt test
```

**Salida esperada:**

```
[info] LimpiezaDatosSpec:
[info]   eliminarNulos
[info]   - should reducir el número de filas cuando hay valores nulos
[info]   agregarImporte
[info]   - should calcular precio multiplicado por cantidad
[info] Tests: succeeded 2, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: XX s
```

---

---

### Vía 3 — Databricks

> **Versión Scala:** 2.13 | **Runtime Databricks:** 16.4 LTS (Spark 3.5.2 + Scala 2.13)
**Entorno:** Databricks o Azure Databricks
> 

---

> 💡 **Importante:** los ejercicios de esta vía son equivalentes en concepto a los de las Vías 1 y 2, pero el código se adapta a las convenciones de Databricks. No intentes copiar directamente el código de Almond a Databricks — las inicializaciones son distintas.
> 

---

---

## 🖥️ Configuración del clúster

> Solo es necesario hacerlo una vez. Si ya tienes un clúster activo del Runtime 16.4 LTS, omite este paso.
> 

### En Azure Databricks

1. En el menú izquierdo → **Compute** → **Create compute**
2. Configura:
    - **Cluster name:** `curso-scala-dia23`
    - **Databricks Runtime:** `16.4 LTS (Scala 2.13, Spark 3.5.2)`
    - **Worker type:** `Standard_DS3_v2` (o el que indique el formador)
    - **Min workers:** 1 | **Max workers:** 2
3. Haz clic en **Create compute**

> ✅ **Comprobación:** el Runtime 16.4 LTS garantiza Scala 2.13 — la misma versión del curso. Verás `Scala 2.13` junto al nombre del runtime en la lista.
> 

---

## 📓 Crear el notebook

1. En el menú izquierdo → **Workspace** → tu carpeta personal
2. Haz clic derecho → **Create** → **Notebook**
3. Configura:
    - **Name:** `dia23_sesion2_buenas_practicas`
    - **Default Language:** `Scala`
    - **Cluster:** selecciona `curso-scala-dia23`
4. Haz clic en **Create**

> 💡 En Databricks, el lenguaje por defecto del notebook se establece al crearlo. Puedes mezclar celdas de distintos lenguajes con directivas `%python`, `%sql`, `%sh`, pero en este notebook todo será Scala.
> 

---

## 💻 Práctica — Ejercicios P1–P6

> En Databricks **no hay celda de inicialización**. `spark` y `spark.implicits._` ya están disponibles en todas las celdas. Empieza directamente con P1.
> 

---

### 📂 Celda de datos — Crear los datos de práctica

> Ejecuta esta celda antes de los ejercicios. Crea los datos directamente en memoria — no necesitamos escribir ficheros en disco para los ejercicios principales.
> 

```scala
// Datos de práctica — en memoria
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

val ventasData = Seq(
  (1,  "Laptop",    "Tecnologia", 1200.0, 2, "Madrid"),
  (2,  "Teclado",   "Tecnologia",   45.0, 5, "Valencia"),
  (3,  "Monitor",   "Tecnologia",  350.0, 1, "Madrid"),
  (4,  "Silla",     "Oficina",     120.0, 4, "Barcelona"),
  (5,  "Mesa",      "Oficina",     250.0, 2, "Sevilla"),
  (6,  "Laptop",    "Tecnologia", 1200.0, 1, "Madrid"),
  (7,  "Tablet",    "Tecnologia",  600.0, 2, "Bilbao"),
  (8,  "Raton",     "Tecnologia",   25.0,10, "Madrid"),
  (9,  "Impresora", "Tecnologia",  200.0, 1, "Sevilla"),
  (10, "Silla",     "Oficina",     120.0, 3, "Madrid"),
  (11, "Mesa",      "Oficina",     250.0, 1, "Barcelona"),
  (12, "Laptop",    "Tecnologia", 1200.0, 1, "Valencia"),
  (13, "Monitor",   "Tecnologia",  350.0, 2, "Madrid"),
  (14, "Tablet",    "Tecnologia",  600.0, 1, "Sevilla"),
  (15, "Raton",     "Tecnologia",   25.0, 8, "Bilbao")
)

val dfVentas = ventasData.toDF("id", "producto", "categoria", "precio", "cantidad", "ciudad")

val clientesData = Seq(
  (Some(1), Some("Ana Garcia"),    "Madrid",    Some("Premium")),
  (Some(2), Some("Pedro Lopez"),   "Valencia",  Some("Estandar")),
  (Some(3), Some("Maria Ruiz"),    "Barcelona", Some("Premium")),
  (Some(4), None,                  "Sevilla",   Some("Estandar")),
  (Some(5), Some("Carlos Diaz"),   "Bilbao",    Some("Premium")),
  (Some(6), Some("Laura Sanz"),    "Madrid",    Some("Estandar")),
  (Some(7), Some("Jose Martin"),   "Valencia",  None),
  (Some(8), Some("Isabel Perez"),  "Madrid",    Some("Premium"))
)

val dfClientes = clientesData.toDF("id", "nombre", "ciudad", "segmento")

println(s"✅ Datos listos | Spark ${spark.version} | Scala ${scala.util.Properties.versionString}")
dfVentas.show(5)
```

**Salida esperada:**

```
✅ Datos listos | Spark 3.5.2 | Scala version 2.13.x
+---+--------+-----------+------+--------+---------+
| id|producto|  categoria|precio|cantidad|   ciudad|
+---+--------+-----------+------+--------+---------+
|  1|  Laptop| Tecnologia|1200.0|       2|   Madrid|
|  2| Teclado| Tecnologia|  45.0|       5| Valencia|
...
```

---

### 📝 P1 — Funciones de transformación reutilizables

> **Objetivo:** practicar el patrón `DataFrame => DataFrame` en el entorno Databricks.
> 

```scala
// P1 — Transformaciones como funciones DataFrame => DataFrame

def agregarImporte(df: DataFrame): DataFrame =
  df.withColumn("importe", col("precio") * col("cantidad"))

def soloTecnologia(df: DataFrame): DataFrame =
  df.filter(col("categoria") === "Tecnologia")

def seleccionarColumnas(df: DataFrame): DataFrame =
  df.select("producto", "ciudad", "importe")

// Pipeline encadenado
val dfResultado = seleccionarColumnas(soloTecnologia(agregarImporte(dfVentas)))

println("=== Resultado del pipeline ===")
dfResultado.show()
```

**Salida esperada:**

```
=== Resultado del pipeline ===
+----------+---------+-------+
|  producto|   ciudad|importe|
+----------+---------+-------+
|    Laptop|   Madrid| 2400.0|
|   Teclado| Valencia|  225.0|
|   Monitor|   Madrid|  350.0|
|    Laptop|   Madrid| 1200.0|
|    Tablet|   Bilbao| 1200.0|
|     Raton|   Madrid|  250.0|
| Impresora|  Sevilla|  200.0|
|    Laptop| Valencia| 1200.0|
|   Monitor|   Madrid|  700.0|
|    Tablet|  Sevilla|  600.0|
|     Raton|   Bilbao|  200.0|
+----------+---------+-------+
```

---

### 📝 P2 — Agrupar transformaciones en un `object`

> **Objetivo:** agrupar funciones relacionadas en un `object`, exactamente igual que en un fichero `.scala` de un proyecto sbt.
> 

```scala
// P2a — Definir el object (celda propia)
object TransformacionesVentas {

  def agregarImporte(df: DataFrame): DataFrame =
    df.withColumn("importe", col("precio") * col("cantidad"))

  def filtrarCategoria(df: DataFrame, categoria: String): DataFrame =
    df.filter(col("categoria") === categoria)

  def agruparPorCiudad(df: DataFrame): DataFrame =
    df.groupBy("ciudad")
      .agg(
        sum("importe").as("total_ventas"),
        count("*").as("num_pedidos")
      )
      .orderBy(desc("total_ventas"))

  def pipeline(df: DataFrame, categoria: String): DataFrame =
    agruparPorCiudad(filtrarCategoria(agregarImporte(df), categoria))
}

println("✅ Object TransformacionesVentas definido")
```

```scala
// P2b — Usar el object
println("=== Ventas de Tecnologia por ciudad ===")
TransformacionesVentas.pipeline(dfVentas, "Tecnologia").show()

println("=== Ventas de Oficina por ciudad ===")
TransformacionesVentas.pipeline(dfVentas, "Oficina").show()
```

**Salida esperada:**

```
=== Ventas de Tecnologia por ciudad ===
+---------+------------+-----------+
|   ciudad|total_ventas|num_pedidos|
+---------+------------+-----------+
|   Madrid|      6475.0|          6|
| Valencia|      1425.0|          2|
|  Sevilla|       800.0|          2|
|   Bilbao|       800.0|          2|
+---------+------------+-----------+

=== Ventas de Oficina por ciudad ===
+---------+------------+-----------+
|   ciudad|total_ventas|num_pedidos|
+---------+------------+-----------+
|   Madrid|       720.0|          2|
|Barcelona|       500.0|          2|
|  Sevilla|       500.0|          1|
+---------+------------+-----------+
```

---

### 📝 P3 — Limpieza de datos: manejo de nulos

> **Objetivo:** explorar el DataFrame de clientes con nulos y aplicar las dos estrategias de limpieza.
> 

```scala
// P3 — Detección y limpieza de nulos
println("=== Clientes (con nulos) ===")
dfClientes.show()

println("=== Nulos por columna ===")
dfClientes.columns.foreach { nombreCol =>
  val nulos = dfClientes.filter(col(nombreCol).isNull).count()
  println(f"  $nombreCol%-12s → $nulos nulos")
}

// Estrategia 1: eliminar filas con cualquier nulo
val dfSinNulos = dfClientes.na.drop()
println(s"\nFilas tras eliminar nulos: ${dfSinNulos.count()} (de ${dfClientes.count()})")

// Estrategia 2: rellenar nulos con valores por defecto
val dfRelleno = dfClientes.na.fill(Map(
  "nombre"   -> "Desconocido",
  "segmento" -> "Estandar"
))
println("\n=== Clientes con nulos rellenados ===")
dfRelleno.show()
```

**Salida esperada:**

```
=== Nulos por columna ===
  id           → 0 nulos
  nombre       → 1 nulos
  ciudad       → 0 nulos
  segmento     → 1 nulos

Filas tras eliminar nulos: 6 (de 8)
```

---

### 📝 P4 — Logging con `display()` y mensajes de progreso

> **Objetivo:** practicar el patrón de trazabilidad de un pipeline y aprovechar `display()`, la función de visualización nativa de Databricks que no existe en Almond ni IntelliJ.
> 

```scala
// P4 — Pipeline con logging y display() de Databricks

def logInfo(msg: String): Unit =
  println(s"[${java.time.LocalTime.now().toString.take(8)}] [INFO ] $msg")

def ejecutarPipelineConLogs(): Unit = {
  logInfo("=== Iniciando pipeline de ventas ===")
  logInfo(s"Registros originales: ${dfVentas.count()}")

  val nulos = dfVentas.filter(col("precio").isNull || col("cantidad").isNull).count()
  if (nulos > 0) println(s"[WARN ] $nulos registros con precio o cantidad nulos")

  val dfLimpio = dfVentas.na.drop(Seq("precio", "cantidad"))
  logInfo(s"Registros tras limpieza: ${dfLimpio.count()}")

  val dfConImporte = dfLimpio.withColumn("importe", col("precio") * col("cantidad"))
  val totalImporte = dfConImporte.agg(sum("importe")).collect()(0).getDouble(0)
  logInfo(f"Importe total calculado: $totalImporte%.2f €")
  logInfo("=== Pipeline completado ===")

  // display() renderiza el DataFrame como tabla interactiva en Databricks
  display(dfConImporte)
}

ejecutarPipelineConLogs()
```

**Salida esperada:**

```
[HH:MM:SS] [INFO ] === Iniciando pipeline de ventas ===
[HH:MM:SS] [INFO ] Registros originales: 15
[HH:MM:SS] [INFO ] Registros tras limpieza: 15
[HH:MM:SS] [INFO ] Importe total calculado: 14575.00 €
[HH:MM:SS] [INFO ] === Pipeline completado ===
```

*(Seguido de la tabla interactiva renderizada por `display()`)*

> 💡 **`display()` es exclusivo de Databricks.** Renderiza DataFrames como tablas interactivas con paginación, ordenación por columna y descarga a CSV. En Almond usamos `.show()`, en IntelliJ también. En Databricks puedes usar ambos, pero `display()` es la opción preferida para exploración.
> 

---

### 📝 P5 — Función genérica de resumen estadístico

> **Objetivo:** demostrar la reutilización de funciones y aprovechar `display()` para explorar los resultados visualmente.
> 

```scala
// P5 — Función genérica de resumen estadístico
def resumenPorGrupo(df: DataFrame, colValor: String, colGrupo: String): DataFrame =
  df.groupBy(colGrupo)
    .agg(
      count("*").as("n"),
      round(avg(colValor), 2).as("media"),
      round(min(colValor), 2).as("minimo"),
      round(max(colValor), 2).as("maximo"),
      round(sum(colValor), 2).as("total")
    )
    .orderBy(desc("total"))

val dfConImporte = dfVentas.withColumn("importe", col("precio") * col("cantidad"))

println("=== Importes por ciudad ===")
display(resumenPorGrupo(dfConImporte, "importe", "ciudad"))
```

```scala
// P5b — Mismo pipeline, distinta agrupación
println("=== Precios por categoría ===")
display(resumenPorGrupo(dfConImporte, "precio", "categoria"))
```

> 💡 En Databricks, después de `display()` puedes hacer clic en el icono de gráfico de barras bajo la tabla para ver una visualización automática sin escribir código adicional.
> 

---

### 📝 P6 — Escritura en Delta Lake

> **Objetivo:** completar el ciclo lectura → transformación → escritura usando **Delta Lake**, el formato nativo de Databricks, equivalente a Parquet pero con capacidades adicionales (transacciones ACID, historial de versiones).
> 

```scala
// P6 — Escritura en Delta Lake (formato nativo de Databricks)

val dfFinal = dfVentas
  .withColumn("importe", col("precio") * col("cantidad"))
  .na.drop()

// Ruta en DBFS (File System de Databricks) — válida en Community Edition
val rutaSalida = "/tmp/dia23/resultado_ventas_delta"

dfFinal
  .write
  .format("delta")          // Delta Lake en lugar de Parquet
  .mode("overwrite")
  .save(rutaSalida)

println(s"✅ Resultado escrito en: $rutaSalida")

// Verificar lectura
val dfVerif = spark.read.format("delta").load(rutaSalida)
println(s"Registros en Delta: ${dfVerif.count()}")
dfVerif.printSchema()
display(dfVerif)
```

**Salida esperada:**

```
✅ Resultado escrito en: /tmp/dia23/resultado_ventas_delta
Registros en Delta: 15
root
 |-- id: integer (nullable = false)
 |-- producto: string (nullable = true)
 |-- categoria: string (nullable = true)
 |-- precio: double (nullable = false)
 |-- cantidad: integer (nullable = false)
 |-- ciudad: string (nullable = true)
 |-- importe: double (nullable = false)
```

> 💡 **Delta Lake vs Parquet:** Delta Lake envuelve Parquet añadiendo un directorio `_delta_log/` con el registro de transacciones. En Databricks es el formato estándar de producción; en local (Vías 1 y 2) usamos Parquet directamente porque Delta requiere el conector adicional `delta-core`.
> 

---

## 🔍 Explorar el historial de versiones con Delta Lake

> Una de las ventajas únicas de Delta Lake es el **time travel**: puedes consultar versiones anteriores de los datos.
> 

```scala
// Bonus — Time travel con Delta Lake

// Escribir una segunda versión sobreescribiendo con menos filas
dfFinal
  .filter(col("ciudad") === "Madrid")
  .write
  .format("delta")
  .mode("overwrite")
  .save(rutaSalida)

println("=== Versión actual (solo Madrid) ===")
spark.read.format("delta").load(rutaSalida).show()

// Leer la versión 0 (la original con todos los datos)
println("=== Versión 0 (todos los registros) ===")
spark.read
  .format("delta")
  .option("versionAsOf", 0)
  .load(rutaSalida)
  .show()
```

**Salida esperada:**

```
=== Versión actual (solo Madrid) ===
(7 filas — solo registros de Madrid)

=== Versión 0 (todos los registros) ===
(15 filas — todos los registros originales)
```

---

---

## 🔄 Tabla de equivalencias de código

| Concepto | Vía 1 (IntelliJ) / Vía 2 (Almond) | Vía 3 (Databricks) |
| --- | --- | --- |
| Crear SparkSession | `SparkSession.builder()...getOrCreate()` | **No hace falta** — `spark` es implícito |
| Activar implicits | `import spark.implicits._` | **Ya activo** |
| Cargar dependencias | `import $ivy...` (Almond) / `build.sbt` (IntelliJ) | **No hace falta** — preinstalado |
| Silenciar logs | `spark.sparkContext.setLogLevel("ERROR")` | **No hace falta** |
| Mostrar DataFrame | `.show()` | `display()` (o `.show()`) |
| Ruta de ficheros | `C:/Curso-Scala/datos/...` | `/tmp/...` o `/Volumes/catalogo/schema/volumen/` |
| Formato de salida estándar | Parquet | **Delta Lake** |
| Versión de Spark | 4.1.1 | **3.5.2** (Runtime 16.4 LTS) |

---

## 💾 Guardar el trabajo

En Databricks, los notebooks se guardan automáticamente en el workspace. Para exportar una copia local:

```
File → Export → Source File (.scala)   ← código Scala puro
File → Export → IPython Notebook (.ipynb)   ← compatible con Jupyter
```

Para descargar los ficheros Delta generados en `/tmp/`:

```scala
// Descargar desde DBFS a la descarga del navegador (solo Community Edition)
// En Azure Databricks, usar Azure Storage Explorer o dbutils
dbutils.fs.ls("/tmp/dia23/")
```

---