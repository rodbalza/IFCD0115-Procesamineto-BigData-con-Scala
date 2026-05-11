# Tutorial — IntelliJ + Delta Lake + Power BI Desktop

> **Curso:** IFCD0115 — Procesamiento Big Data con Scala
> **Entorno:** Scala 2.13.18 · Spark 4.1.1 · IntelliJ IDEA Community · sbt · Windows 11
> **Objetivo:** Escribir datos en formato Delta Lake desde IntelliJ usando Spark, y consumirlos directamente desde Power BI Desktop.

---

## Índice

1. [¿Qué es Delta Lake y por qué usarlo?](#1-qué-es-delta-lake-y-por-qué-usarlo)
2. [Flujo completo](#2-flujo-completo)
3. [Parte 1 — Configurar el proyecto en IntelliJ](#3-parte-1--configurar-el-proyecto-en-intellij)
4. [Parte 2 — Código Scala: crear y escribir en Delta Lake](#4-parte-2--código-scala-crear-y-escribir-en-delta-lake)
5. [Parte 3 — Ejecutar el proyecto desde IntelliJ](#5-parte-3--ejecutar-el-proyecto-desde-intellij)
6. [Parte 4 — Power BI lee el Delta Lake](#6-parte-4--power-bi-lee-el-delta-lake)
7. [Parte 5 — Construir el informe y el dashboard](#7-parte-5--construir-el-informe-y-el-dashboard)
8. [Qué ocurre cuando actualizas los datos](#8-qué-ocurre-cuando-actualizas-los-datos)
9. [Errores frecuentes](#9-errores-frecuentes)
10. [Resumen del flujo completo](#10-resumen-del-flujo-completo)

---

## 1. ¿Qué es Delta Lake y por qué usarlo?

Delta Lake es una capa de almacenamiento open source que se sitúa **encima de Parquet** y añade características de base de datos:

| Característica | CSV/Parquet plano | Delta Lake |
|---|---|---|
| Transacciones ACID | ❌ | ✅ |
| Historial de versiones (time travel) | ❌ | ✅ |
| Escrituras incrementales (`merge`, `update`, `delete`) | ❌ | ✅ |
| Schema enforcement automático | ❌ | ✅ |
| Compatible con Power BI | ✅ (CSV/Parquet) | ✅ (como Parquet) |
| Estándar en la industria Big Data | — | ✅ (Databricks, Azure, AWS) |

> 💡 **Analogía para principiantes:** si Parquet es como una hoja Excel muy eficiente, Delta Lake es como esa misma hoja pero con control de versiones tipo Git, transacciones y la posibilidad de deshacer cambios.

Un directorio Delta Lake tiene esta estructura en disco:

```
ventas_delta/
  ├── _delta_log/              ← registro de transacciones (JSON)
  │     ├── 00000000000000000000.json
  │     ├── 00000000000000000001.json
  │     └── ...
  ├── part-00000-xxxx.parquet  ← los datos reales (formato Parquet)
  ├── part-00001-xxxx.parquet
  └── ...
```

Power BI lee la carpeta Delta Lake exactamente igual que lee una carpeta Parquet, porque los datos **son** Parquet. El `_delta_log` simplemente se ignora.

---

## 2. Flujo completo

```
IntelliJ IDEA (Scala 2.13.18 + Spark 4.1.1)
    └── Proyecto sbt con dependencia delta-spark
    └── VentasDelta.scala
            └── Crea DataFrame de ventas
            └── Calcula métricas con Spark
            └── Escribe en Delta Lake
                    └── C:\Curso-Scala\delta\ventas_detalle\
                    └── C:\Curso-Scala\delta\resumen_ciudad\
                    └── C:\Curso-Scala\delta\resumen_categoria\
                    └── C:\Curso-Scala\delta\evolucion_mensual\
                            └── Power BI Desktop
                                    ├── Obtener datos → Carpeta (Parquet)
                                    ├── Informe con tablas y gráficos
                                    └── Dashboard interactivo
```

---

## 3. Parte 1 — Configurar el proyecto en IntelliJ

### 3.1 Crear el proyecto sbt

1. Abre **IntelliJ IDEA**.
2. En la pantalla de bienvenida: **New Project**.
3. En el panel izquierdo selecciona **Scala**.
4. A la derecha, elige **sbt**.
5. Rellena los campos:
   - **Name:** `ventas-delta`
   - **Location:** `C:\Curso-Scala\ventas-delta`
   - **JDK:** selecciona el JDK 17 que ya tienes instalado.
   - **sbt:** la versión que tengas instalada (1.9.x o superior).
   - **Scala:** `2.13.18`
6. Haz clic en **Create**.

IntelliJ creará la estructura de proyecto y descargará los resolvers de sbt (puede tardar 1-2 minutos la primera vez).

La estructura resultante es:

```
ventas-delta/
  ├── build.sbt                  ← configuración del proyecto (AQUÍ AÑADIREMOS DEPENDENCIAS)
  ├── project/
  │     └── build.properties     ← versión de sbt
  └── src/
        └── main/
              └── scala/         ← AQUÍ VA TODO EL CÓDIGO
```

---

### 3.2 Configurar `build.sbt`

Abre el archivo `build.sbt` que está en la raíz del proyecto. Verás algo parecido a esto por defecto:

```scala
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "ventas-delta"
  )
```

**Reemplaza todo el contenido** de `build.sbt` por el siguiente:

```scala
// Fichero: build.sbt  (raíz del proyecto)

ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "ventas-delta",

    // ── Dependencias ──────────────────────────────────────────────────────
    libraryDependencies ++= Seq(

      // Spark Core y SQL  (provided = ya está en el classpath de ejecución local)
      "org.apache.spark" %% "spark-core" % "4.1.1",
      "org.apache.spark" %% "spark-sql"  % "4.1.1",

      // Delta Lake para Spark 4.x / Scala 2.13
      "io.delta" %% "delta-spark" % "4.0.0"
    ),

    // ── Opciones del compilador ───────────────────────────────────────────
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked"
    ),

    // ── Evitar conflictos de dependencias transitivas ─────────────────────
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf"              => MergeStrategy.concat
      case x                             => MergeStrategy.first
    }
  )
```

> ⚠️ **Nota sobre versiones:** `delta-spark 4.0.0` es la versión compatible con Spark 4.x y Scala 2.13. Si usas Spark 3.5.x, la dependencia sería `delta-core 3.2.0`.

Después de guardar `build.sbt`, IntelliJ mostrará una notificación en la esquina superior derecha:

```
sbt project needs to be reloaded
[Load sbt changes]
```

Haz clic en **Load sbt changes**. sbt descargará Delta Lake y sus dependencias transitivas (~150 MB). Espera a que la barra de progreso inferior desaparezca.

---

### 3.3 Crear el archivo `plugin.sbt`

Delta Lake requiere el plugin `sbt-assembly` para empaquetar correctamente. Crea el archivo en la carpeta `project/`:

**Ruta:** `project/plugins.sbt`

```scala
// Fichero: project/plugins.sbt

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")
```

Guarda el archivo. IntelliJ te pedirá recargar el proyecto de nuevo → **Load sbt changes**.

---

### 3.4 Crear las carpetas de salida

Abre una terminal de PowerShell y ejecuta:

```powershell
# Crear las carpetas donde Delta Lake escribirá los datos
New-Item -ItemType Directory -Force -Path "C:\Curso-Scala\delta\ventas_detalle"
New-Item -ItemType Directory -Force -Path "C:\Curso-Scala\delta\resumen_ciudad"
New-Item -ItemType Directory -Force -Path "C:\Curso-Scala\delta\resumen_categoria"
New-Item -ItemType Directory -Force -Path "C:\Curso-Scala\delta\evolucion_mensual"

Write-Host "✅ Carpetas creadas en C:\Curso-Scala\delta\"
```

---

## 4. Parte 2 — Código Scala: crear y escribir en Delta Lake

### 4.1 Dónde crear el archivo

En IntelliJ, en el panel **Project** (izquierda):

1. Expande `ventas-delta → src → main → scala`.
2. Haz clic derecho sobre la carpeta `scala` → **New → Scala Class**.
3. En el diálogo, escribe el nombre: `VentasDelta` y selecciona tipo **Object**.
4. IntelliJ crea el archivo `src/main/scala/VentasDelta.scala`.

---

### 4.2 Código completo — `VentasDelta.scala`

Copia y pega el siguiente código en su totalidad dentro del archivo `VentasDelta.scala`:

```scala
// Fichero: src/main/scala/VentasDelta.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.delta.catalog.DeltaCatalog

object VentasDelta extends App {

  // ── 1. Crear SparkSession con extensión Delta Lake ──────────────────────
  //
  //  Delta Lake se integra con Spark mediante dos configuraciones:
  //    - spark.sql.extensions: activa el parser y las funciones Delta
  //    - spark.sql.catalog.spark_catalog: reemplaza el catálogo por defecto
  //      para soportar tablas Delta de forma nativa
  //
  val spark = SparkSession.builder()
    .appName("VentasDelta")
    .master("local[*]")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  println(s"✅ Spark ${spark.version} con Delta Lake listo")

  // ── 2. Dataset de ventas ────────────────────────────────────────────────
  //
  //  Mismo dataset que en el tutorial CSV: 30 registros, 6 meses,
  //  5 ciudades, 4 categorías. Tienda de material tecnológico.
  //
  val ventasRaw = Seq(
    (1,  "Laptop Pro",         "Tecnología",     1299.99, 2, "2024-01", "Madrid"),
    (2,  "Teclado Mecánico",   "Periféricos",      79.90, 5, "2024-01", "Madrid"),
    (3,  "Monitor 27\"",       "Tecnología",      349.00, 1, "2024-01", "Barcelona"),
    (4,  "Silla Ergonómica",   "Oficina",         320.00, 3, "2024-01", "Valencia"),
    (5,  "Ratón Inalámbrico",  "Periféricos",      29.95, 8, "2024-01", "Sevilla"),
    (6,  "Laptop Air",         "Tecnología",      999.00, 3, "2024-02", "Madrid"),
    (7,  "Auriculares BT",     "Audio",           149.00, 4, "2024-02", "Bilbao"),
    (8,  "Webcam HD",          "Periféricos",      79.00, 6, "2024-02", "Barcelona"),
    (9,  "Mesa Standing",      "Oficina",         450.00, 2, "2024-02", "Madrid"),
    (10, "SSD 1TB",            "Almacenamiento",  119.99, 5, "2024-02", "Sevilla"),
    (11, "Laptop Pro",         "Tecnología",     1299.99, 1, "2024-03", "Valencia"),
    (12, "Hub USB-C",          "Periféricos",      44.90, 7, "2024-03", "Madrid"),
    (13, "Monitor 32\"",       "Tecnología",      499.00, 2, "2024-03", "Bilbao"),
    (14, "Silla Gamer",        "Oficina",         280.00, 4, "2024-03", "Barcelona"),
    (15, "Micrófono USB",      "Audio",           109.00, 3, "2024-03", "Madrid"),
    (16, "Laptop Air",         "Tecnología",      999.00, 2, "2024-04", "Sevilla"),
    (17, "Teclado Mecánico",   "Periféricos",      79.90, 3, "2024-04", "Valencia"),
    (18, "SSD 2TB",            "Almacenamiento",  199.99, 4, "2024-04", "Madrid"),
    (19, "Auriculares BT",     "Audio",           149.00, 6, "2024-04", "Barcelona"),
    (20, "Ratón Vertical",     "Periféricos",      39.95, 5, "2024-04", "Bilbao"),
    (21, "Laptop Pro",         "Tecnología",     1299.99, 2, "2024-05", "Madrid"),
    (22, "Webcam 4K",          "Periféricos",     129.00, 4, "2024-05", "Sevilla"),
    (23, "Mesa Escritorio",    "Oficina",         380.00, 1, "2024-05", "Valencia"),
    (24, "SSD 1TB",            "Almacenamiento",  119.99, 3, "2024-05", "Madrid"),
    (25, "Monitor 27\"",       "Tecnología",      349.00, 2, "2024-05", "Barcelona"),
    (26, "Laptop Air",         "Tecnología",      999.00, 4, "2024-06", "Bilbao"),
    (27, "Hub USB-C",          "Periféricos",      44.90, 9, "2024-06", "Madrid"),
    (28, "Auriculares BT",     "Audio",           149.00, 5, "2024-06", "Sevilla"),
    (29, "Silla Ergonómica",   "Oficina",         320.00, 2, "2024-06", "Valencia"),
    (30, "SSD 2TB",            "Almacenamiento",  199.99, 3, "2024-06", "Barcelona")
  ).toDF("id", "producto", "categoria", "precio_unitario", "unidades", "mes", "ciudad")

  println(s"Dataset creado: ${ventasRaw.count()} filas")
  ventasRaw.show(5)

  // ── 3. Transformaciones Spark ───────────────────────────────────────────

  // Tabla de detalle: añadir columna de importe total
  val ventasDetalle = ventasRaw
    .withColumn("importe_total",
      round(col("precio_unitario") * col("unidades"), 2))

  // Tabla de resumen por ciudad
  val resumenCiudad = ventasDetalle
    .groupBy("ciudad")
    .agg(
      count("id")                    .alias("num_ventas"),
      sum("unidades")                .alias("total_unidades"),
      round(sum("importe_total"), 2) .alias("facturacion_total"),
      round(avg("importe_total"), 2) .alias("ticket_medio")
    )
    .orderBy(col("facturacion_total").desc)

  // Tabla de resumen por categoría
  val resumenCategoria = ventasDetalle
    .groupBy("categoria")
    .agg(
      count("id")                        .alias("num_ventas"),
      sum("unidades")                    .alias("total_unidades"),
      round(sum("importe_total"), 2)     .alias("facturacion_total"),
      round(avg("precio_unitario"), 2)   .alias("precio_medio")
    )
    .orderBy(col("facturacion_total").desc)

  // Tabla de evolución mensual
  val evolucionMensual = ventasDetalle
    .groupBy("mes")
    .agg(
      count("id")                    .alias("num_ventas"),
      round(sum("importe_total"), 2) .alias("facturacion_total")
    )
    .orderBy("mes")

  println("\n=== Resumen por ciudad ===")
  resumenCiudad.show()

  println("=== Resumen por categoría ===")
  resumenCategoria.show()

  println("=== Evolución mensual ===")
  evolucionMensual.show()

  // ── 4. Escribir en Delta Lake ───────────────────────────────────────────
  //
  //  .format("delta")  indica a Spark que use el writer de Delta Lake
  //  .mode("overwrite") sobrescribe si ya existe (ideal para desarrollo)
  //
  //  Delta Lake crea automáticamente la carpeta _delta_log con el
  //  registro de transacciones. Los datos se guardan como Parquet.
  //
  val rutaBase = "C:/Curso-Scala/delta"

  println("\nEscribiendo en Delta Lake...")

  // Tabla 1 — Detalle de ventas
  ventasDetalle
    .coalesce(1)                          // un solo fichero Parquet (más cómodo para Power BI)
    .write
    .format("delta")
    .mode("overwrite")
    .save(s"$rutaBase/ventas_detalle")

  println(s"  ✅ ventas_detalle  → $rutaBase/ventas_detalle")

  // Tabla 2 — Resumen por ciudad
  resumenCiudad
    .coalesce(1)
    .write
    .format("delta")
    .mode("overwrite")
    .save(s"$rutaBase/resumen_ciudad")

  println(s"  ✅ resumen_ciudad  → $rutaBase/resumen_ciudad")

  // Tabla 3 — Resumen por categoría
  resumenCategoria
    .coalesce(1)
    .write
    .format("delta")
    .mode("overwrite")
    .save(s"$rutaBase/resumen_categoria")

  println(s"  ✅ resumen_categoria → $rutaBase/resumen_categoria")

  // Tabla 4 — Evolución mensual
  evolucionMensual
    .coalesce(1)
    .write
    .format("delta")
    .mode("overwrite")
    .save(s"$rutaBase/evolucion_mensual")

  println(s"  ✅ evolucion_mensual → $rutaBase/evolucion_mensual")

  // ── 5. Verificar leyendo de vuelta desde Delta Lake ─────────────────────
  println("\n=== Verificación — leyendo desde Delta Lake ===")

  val verificacion = spark.read
    .format("delta")
    .load(s"$rutaBase/ventas_detalle")

  println(s"Filas en ventas_detalle (Delta): ${verificacion.count()}")
  verificacion.printSchema()

  // ── 6. Inspeccionar el historial de la tabla Delta ──────────────────────
  //
  //  DeltaTable.forPath muestra el log de transacciones.
  //  Cada vez que ejecutes el programa y sobreescribas, se añade una entrada.
  //
  import io.delta.tables.DeltaTable

  val deltaTable = DeltaTable.forPath(spark, s"$rutaBase/ventas_detalle")
  println("\n=== Historial de transacciones Delta Lake ===")
  deltaTable.history().select("version", "timestamp", "operation").show()

  // ── 7. Mostrar estructura de carpetas generada ──────────────────────────
  import java.io.File

  def listarCarpeta(ruta: String): Unit = {
    val carpeta = new File(ruta)
    if (carpeta.exists()) {
      println(s"\n📁 $ruta")
      carpeta.listFiles().sortBy(_.getName).foreach { f =>
        if (f.isDirectory) println(s"   📂 ${f.getName}/")
        else {
          val kb = f.length() / 1024.0
          println(f"   📄 ${f.getName}%-50s (${kb}%.1f KB)")
        }
      }
    }
  }

  println("\n=== Estructura generada en disco ===")
  listarCarpeta(s"$rutaBase/ventas_detalle")

  println("\n✅ Pipeline completado. Datos listos para Power BI.")
  spark.stop()
}
```

---

### 4.3 Puntos clave del código

| Línea / Sección | Explicación |
|---|---|
| `spark.sql.extensions = DeltaSparkSessionExtension` | Registra los comandos SQL propios de Delta (`DESCRIBE HISTORY`, `VACUUM`, etc.) |
| `spark.sql.catalog.spark_catalog = DeltaCatalog` | Permite referenciar tablas Delta por nombre en SQL sin especificar la ruta |
| `.format("delta")` en el write | Le dice a Spark que use el writer de Delta Lake en lugar del writer Parquet estándar |
| `.format("delta")` en el read | Usa el reader Delta, que consulta el `_delta_log` para saber qué ficheros son válidos |
| `DeltaTable.forPath(...)` | API de Delta para operaciones avanzadas: history, merge, vacuum, update, delete |
| `coalesce(1)` | Fuerza un único fichero Parquet por tabla; facilita la carga en Power BI |

---

## 5. Parte 3 — Ejecutar el proyecto desde IntelliJ

### 5.1 Configurar la Run Configuration

1. En IntelliJ, en el menú superior: **Run → Edit Configurations**.
2. Haz clic en `+` → **Application**.
3. Rellena:
   - **Name:** `VentasDelta`
   - **Main class:** `VentasDelta`
   - **Use classpath of module:** `ventas-delta`
   - **JVM options:** `-Xmx2g` (asigna 2 GB de heap a Spark; en equipos con más RAM puedes poner `-Xmx4g`)
4. Haz clic en **OK**.

### 5.2 Ejecutar

Haz clic en el botón ▶ verde (o `Shift+F10`).

La primera ejecución tarda más porque Spark inicializa el contexto. Las siguientes son más rápidas.

**Salida esperada en la consola de IntelliJ:**

```
✅ Spark 4.1.1 con Delta Lake listo
Dataset creado: 30 filas
+---+---------------+----------+---------------+--------+-------+---------+
| id|       producto| categoria|precio_unitario|unidades|    mes|   ciudad|
+---+---------------+----------+---------------+--------+-------+---------+
|  1|     Laptop Pro|Tecnología|        1299.99|       2|2024-01|   Madrid|
|  2|Teclado Mecánic|Periférico|          79.90|       5|2024-01|   Madrid|
|  3|    Monitor 27"|Tecnología|         349.00|       1|2024-01|Barcelona|
|  4|Silla Ergonómic|   Oficina|         320.00|       3|2024-01| Valencia|
|  5|Ratón Inalámbri|Periférico|          29.95|       8|2024-01|  Sevilla|
+---+---------------+----------+---------------+--------+-------+---------+

=== Resumen por ciudad ===
+---------+----------+--------------+-----------------+------------+
|   ciudad|num_ventas|total_unidades|facturacion_total|ticket_medio|
+---------+----------+--------------+-----------------+------------+
|   Madrid|        10|            44|         17007.52|     1700.75|
|Barcelona|         6|            18|          7117.67|     1186.28|
|   Bilbao|         4|            18|          6695.60|     1673.90|
| Valencia|         5|            13|          5199.67|     1039.93|
|  Sevilla|         5|            23|          4474.65|      894.93|
+---------+----------+--------------+-----------------+------------+

Escribiendo en Delta Lake...
  ✅ ventas_detalle    → C:/Curso-Scala/delta/ventas_detalle
  ✅ resumen_ciudad    → C:/Curso-Scala/delta/resumen_ciudad
  ✅ resumen_categoria → C:/Curso-Scala/delta/resumen_categoria
  ✅ evolucion_mensual → C:/Curso-Scala/delta/evolucion_mensual

=== Verificación — leyendo desde Delta Lake ===
Filas en ventas_detalle (Delta): 30

=== Historial de transacciones Delta Lake ===
+-------+-------------------+---------+
|version|          timestamp|operation|
+-------+-------------------+---------+
|      0|2024-xx-xx xx:xx:xx|    WRITE|
+-------+-------------------+---------+

=== Estructura generada en disco ===

📁 C:/Curso-Scala/delta/ventas_detalle
   📂 _delta_log/
   📄 part-00000-xxxxxxxxxxxx.parquet        (3.2 KB)

✅ Pipeline completado. Datos listos para Power BI.
```

### 5.3 Verificar en el Explorador de Windows

Navega a `C:\Curso-Scala\delta\`. Deberías ver:

```
delta/
  ├── ventas_detalle/
  │     ├── _delta_log/
  │     │     └── 00000000000000000000.json
  │     └── part-00000-xxxx.parquet
  ├── resumen_ciudad/
  │     ├── _delta_log/
  │     └── part-00000-xxxx.parquet
  ├── resumen_categoria/
  │     ├── _delta_log/
  │     └── part-00000-xxxx.parquet
  └── evolucion_mensual/
        ├── _delta_log/
        └── part-00000-xxxx.parquet
```

---

## 6. Parte 4 — Power BI lee el Delta Lake

### Por qué Power BI puede leer Delta Lake

Power BI no tiene un conector Delta Lake nativo en Power BI Desktop gratuito, pero **no lo necesita**: como los datos Delta se almacenan como ficheros Parquet estándar, Power BI los lee directamente como **carpeta Parquet**. El `_delta_log` simplemente se ignora durante la carga.

> 💡 **Importante:** si tienes Power BI Premium o Power BI Service con conectores adicionales, existe un conector Delta Lake nativo que aprovecha el time travel. En este tutorial usamos el método universal (carpeta Parquet) que funciona con Power BI Desktop gratuito.

### 6.1 Cargar `ventas_detalle`

1. Abre **Power BI Desktop**.
2. Cierra el diálogo de bienvenida si aparece.
3. En la cinta: **Inicio → Obtener datos → Más…**
4. En el buscador escribe `Parquet` → selecciona **Parquet** → **Conectar**.
5. En el campo de URL escribe la ruta del archivo Parquet (no la carpeta, el fichero concreto):

   ```
   C:\Curso-Scala\delta\ventas_detalle\part-00000-xxxx.parquet
   ```

   > Para encontrar el nombre exacto del fichero, abre el Explorador de Windows → navega a `C:\Curso-Scala\delta\ventas_detalle\` → copia el nombre del archivo `.parquet` que hay ahí.

6. Haz clic en **Aceptar**.
7. Aparece la previsualización de datos. Verifica que las columnas sean correctas.
8. Haz clic en **Transformar datos** para abrir Power Query.

### 6.2 En Power Query — Ajustar tipos

En el Editor de Power Query:

1. Selecciona la columna `id` → clic derecho → **Cambiar tipo → Número entero**.
2. Selecciona `precio_unitario` → **Número decimal**.
3. Selecciona `unidades` → **Número entero**.
4. Selecciona `importe_total` → **Número decimal**.
5. `mes` y `ciudad` y `categoria` → déjalos como **Texto**.

Renombra la consulta:
- Panel izquierdo "Consultas" → clic derecho → **Cambiar nombre** → `ventas_detalle`.

### 6.3 Cargar las otras tres tablas

Repite el proceso de **Inicio → Obtener datos → Parquet** para cada carpeta:

| Ruta del archivo `.parquet` | Nombre de consulta |
|---|---|
| `C:\Curso-Scala\delta\resumen_ciudad\part-00000-xxxx.parquet` | `resumen_ciudad` |
| `C:\Curso-Scala\delta\resumen_categoria\part-00000-xxxx.parquet` | `resumen_categoria` |
| `C:\Curso-Scala\delta\evolucion_mensual\part-00000-xxxx.parquet` | `evolucion_mensual` |

Para cada tabla cambia los tipos numéricos a `Número decimal` o `Número entero` según corresponda.

Cuando hayas añadido las cuatro consultas, haz clic en **Cerrar y aplicar**.

---

## 7. Parte 5 — Construir el informe y el dashboard

> La construcción del informe es idéntica al tutorial CSV. Se reproduce aquí completa para que el tutorial sea autosuficiente.

### Página 1 — Resumen Ejecutivo

#### Tarjetas KPI

1. En **Visualizaciones** (panel derecho) selecciona el icono **Tarjeta**.
2. Crea cuatro tarjetas con estos campos:

| Campo | Tabla | Agregación | Etiqueta |
|---|---|---|---|
| `facturacion_total` | `resumen_ciudad` | Suma | `Facturación Total` |
| `num_ventas` | `resumen_ciudad` | Suma | `Total Ventas` |
| `total_unidades` | `resumen_ciudad` | Suma | `Unidades Vendidas` |
| `ticket_medio` | `resumen_ciudad` | Promedio | `Ticket Medio` |

Para cambiar la etiqueta: selecciona la tarjeta → panel **Formato** (icono rodillo) → **Etiqueta de categoría** → escribe el nombre.

Coloca las cuatro tarjetas en fila en la parte superior de la página.

#### Gráfico de barras — Facturación por ciudad

1. Selecciona **Gráfico de barras agrupadas**.
2. Arrastra:
   - `ciudad` (tabla `resumen_ciudad`) → campo **Eje Y**
   - `facturacion_total` → campo **Valores**
3. Ordena: clic en `...` del gráfico → **Ordenar por → facturacion_total → Descendente**.
4. Título: `Facturación por Ciudad`.

#### Gráfico de anillos — Distribución por categoría

1. Selecciona **Gráfico de anillos**.
2. Arrastra:
   - `categoria` (tabla `resumen_categoria`) → **Leyenda**
   - `facturacion_total` → **Valores**
3. Título: `Distribución por Categoría`.

---

### Página 2 — Análisis Temporal

Haz clic en `+` en la barra inferior para añadir una nueva página. Renómbrala `Análisis Temporal`.

#### Gráfico de líneas — Evolución mensual

1. Selecciona **Gráfico de líneas**.
2. Arrastra:
   - `mes` (tabla `evolucion_mensual`) → **Eje X**
   - `facturacion_total` → **Valores**
   - `num_ventas` → **Información sobre herramientas**
3. Formato → **Marcadores** → activar.
4. Título: `Evolución de Facturación Mensual`.

#### Tabla detallada con barras condicionales

1. Selecciona la visualización **Tabla**.
2. Arrastra desde `evolucion_mensual`: `mes`, `num_ventas`, `facturacion_total`.
3. Formato → **Formato condicional** en `facturacion_total` → **Barras de datos** → activar.
4. Título: `Detalle Mensual`.

---

### Página 3 — Detalle de Ventas

Añade una tercera página. Renómbrala `Detalle de Ventas`.

#### Segmentadores (filtros interactivos)

1. Selecciona **Segmentación de datos** (icono de embudo).
2. Arrastra `ciudad` (tabla `ventas_detalle`) → **Campo**.
3. Formato → **Estilo → Mosaico** para mostrar botones.
4. Repite con `categoria`.

Coloca los dos segmentadores en la parte superior de la página.

#### Tabla de transacciones

1. Selecciona **Tabla**.
2. Arrastra desde `ventas_detalle`: `producto`, `categoria`, `ciudad`, `mes`, `unidades`, `precio_unitario`, `importe_total`.
3. Título: `Detalle de Transacciones`.

#### Barras apiladas — Categorías por ciudad

1. Selecciona **Gráfico de barras apiladas**.
2. Arrastra:
   - `ciudad` (tabla `ventas_detalle`) → **Eje Y**
   - `importe_total` → **Valores**
   - `categoria` → **Leyenda**
3. Título: `Composición por Ciudad y Categoría`.

---

### Página Dashboard

Añade una cuarta página. Renómbrala `Dashboard`. Muévela al principio arrastrando la pestaña.

#### Configurar el fondo

1. Haz clic en un área vacía del lienzo.
2. Panel **Formato** (sin elemento seleccionado) → **Fondo** → color `#1B2A3B` (azul oscuro corporativo).
3. **Tamaño de página** → `16:9`.

#### Añadir título

1. **Insertar → Cuadro de texto**.
2. Escribe: `Dashboard de Ventas — Tienda Tecnológica 2024`.
3. Fuente 22pt, negrita, color blanco. Centra el cuadro en la parte superior.

#### Layout del dashboard

Añade los siguientes elementos desde las páginas anteriores (puedes copiar y pegar con `Ctrl+C / Ctrl+V` entre páginas) o crear nuevas instancias:

```
┌────────────────────────────────────────────────────────────────┐
│  Dashboard de Ventas — Tienda Tecnológica 2024                 │
├──────────┬──────────┬──────────┬────────────────────────────── │
│  KPI     │  KPI     │  KPI     │  KPI                          │
│ Total €  │ Ventas   │ Unidades │ Ticket Medio                  │
├──────────┴──────────┴──────────┴───────────────┬───────────────│
│                                                │               │
│  Barras — Facturación por Ciudad               │  Anillo       │
│                                                │  Categorías   │
│                                                │               │
├────────────────────────────────────────────────┴───────────────│
│                                                                │
│  Líneas — Evolución Mensual de Facturación                     │
│                                                                │
├────────────────────────────────────────────────────────────────│
│  [Segmentador Ciudad]   [Segmentador Categoría]                │
└────────────────────────────────────────────────────────────────┘
```

**Pasos para alinear los KPIs:**
Selecciona las cuatro tarjetas con `Ctrl+clic` → clic derecho → **Alinear → Distribuir horizontalmente**.

**Añadir segmentadores al dashboard:**
Inserta dos segmentadores pequeños (ciudad, categoría) en la franja inferior del dashboard para filtrar todos los gráficos a la vez.

---

## 8. Qué ocurre cuando actualizas los datos

Una de las ventajas de Delta Lake sobre CSV es que puedes actualizar los datos de forma incremental. Aquí tienes cómo hacerlo y ver el impacto en Power BI.

### 8.1 Añadir nuevas ventas al código Scala

En `VentasDelta.scala`, añade al final del `Seq` de ventas (antes del `.toDF(...)`) filas nuevas correspondientes a julio 2024. Luego, en la sección de escritura, cambia `.mode("overwrite")` por `.mode("append")`:

```scala
// Cambiar en la sección de escritura para añadir datos sin borrar los anteriores:
ventasDetalle
  .coalesce(1)
  .write
  .format("delta")
  .mode("append")       // ← en lugar de "overwrite"
  .save(s"$rutaBase/ventas_detalle")
```

Vuelve a ejecutar desde IntelliJ (`Shift+F10`).

Delta Lake registrará una nueva transacción en `_delta_log`. Ahora el historial mostrará:

```
+-------+-------------------+---------+
|version|          timestamp|operation|
+-------+-------------------+---------+
|      1|2024-xx-xx xx:xx:xx|    WRITE|   ← append con nuevos datos
|      0|2024-xx-xx xx:xx:xx|    WRITE|   ← escritura inicial
+-------+-------------------+---------+
```

### 8.2 Actualizar Power BI

En Power BI Desktop: **Inicio → Actualizar**.

Todos los gráficos e informes se actualizan automáticamente con los nuevos datos, sin necesidad de volver a cargar los archivos.

---

## 9. Errores frecuentes

| Error | Causa | Solución |
|---|---|---|
| `ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension` | La dependencia `delta-spark` no se descargó correctamente | En IntelliJ: **View → Tool Windows → sbt** → botón de recarga (🔄). Verificar que `build.sbt` tiene la dependencia exacta y hacer `sbt update` |
| `java.lang.UnsatisfiedLinkError` al arrancar Spark en Windows | Falta `winutils.exe` en `C:\hadoop\bin` | Descargar `winutils.exe` compatible con Hadoop 3.x y colocarlo en `C:\hadoop\bin`. Añadir `HADOOP_HOME=C:\hadoop` a las variables de entorno |
| `AnalysisException: delta is not a built-in format` | SparkSession no tiene las extensiones Delta | Verificar que las dos líneas `.config("spark.sql.extensions", ...)` y `.config("spark.sql.catalog.spark_catalog", ...)` están en el builder |
| `FileAlreadyExistsException` | Intento de escribir con `errorIfExists` (modo por defecto) | Añadir `.mode("overwrite")` o `.mode("append")` en el write |
| Power BI muestra error al abrir el `.parquet` | Ruta con espacios o caracteres especiales | Usar la ruta `C:\Curso-Scala\delta\...` sin espacios. Si el nombre de usuario de Windows tiene espacio, mover el proyecto a `C:\delta\` |
| Columnas numéricas aparecen como `Any` en Power Query | Power Query no infirió el tipo automáticamente | Clic derecho en la columna → Cambiar tipo → Número decimal / entero |
| `_delta_log` aparece como tabla en Power BI | Se seleccionó la carpeta raíz en lugar del archivo `.parquet` | Navegar dentro de la carpeta y seleccionar el archivo `part-00000-xxxx.parquet` directamente |
| `OutOfMemoryError` al ejecutar en IntelliJ | Heap de la JVM demasiado pequeño | En Run Configuration → JVM options: `-Xmx4g -Xms1g` |
| sbt no encuentra `delta-spark 4.0.0` | Repositorio Maven Central con caché desactualizada | En la sbt Shell de IntelliJ ejecutar: `reload` y luego `update` |

---

## 10. Resumen del flujo completo

```
INTELLIJ IDEA (Vía 1 — Spark Shell / sbt)
│
├── build.sbt
│     ├── scalaVersion := "2.13.18"
│     ├── "org.apache.spark" %% "spark-core" % "4.1.1"
│     ├── "org.apache.spark" %% "spark-sql"  % "4.1.1"
│     └── "io.delta"         %% "delta-spark" % "4.0.0"
│
└── src/main/scala/VentasDelta.scala
      ├── SparkSession con DeltaSparkSessionExtension + DeltaCatalog
      ├── DataFrame de 30 ventas (6 meses, 5 ciudades, 4 categorías)
      ├── Transformaciones: detalle + ciudad + categoría + mes
      └── .write.format("delta").mode("overwrite").save(ruta)
              │
              ▼
C:\Curso-Scala\delta\
  ├── ventas_detalle\      ← Parquet + _delta_log
  ├── resumen_ciudad\      ← Parquet + _delta_log
  ├── resumen_categoria\   ← Parquet + _delta_log
  └── evolucion_mensual\   ← Parquet + _delta_log
              │
              ▼
POWER BI DESKTOP
  ├── Obtener datos → Parquet (x4 archivos .parquet)
  ├── Power Query: cambiar tipos + renombrar consultas
  ├── Página "Resumen Ejecutivo"  → KPIs + barras + anillo
  ├── Página "Análisis Temporal"  → líneas + tabla condicional
  ├── Página "Detalle de Ventas"  → segmentadores + tabla + apiladas
  └── Página "Dashboard"          → layout completo + filtros interactivos
```

---

### Diferencias clave respecto al tutorial CSV

| Aspecto | Tutorial CSV (Almond) | Este tutorial (Delta Lake + IntelliJ) |
|---|---|---|
| Entorno de escritura | Jupyter + Almond kernel | IntelliJ + sbt + JVM |
| Formato de salida | CSV plano | Delta Lake (Parquet + log) |
| Cómo lee Power BI | Texto/CSV | Parquet (mismo conector) |
| Actualización incremental | Hay que sobrescribir el CSV | `.mode("append")` sin perder datos anteriores |
| Historial de cambios | ❌ | ✅ `DeltaTable.history()` |
| Transacciones ACID | ❌ | ✅ |
| Integración con Databricks | Limitada | Nativa |

---

> 💾 **Guardar el informe Power BI:** Archivo → Guardar como → `ventas_delta.pbix`.

> 🔄 **Ciclo de trabajo:** modifica el código Scala → ejecuta desde IntelliJ (`Shift+F10`) → en Power BI haz clic en **Inicio → Actualizar** → los gráficos reflejan los nuevos datos inmediatamente.
---
### Ejercicio — Delta Lake desde Almond (Jupyter + VSCode)
Entorno: Scala 2.13.18 · Spark 4.1.1 · kernel Almond · Windows 11
En el tutorial anterior escribiste datos en Delta Lake desde un proyecto IntelliJ con sbt. El objetivo de este ejercicio es reproducir el mismo flujo desde un notebook de Almond en Jupyter, comprobando que el formato Delta Lake no depende del entorno de ejecución sino de la dependencia y la configuración de SparkSession.

Lo que debes conseguir
Al finalizar el ejercicio deberás tener en C:\Curso-Scala\delta-almond\ cuatro carpetas Delta Lake con la misma estructura que generó el proyecto IntelliJ:
```Bash
delta-almond/
  ├── ventas_detalle/
  │     ├── _delta_log/
  │     └── part-00000-xxxx.parquet
  ├── resumen_ciudad/
  ├── resumen_categoria/
  └── evolucion_mensual/
```
Y Power BI debe poder cargar esos archivos .parquet exactamente igual que hizo con los del tutorial anterior.

Pistas

- La dependencia de Delta Lake se declara con $ivy igual que Spark. Consulta el tutorial de IntelliJ para ver el nombre exacto del artefacto.
- La SparkSession necesita dos .config(...) adicionales respecto a los notebooks anteriores del curso. Sin ellos, .format("delta") fallará con un error de formato no reconocido.
- Si tienes una SparkSession activa en el kernel sin esas configuraciones, deberás reiniciar el kernel antes de volver a crearla.
- El dataset, las transformaciones y las rutas de escritura son los mismos que en el tutorial. Solo cambia la forma de declarar dependencias y crear la sesión.
- Usa DeltaTable.forPath(spark, ruta).history() al final para verificar que Delta Lake registró correctamente la transacción.


Entregables
Un fichero ventas_delta_almond.ipynb guardado en C:\Curso-Scala\notebooks\ con todas las celdas ejecutadas y sus salidas visibles.
