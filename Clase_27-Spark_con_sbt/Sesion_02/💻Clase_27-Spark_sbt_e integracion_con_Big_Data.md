# 💻Clase 27 - Spark+sbt e integracion con Big Data

---

# Agenda:

<aside>
💡

#### 9:00 - 9:50    → Sesión 1: **Estructura de proyectos Spark con sbt**

#### 9:50 - 11:20   → Practica

#### **11:20 - 11:40  →  Descanso**

#### 11:40 - 12:40  → Sesión 2: Integración de Spark con el ecosistema Big Data

#### 12:40 - 14:00  → Practica

</aside>

---

# **Sesión 1. Estructura de proyectos Spark con sbt**

**1. Estructura de proyectos Spark con sbt**

---

📝

Hasta ahora hemos trabajado en notebooks de Almond: ideales para exploración, análisis y aprendizaje. En un entorno profesional, las aplicaciones Spark se empaquetan como **proyectos sbt** que se compilan, testean y despliegan de forma reproducible.

La estructura estándar de un proyecto Spark con sbt es la siguiente:

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image.png)

**¿Para qué sirve cada carpeta?**

![3.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/3.png)

**2. Gestión de dependencias en `build.sbt`**

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%201.png)

**3. Patrones de diseño**

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%202.png)

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%203.png)

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%204.png)

**4. Testing en Spark con ScalaTest**

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%205.png)

**5. Logging con slf4j y log4j2**

> Spark 4.x usa **log4j2**. El patrón estándar usa la fachada **slf4j**, que permite cambiar la implementación sin modificar el código de la aplicación.
> 

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%206.png)

# **Practica 1 - 🔵 Vía 1 — IntelliJ IDEA + sbt**

**📝 P1 — Crear la estructura del proyecto**

> **Objetivo:** generar el árbol de directorios estándar de un proyecto Spark con sbt.
> 

Empezamos por crear el proyecto en IntelliJ Idea:

1. Haz clic en **New Project**.
    
    ![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%207.png)
    
2. Selecciona una ruta sencilla, por ejemplo:
    
    ```
    C:\Curso-Scala\proyectos
    ```
    
3. Como tipo de proyecto, puedes elegir **Scala / sbt** si IntelliJ te lo ofrece.
4. Nombre del proyecto:
    
    ```
    ventas-spark
    ```
    
5. Crea el proyecto.
    
    ![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%208.png)
    
    Debería verse esto:
    

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%209.png)

1. Abre la terminal integrada de IntelliJ :
    
    ```
    View → Tool Windows → Terminal
    o ALT + F12
    ```
    
    O usa el botón de terminal que tienes en la barra izquierda/inferior.
    
    ![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2010.png)
    
2. En esa terminal pega este bloque:
    
    ```
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
    
    > Este paso creará las carpetas internas que necesita la práctica: `jobs`, `transformations`, `utils`, `resources`, `test` y `data`
    > 
    
    Salida esperada:
    
    ```
    ✅ Estructura creada en C:\Curso-Scala\proyectos\ventas-spark
    ```
    
3. Ahora en IntelliJ puedes hacer una de estas dos cosas para ver las carpetas nuevas:
    
    ```
    Clic derecho sobre ventas-spark → Reload from Disk
    ```
    
    ![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2011.png)
    

**📝 P2 — Crear `build.sbt` y `build.properties`**

> **Objetivo:** configurar correctamente las dependencias con las versiones exactas del curso.
> 

Como IntelliJ ya te creó un `build.sbt`, ahora debes **reemplazar todo su contenido** por este:

```
ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version      := "1.0.0"
ThisBuild / organization := "com.empresa"

lazy val root = (project in file("."))
  .settings(
    name := "ventas-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "4.1.1",
      "org.apache.spark" %% "spark-sql"  % "4.1.1",
      "org.slf4j"                % "slf4j-api"         % "2.0.9",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0" % "runtime",
      "org.scalatest"    %% "scalatest"  % "3.2.18" % Test,
      "org.apache.spark" %% "spark-core" % "4.1.1"  % Test,
      "org.apache.spark" %% "spark-sql"  % "4.1.1"  % Test
    )
  )
```

Guarda el archivo con CTRL + s.

Ahora toca **ejecutar una comprobación de compilación**, desde la propia terminal de IntelliJ ejecuta:

```
sbt compile
```

Salida:

```
[success] Total time: 13 s, completed 12 may 2026 21:50:46
```

**📝 P3 — Crear los datos de prueba**

> **Objetivo:** generar el CSV que usarán tanto el job principal como los tests.
> 

En la misma terminal de IntelliJ, pega esto:

```
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

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2012.png)

**📝 P4 — Crear `LimpiezaDatos.scala`**

> **Objetivo:** escribir el primer fichero `.scala` del proyecto con las transformaciones reutilizables.
> 

En IntelliJ, abre esta ruta en el panel izquierdo:

```
src/main/scala/com.empresa/transformations
```

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2013.png)

Luego:

```
Clic derecho sobre transformations → New → Scala Class
```

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2014.png)

Selecciona:

```
Object
```

Nombre:

```
LimpiezaDatos
```

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2015.png)

Y pega este código completo:

```
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

Después guarda y ejecuta:

```
sbt compile
```

Este es el primer fichero de transformaciones reutilizables

**Salida esperada:**

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2016.png)

> ⏳ La primera compilación descarga las dependencias de Spark (~400 MB). Las siguientes son inmediatas.
> 

**📝 P5 — Crear `SparkUtils.scala`**

> **Objetivo:** centralizar la creación de la `SparkSession` en un objeto singleton reutilizable.
> 

Crea `src/main/scala/com.empresa/utils/SparkUtils.scala`:

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2017.png)

Ahora pulsa **Enter** o confirma la creación del archivo. Luego pega este código completo:

```
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

Guarda y compila:

```
sbt compile
```

Salida esperada:

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2018.png)

**📝 P6 — Crear `VentasJob.scala` con logging real**

> **Objetivo:** escribir el job principal usando `slf4j` para logging profesional.
> 

Crea `src/main/scala/com.empresa/jobs:`

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2019.png)

Luego:

```
jobs → New → Scala Class/File → Object → VentasJob
```

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2020.png)

Enter. Luego pega este código:

```
package com.empresa.jobs

import org.apache.spark.sql.SparkSession
import com.empresa.transformations.LimpiezaDatos
import org.slf4j.LoggerFactory

object VentasJob {

  private val logger = LoggerFactory.getLogger(getClass)

  def ejecutar(spark: SparkSession, rutaEntrada: String, rutaSalida: String): Unit = {
    logger.info("=== Iniciando VentasJob ===")
    logger.info(s"Entrada:$rutaEntrada")

    val dfRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(rutaEntrada)

    logger.info(s"Registros leídos:${dfRaw.count()}")

    val dfLimpio = LimpiezaDatos.pipeline(dfRaw)
    logger.info(s"Registros tras limpieza:${dfLimpio.count()}")

    dfLimpio.write.mode("overwrite").parquet(rutaSalida)
    logger.info(s"Resultado escrito en:$rutaSalida")
    logger.info("=== VentasJob completado ===")
  }
}
```

Guarda y luego ejecuta:

```
sbt compile
```

Salida esperada:

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2021.png)

**📝 P7 — Crear `Main.scala` y ejecutar con `sbt run`**

> **Objetivo:** crear el punto de entrada de la aplicación y ejecutarla de principio a fin.
> 

Ahora crea este archivo en:

```
src/main/scala/com.empresa
```

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2022.png)

Es decir, dentro del paquete `com.empresa`, no dentro de `jobs`, `utils` ni `transformations`.

Luego:

```
com.empresa → New → Scala Class/File → Object → Main
```

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2023.png)

Presiona Enter. Luego pega el siguiente código:

```
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

Guardar y luego compilar:

```
sbt compile
```

Salida esperada:

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2024.png)

Si compila correctamente, probaremos la ejecución completa con:

```
sbt run
```

Si estas en la terminal PowerShell de IntelliJ.

También puedes ejecutar desde la sbt shell:

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2025.png)

Escribiendo:

```
run
```

O en la parte superior en el boton ▶️:

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2026.png)

o, pulsando con el botón derecho del mouse sobre el código:

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2027.png)

Salida esperada:

```
PS C:\Curso-Scala\proyectos\ventas-spark> sbt run
[info] welcome to sbt 1.12.11 (Eclipse Adoptium Java 17.0.18)
[info] loading global plugins from C:\SparkDev\SbtData\.sbt\plugins
[info] loading project definition from C:\Curso-Scala\proyectos\ventas-spark\project
[info] loading settings for project root from build.sbt...
[info] set current project to ventas-spark (in build file:/C:/Curso-Scala/proyectos/ventas-spark/)
[info] running com.empresa.Main
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
26/05/12 22:48:01 INFO SparkContext: Running Spark version 4.1.1
26/05/12 22:48:01 INFO SparkContext: OS info Windows 11, 10.0, amd64
26/05/12 22:48:01 INFO SparkContext: Java version 17.0.18+8
26/05/12 22:48:03 INFO ResourceUtils: ==============================================================
26/05/12 22:48:03 INFO ResourceUtils: No custom resources configured for spark.driver.
26/05/12 22:48:03 INFO ResourceUtils: ==============================================================
26/05/12 22:48:03 INFO SparkContext: Submitted application: VentasApp
26/05/12 22:48:03 INFO SecurityManager: Changing view acls to: juanj
26/05/12 22:48:03 INFO SecurityManager: Changing modify acls to: juanj
26/05/12 22:48:03 INFO SecurityManager: Changing view acls groups to: juanj
26/05/12 22:48:03 INFO SecurityManager: Changing modify acls groups to: juanj
26/05/12 22:48:03 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: juanj groups with view permissions: EMPTY; users with modify permissions: juanj; groups with modify permissions: EMPTY; RPC SSL disabled
26/05/12 22:48:04 INFO Utils: Successfully started service 'sparkDriver' on port 63517.
26/05/12 22:48:04 INFO SparkEnv: Registering MapOutputTracker
26/05/12 22:48:04 INFO SparkEnv: Registering BlockManagerMaster
26/05/12 22:48:04 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
26/05/12 22:48:04 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
26/05/12 22:48:04 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
26/05/12 22:48:04 INFO DiskBlockManager: Created local directory at C:\SparkDev\Temp\blockmgr-73c55b99-caab-43f7-ad37-0524e6582f8e
26/05/12 22:48:04 INFO SparkEnv: Registering OutputCommitCoordinator
26/05/12 22:48:04 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
26/05/12 22:48:04 INFO ResourceProfile: Limiting resource is cpu
26/05/12 22:48:04 INFO ResourceProfileManager: Added ResourceProfile id: 0
26/05/12 22:48:04 INFO SecurityManager: Changing view acls to: juanj
26/05/12 22:48:04 INFO SecurityManager: Changing modify acls to: juanj
26/05/12 22:48:04 INFO SecurityManager: Changing view acls groups to: juanj
26/05/12 22:48:04 INFO SecurityManager: Changing modify acls groups to: juanj
26/05/12 22:48:04 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: juanj groups with view permissions: EMPTY; users with modify permissions: juanj; groups with modify permissions: EMPTY; RPC SSL disabled
26/05/12 22:48:04 INFO Executor: Starting executor ID driver on host balrodjj
26/05/12 22:48:04 INFO Executor: OS info Windows 11, 10.0, amd64
26/05/12 22:48:04 INFO Executor: Java version 17.0.18+8
26/05/12 22:48:04 INFO Executor: Starting executor with user classpath (userClassPathFirst = false): ''
26/05/12 22:48:04 INFO Executor: Created or updated repl class loader org.apache.spark.util.MutableURLClassLoader@bd08e60 for default.
26/05/12 22:48:04 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 63518.
26/05/12 22:48:04 INFO NettyBlockTransferService: Server created on balrodjj:63518
26/05/12 22:48:04 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
26/05/12 22:48:04 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, balrodjj, 63518, None)
26/05/12 22:48:04 INFO BlockManagerMasterEndpoint: Registering block manager balrodjj:63518 with 434.4 MiB RAM, BlockManagerId(driver, balrodjj, 63518, None)
26/05/12 22:48:04 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, balrodjj, 63518, None)
26/05/12 22:48:04 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, balrodjj, 63518, None)
26/05/12 22:48:05 INFO VentasJob$: === Iniciando VentasJob ===
26/05/12 22:48:05 INFO VentasJob$: Entrada: data/ventas_test.csv
26/05/12 22:48:05 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
26/05/12 22:48:05 INFO SharedState: Warehouse path is 'file:/C:/Curso-Scala/proyectos/ventas-spark/spark-warehouse'.
26/05/12 22:48:06 INFO InMemoryFileIndex: It took 53 ms to list leaf files for 1 paths.
26/05/12 22:48:06 INFO InMemoryFileIndex: It took 2 ms to list leaf files for 1 paths.
26/05/12 22:48:08 INFO FileSourceStrategy: Pushed Filters:
26/05/12 22:48:08 INFO FileSourceStrategy: Post-Scan Filters: Set((length(trim(value#0, None)) > 0))
26/05/12 22:48:09 INFO CodeGenerator: Code generated in 460.7012 ms
26/05/12 22:48:09 INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
26/05/12 22:48:09 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 376.0 B, free 434.4 MiB)
26/05/12 22:48:09 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 38.6 KiB, free 434.4 MiB)
26/05/12 22:48:09 INFO SparkContext: Created broadcast 0 from csv at VentasJob.scala:18
26/05/12 22:48:09 INFO FileSourceScanExec: Planning scan with bin packing, max size: 4194304 bytes, open cost is considered as scanning 4194304 bytes.
26/05/12 22:48:09 INFO SparkContext: Starting job: csv at VentasJob.scala:18
26/05/12 22:48:09 INFO DAGScheduler: Got job 0 (csv at VentasJob.scala:18) with 1 output partitions
26/05/12 22:48:09 INFO DAGScheduler: Final stage: ResultStage 0 (csv at VentasJob.scala:18)
26/05/12 22:48:09 INFO DAGScheduler: Parents of final stage: List()
26/05/12 22:48:09 INFO DAGScheduler: Missing parents: List()
26/05/12 22:48:09 INFO DAGScheduler: Missing parents found for ResultStage 0: List()
26/05/12 22:48:09 INFO DAGScheduler: Submitting ResultStage 0 (MapPartitionsRDD[3] at csv at VentasJob.scala:18), which has no missing parents
26/05/12 22:48:10 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 14.3 KiB, free 434.3 MiB)
26/05/12 22:48:10 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 6.8 KiB, free 434.3 MiB)
26/05/12 22:48:10 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1686
26/05/12 22:48:10 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (MapPartitionsRDD[3] at csv at VentasJob.scala:18) (first 15 tasks are for partitions Vector(0))
26/05/12 22:48:10 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks resource profile 0
26/05/12 22:48:10 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (balrodjj,executor driver, partition 0, PROCESS_LOCAL, 10472 bytes)
26/05/12 22:48:10 INFO Executor: Starting executor with user classpath (userClassPathFirst = false): ''
26/05/12 22:48:10 INFO Executor: Using REPL class URI: spark://balrodjj:63517/artifacts/05d1549d-31aa-4a62-93a9-c33d34abfbed/classes/
26/05/12 22:48:10 INFO Executor: Created or updated repl class loader org.apache.spark.executor.ExecutorClassLoader@5070d7f3 for 05d1549d-31aa-4a62-93a9-c33d34abfbed.
26/05/12 22:48:10 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
26/05/12 22:48:10 INFO TransportClientFactory: Successfully created connection to balrodjj/192.168.1.131:63517 after 39 ms (0 ms spent in bootstraps)
26/05/12 22:48:10 INFO CodeGenerator: Code generated in 243.0579 ms
26/05/12 22:48:10 INFO FileScanRDD: Reading File path: file:///C:/Curso-Scala/proyectos/ventas-spark/data/ventas_test.csv, range: 0-328, partition values: [empty row]
26/05/12 22:48:10 INFO CodeGenerator: Code generated in 46.4929 ms
26/05/12 22:48:10 INFO HadoopLineRecordReader: Found UTF-8 BOM and skipped it
26/05/12 22:48:10 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1728 bytes result sent to driver
26/05/12 22:48:10 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 749 ms on balrodjj (executor driver) (1/1)
26/05/12 22:48:10 INFO TaskSchedulerImpl: Removed TaskSet 0.0 whose tasks have all completed, from pool
26/05/12 22:48:10 INFO DAGScheduler: ResultStage 0 (csv at VentasJob.scala:18) finished in 934 ms
26/05/12 22:48:10 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
26/05/12 22:48:10 INFO TaskSchedulerImpl: Canceling stage 0
26/05/12 22:48:10 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Stage finished
26/05/12 22:48:10 INFO DAGScheduler: Job 0 finished: csv at VentasJob.scala:18, took 1007.934 ms
26/05/12 22:48:10 INFO CodeGenerator: Code generated in 15.9191 ms
26/05/12 22:48:11 INFO FileSourceStrategy: Pushed Filters:
26/05/12 22:48:11 INFO FileSourceStrategy: Post-Scan Filters: Set()
26/05/12 22:48:11 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 376.0 B, free 434.3 MiB)
26/05/12 22:48:11 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 38.6 KiB, free 434.3 MiB)
26/05/12 22:48:11 INFO SparkContext: Created broadcast 2 from csv at VentasJob.scala:18
26/05/12 22:48:11 INFO FileSourceScanExec: Planning scan with bin packing, max size: 4194304 bytes, open cost is considered as scanning 4194304 bytes.
26/05/12 22:48:11 INFO SparkContext: Starting job: csv at VentasJob.scala:18
26/05/12 22:48:11 INFO DAGScheduler: Got job 1 (csv at VentasJob.scala:18) with 1 output partitions
26/05/12 22:48:11 INFO DAGScheduler: Final stage: ResultStage 1 (csv at VentasJob.scala:18)
26/05/12 22:48:11 INFO DAGScheduler: Parents of final stage: List()
26/05/12 22:48:11 INFO DAGScheduler: Missing parents: List()
26/05/12 22:48:11 INFO DAGScheduler: Missing parents found for ResultStage 1: List()
26/05/12 22:48:11 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[9] at csv at VentasJob.scala:18), which has no missing parents
26/05/12 22:48:11 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 22.8 KiB, free 434.3 MiB)
26/05/12 22:48:11 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 10.3 KiB, free 434.3 MiB)
26/05/12 22:48:11 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1686
26/05/12 22:48:11 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (MapPartitionsRDD[9] at csv at VentasJob.scala:18) (first 15 tasks are for partitions Vector(0))
26/05/12 22:48:11 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks resource profile 0
26/05/12 22:48:11 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1) (balrodjj,executor driver, partition 0, PROCESS_LOCAL, 10260 bytes)
26/05/12 22:48:11 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
26/05/12 22:48:11 INFO CodeGenerator: Code generated in 11.5667 ms
26/05/12 22:48:11 INFO FileScanRDD: Reading File path: file:///C:/Curso-Scala/proyectos/ventas-spark/data/ventas_test.csv, range: 0-328, partition values: [empty row]
26/05/12 22:48:11 INFO CodeGenerator: Code generated in 10.814 ms
26/05/12 22:48:11 INFO HadoopLineRecordReader: Found UTF-8 BOM and skipped it
26/05/12 22:48:11 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 1972 bytes result sent to driver
26/05/12 22:48:11 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 101 ms on balrodjj (executor driver) (1/1)
26/05/12 22:48:11 INFO TaskSchedulerImpl: Removed TaskSet 1.0 whose tasks have all completed, from pool
26/05/12 22:48:11 INFO DAGScheduler: ResultStage 1 (csv at VentasJob.scala:18) finished in 133 ms
26/05/12 22:48:11 INFO DAGScheduler: Job 1 is finished. Cancelling potential speculative or zombie tasks for this job
26/05/12 22:48:11 INFO TaskSchedulerImpl: Canceling stage 1
26/05/12 22:48:11 INFO TaskSchedulerImpl: Killing all running tasks in stage 1: Stage finished
26/05/12 22:48:11 INFO DAGScheduler: Job 1 finished: csv at VentasJob.scala:18, took 140.8706 ms
26/05/12 22:48:11 INFO FileSourceStrategy: Pushed Filters:
26/05/12 22:48:11 INFO FileSourceStrategy: Post-Scan Filters: Set()
26/05/12 22:48:11 INFO CodeGenerator: Code generated in 27.5347 ms
26/05/12 22:48:11 INFO MemoryStore: Block broadcast_4 stored as values in memory (estimated size 376.0 B, free 434.3 MiB)
26/05/12 22:48:11 INFO MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 38.6 KiB, free 434.2 MiB)
26/05/12 22:48:11 INFO SparkContext: Created broadcast 4 from $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768
26/05/12 22:48:11 INFO FileSourceScanExec: Planning scan with bin packing, max size: 4194304 bytes, open cost is considered as scanning 4194304 bytes.
26/05/12 22:48:11 INFO DAGScheduler: Registering RDD 13 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) as input to shuffle 0
26/05/12 22:48:11 INFO DAGScheduler: Got map stage job 2 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) with 1 output partitions
26/05/12 22:48:11 INFO DAGScheduler: Final stage: ShuffleMapStage 2 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768)
26/05/12 22:48:11 INFO DAGScheduler: Parents of final stage: List()
26/05/12 22:48:11 INFO DAGScheduler: Missing parents: List()
26/05/12 22:48:11 INFO DAGScheduler: Missing parents found for ShuffleMapStage 2: List()
26/05/12 22:48:11 INFO DAGScheduler: Submitting ShuffleMapStage 2 (MapPartitionsRDD[13] at $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768), which has no missing parents
26/05/12 22:48:11 INFO MemoryStore: Block broadcast_5 stored as values in memory (estimated size 19.9 KiB, free 434.2 MiB)
26/05/12 22:48:11 INFO MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 9.7 KiB, free 434.2 MiB)
26/05/12 22:48:11 INFO SparkContext: Created broadcast 5 from broadcast at DAGScheduler.scala:1686
26/05/12 22:48:11 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 2 (MapPartitionsRDD[13] at $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) (first 15 tasks are for partitions Vector(0))
26/05/12 22:48:11 INFO TaskSchedulerImpl: Adding task set 2.0 with 1 tasks resource profile 0
26/05/12 22:48:11 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 2) (balrodjj,executor driver, partition 0, PROCESS_LOCAL, 10461 bytes)
26/05/12 22:48:11 INFO Executor: Running task 0.0 in stage 2.0 (TID 2)
26/05/12 22:48:11 INFO CodeGenerator: Code generated in 92.8316 ms
26/05/12 22:48:12 INFO SecurityManager: Changing view acls to: juanj
26/05/12 22:48:12 INFO SecurityManager: Changing modify acls to: juanj
26/05/12 22:48:12 INFO SecurityManager: Changing view acls groups to: juanj
26/05/12 22:48:12 INFO SecurityManager: Changing modify acls groups to: juanj
26/05/12 22:48:12 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: juanj groups with view permissions: EMPTY; users with modify permissions: juanj; groups with modify permissions: EMPTY; RPC SSL disabled
26/05/12 22:48:12 INFO FileScanRDD: Reading File path: file:///C:/Curso-Scala/proyectos/ventas-spark/data/ventas_test.csv, range: 0-328, partition values: [empty row]
26/05/12 22:48:12 INFO CodeGenerator: Code generated in 20.9048 ms
26/05/12 22:48:12 INFO HadoopLineRecordReader: Found UTF-8 BOM and skipped it
26/05/12 22:48:12 INFO Executor: Finished task 0.0 in stage 2.0 (TID 2). 2003 bytes result sent to driver
26/05/12 22:48:12 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 2) in 308 ms on balrodjj (executor driver) (1/1)
26/05/12 22:48:12 INFO TaskSchedulerImpl: Removed TaskSet 2.0 whose tasks have all completed, from pool
26/05/12 22:48:12 INFO DAGScheduler: ShuffleMapStage 2 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) finished in 366 ms
26/05/12 22:48:12 INFO DAGScheduler: looking for newly runnable stages
26/05/12 22:48:12 INFO DAGScheduler: running: HashSet()
26/05/12 22:48:12 INFO DAGScheduler: waiting: HashSet()
26/05/12 22:48:12 INFO DAGScheduler: failed: HashSet()
26/05/12 22:48:12 INFO CodeGenerator: Code generated in 12.168 ms
26/05/12 22:48:12 INFO SparkContext: Starting job: $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768
26/05/12 22:48:12 INFO DAGScheduler: Got job 3 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) with 1 output partitions
26/05/12 22:48:12 INFO DAGScheduler: Final stage: ResultStage 4 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768)
26/05/12 22:48:12 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 3)
26/05/12 22:48:12 INFO DAGScheduler: Missing parents: List()
26/05/12 22:48:12 INFO DAGScheduler: Missing parents found for ResultStage 4: List()
26/05/12 22:48:12 INFO DAGScheduler: Submitting ResultStage 4 (MapPartitionsRDD[16] at $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768), which has no missing parents
26/05/12 22:48:12 INFO MemoryStore: Block broadcast_6 stored as values in memory (estimated size 13.8 KiB, free 434.2 MiB)
26/05/12 22:48:12 INFO MemoryStore: Block broadcast_6_piece0 stored as bytes in memory (estimated size 6.6 KiB, free 434.2 MiB)
26/05/12 22:48:12 INFO SparkContext: Created broadcast 6 from broadcast at DAGScheduler.scala:1686
26/05/12 22:48:12 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 4 (MapPartitionsRDD[16] at $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) (first 15 tasks are for partitions Vector(0))
26/05/12 22:48:12 INFO TaskSchedulerImpl: Adding task set 4.0 with 1 tasks resource profile 0
26/05/12 22:48:12 INFO TaskSetManager: Starting task 0.0 in stage 4.0 (TID 3) (balrodjj,executor driver, partition 0, NODE_LOCAL, 9843 bytes)
26/05/12 22:48:12 INFO Executor: Running task 0.0 in stage 4.0 (TID 3)
26/05/12 22:48:12 INFO ShuffleBlockFetcherIterator: Getting 1 (60.0 B) non-empty blocks including 1 (60.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/12 22:48:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 7 ms
26/05/12 22:48:12 INFO CodeGenerator: Code generated in 75.2117 ms
26/05/12 22:48:12 INFO Executor: Finished task 0.0 in stage 4.0 (TID 3). 3852 bytes result sent to driver
26/05/12 22:48:12 INFO TaskSetManager: Finished task 0.0 in stage 4.0 (TID 3) in 136 ms on balrodjj (executor driver) (1/1)
26/05/12 22:48:12 INFO TaskSchedulerImpl: Removed TaskSet 4.0 whose tasks have all completed, from pool
26/05/12 22:48:12 INFO DAGScheduler: ResultStage 4 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) finished in 148 ms
26/05/12 22:48:12 INFO DAGScheduler: Job 3 is finished. Cancelling potential speculative or zombie tasks for this job
26/05/12 22:48:12 INFO TaskSchedulerImpl: Canceling stage 4
26/05/12 22:48:12 INFO TaskSchedulerImpl: Killing all running tasks in stage 4: Stage finished
26/05/12 22:48:12 INFO DAGScheduler: Job 3 finished: $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768, took 158.6752 ms
26/05/12 22:48:12 INFO VentasJob$: Registros le├¡dos: 8
26/05/12 22:48:12 INFO FileSourceStrategy: Pushed Filters:
26/05/12 22:48:12 INFO FileSourceStrategy: Post-Scan Filters: Set(atleastnnonnulls(6, id#17, producto#18, categoria#19, precio#20, cantidad#21, ciudad#22))
26/05/12 22:48:12 INFO CodeGenerator: Code generated in 27.7946 ms
26/05/12 22:48:12 INFO MemoryStore: Block broadcast_7 stored as values in memory (estimated size 376.0 B, free 434.2 MiB)
26/05/12 22:48:12 INFO MemoryStore: Block broadcast_7_piece0 stored as bytes in memory (estimated size 38.6 KiB, free 434.1 MiB)
26/05/12 22:48:12 INFO SparkContext: Created broadcast 7 from $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768
26/05/12 22:48:12 INFO FileSourceScanExec: Planning scan with bin packing, max size: 4194304 bytes, open cost is considered as scanning 4194304 bytes.
26/05/12 22:48:12 INFO DAGScheduler: Registering RDD 20 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) as input to shuffle 1
26/05/12 22:48:12 INFO DAGScheduler: Got map stage job 4 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) with 1 output partitions
26/05/12 22:48:12 INFO DAGScheduler: Final stage: ShuffleMapStage 5 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768)
26/05/12 22:48:12 INFO DAGScheduler: Parents of final stage: List()
26/05/12 22:48:12 INFO DAGScheduler: Missing parents: List()
26/05/12 22:48:12 INFO DAGScheduler: Missing parents found for ShuffleMapStage 5: List()
26/05/12 22:48:12 INFO DAGScheduler: Submitting ShuffleMapStage 5 (MapPartitionsRDD[20] at $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768), which has no missing parents
26/05/12 22:48:12 INFO MemoryStore: Block broadcast_8 stored as values in memory (estimated size 22.3 KiB, free 434.1 MiB)
26/05/12 22:48:12 INFO MemoryStore: Block broadcast_8_piece0 stored as bytes in memory (estimated size 10.4 KiB, free 434.1 MiB)
26/05/12 22:48:12 INFO SparkContext: Created broadcast 8 from broadcast at DAGScheduler.scala:1686
26/05/12 22:48:12 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 5 (MapPartitionsRDD[20] at $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) (first 15 tasks are for partitions Vector(0))
26/05/12 22:48:12 INFO TaskSchedulerImpl: Adding task set 5.0 with 1 tasks resource profile 0
26/05/12 22:48:12 INFO TaskSetManager: Starting task 0.0 in stage 5.0 (TID 4) (balrodjj,executor driver, partition 0, PROCESS_LOCAL, 10461 bytes)
26/05/12 22:48:12 INFO Executor: Running task 0.0 in stage 5.0 (TID 4)
26/05/12 22:48:12 INFO CodeGenerator: Code generated in 111.5738 ms
26/05/12 22:48:12 INFO FileScanRDD: Reading File path: file:///C:/Curso-Scala/proyectos/ventas-spark/data/ventas_test.csv, range: 0-328, partition values: [empty row]
26/05/12 22:48:12 INFO CodeGenerator: Code generated in 23.219 ms
26/05/12 22:48:12 INFO HadoopLineRecordReader: Found UTF-8 BOM and skipped it
26/05/12 22:48:12 INFO Executor: Finished task 0.0 in stage 5.0 (TID 4). 1965 bytes result sent to driver
26/05/12 22:48:12 INFO TaskSetManager: Finished task 0.0 in stage 5.0 (TID 4) in 189 ms on balrodjj (executor driver) (1/1)
26/05/12 22:48:12 INFO TaskSchedulerImpl: Removed TaskSet 5.0 whose tasks have all completed, from pool
26/05/12 22:48:12 INFO DAGScheduler: ShuffleMapStage 5 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) finished in 204 ms
26/05/12 22:48:12 INFO DAGScheduler: looking for newly runnable stages
26/05/12 22:48:12 INFO DAGScheduler: running: HashSet()
26/05/12 22:48:12 INFO DAGScheduler: waiting: HashSet()
26/05/12 22:48:12 INFO DAGScheduler: failed: HashSet()
26/05/12 22:48:12 INFO SparkContext: Starting job: $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768
26/05/12 22:48:12 INFO DAGScheduler: Got job 5 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) with 1 output partitions
26/05/12 22:48:12 INFO DAGScheduler: Final stage: ResultStage 7 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768)
26/05/12 22:48:12 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 6)
26/05/12 22:48:12 INFO DAGScheduler: Missing parents: List()
26/05/12 22:48:12 INFO DAGScheduler: Missing parents found for ResultStage 7: List()
26/05/12 22:48:12 INFO DAGScheduler: Submitting ResultStage 7 (MapPartitionsRDD[23] at $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768), which has no missing parents
26/05/12 22:48:12 INFO MemoryStore: Block broadcast_9 stored as values in memory (estimated size 13.8 KiB, free 434.1 MiB)
26/05/12 22:48:12 INFO MemoryStore: Block broadcast_9_piece0 stored as bytes in memory (estimated size 6.6 KiB, free 434.1 MiB)
26/05/12 22:48:12 INFO SparkContext: Created broadcast 9 from broadcast at DAGScheduler.scala:1686
26/05/12 22:48:12 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 7 (MapPartitionsRDD[23] at $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) (first 15 tasks are for partitions Vector(0))
26/05/12 22:48:12 INFO TaskSchedulerImpl: Adding task set 7.0 with 1 tasks resource profile 0
26/05/12 22:48:12 INFO TaskSetManager: Starting task 0.0 in stage 7.0 (TID 5) (balrodjj,executor driver, partition 0, NODE_LOCAL, 9843 bytes)
26/05/12 22:48:12 INFO Executor: Running task 0.0 in stage 7.0 (TID 5)
26/05/12 22:48:12 INFO ShuffleBlockFetcherIterator: Getting 1 (60.0 B) non-empty blocks including 1 (60.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/05/12 22:48:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/05/12 22:48:12 INFO Executor: Finished task 0.0 in stage 7.0 (TID 5). 3852 bytes result sent to driver
26/05/12 22:48:12 INFO TaskSetManager: Finished task 0.0 in stage 7.0 (TID 5) in 13 ms on balrodjj (executor driver) (1/1)
26/05/12 22:48:12 INFO TaskSchedulerImpl: Removed TaskSet 7.0 whose tasks have all completed, from pool
26/05/12 22:48:12 INFO DAGScheduler: ResultStage 7 ($anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768) finished in 26 ms
26/05/12 22:48:12 INFO DAGScheduler: Job 5 is finished. Cancelling potential speculative or zombie tasks for this job
26/05/12 22:48:12 INFO TaskSchedulerImpl: Canceling stage 7
26/05/12 22:48:12 INFO TaskSchedulerImpl: Killing all running tasks in stage 7: Stage finished
26/05/12 22:48:12 INFO DAGScheduler: Job 5 finished: $anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768, took 36.4232 ms
26/05/12 22:48:12 INFO VentasJob$: Registros tras limpieza: 8
26/05/12 22:48:13 INFO FileSourceStrategy: Pushed Filters:
26/05/12 22:48:13 INFO FileSourceStrategy: Post-Scan Filters: Set(atleastnnonnulls(6, id#17, producto#18, categoria#19, precio#20, cantidad#21, ciudad#22))
26/05/12 22:48:13 INFO ParquetUtils: Using default output committer for Parquet: org.apache.parquet.hadoop.ParquetOutputCommitter
26/05/12 22:48:13 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/05/12 22:48:13 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/05/12 22:48:13 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/05/12 22:48:13 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/05/12 22:48:13 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/05/12 22:48:13 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/05/12 22:48:13 INFO CodeGenerator: Code generated in 21.2638 ms
26/05/12 22:48:13 INFO MemoryStore: Block broadcast_10 stored as values in memory (estimated size 376.0 B, free 434.1 MiB)
26/05/12 22:48:13 INFO MemoryStore: Block broadcast_10_piece0 stored as bytes in memory (estimated size 38.6 KiB, free 434.1 MiB)
26/05/12 22:48:13 INFO SparkContext: Created broadcast 10 from parquet at VentasJob.scala:25
26/05/12 22:48:13 INFO FileSourceScanExec: Planning scan with bin packing, max size: 4194304 bytes, open cost is considered as scanning 4194304 bytes.
26/05/12 22:48:13 INFO SparkContext: Starting job: parquet at VentasJob.scala:25
26/05/12 22:48:13 INFO DAGScheduler: Got job 6 (parquet at VentasJob.scala:25) with 1 output partitions
26/05/12 22:48:13 INFO DAGScheduler: Final stage: ResultStage 8 (parquet at VentasJob.scala:25)
26/05/12 22:48:13 INFO DAGScheduler: Parents of final stage: List()
26/05/12 22:48:13 INFO DAGScheduler: Missing parents: List()
26/05/12 22:48:13 INFO DAGScheduler: Missing parents found for ResultStage 8: List()
26/05/12 22:48:13 INFO DAGScheduler: Submitting ResultStage 8 (MapPartitionsRDD[27] at parquet at VentasJob.scala:25), which has no missing parents
26/05/12 22:48:13 INFO MemoryStore: Block broadcast_11 stored as values in memory (estimated size 243.4 KiB, free 433.8 MiB)
26/05/12 22:48:13 INFO MemoryStore: Block broadcast_11_piece0 stored as bytes in memory (estimated size 88.1 KiB, free 433.7 MiB)
26/05/12 22:48:13 INFO SparkContext: Created broadcast 11 from broadcast at DAGScheduler.scala:1686
26/05/12 22:48:13 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 8 (MapPartitionsRDD[27] at parquet at VentasJob.scala:25) (first 15 tasks are for partitions Vector(0))
26/05/12 22:48:13 INFO TaskSchedulerImpl: Adding task set 8.0 with 1 tasks resource profile 0
26/05/12 22:48:13 INFO TaskSetManager: Starting task 0.0 in stage 8.0 (TID 6) (balrodjj,executor driver, partition 0, PROCESS_LOCAL, 10472 bytes)
26/05/12 22:48:13 INFO Executor: Running task 0.0 in stage 8.0 (TID 6)
26/05/12 22:48:13 INFO CodeGenerator: Code generated in 45.4728 ms
26/05/12 22:48:13 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/05/12 22:48:13 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/05/12 22:48:13 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/05/12 22:48:13 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/05/12 22:48:13 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/05/12 22:48:13 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/05/12 22:48:13 INFO CodecConfig: Compression: SNAPPY
26/05/12 22:48:13 INFO CodecConfig: Compression: SNAPPY
26/05/12 22:48:13 INFO ParquetOutputFormat: ParquetRecordWriter [block size: 134217728b, row group padding size: 8388608b, validating: false]
26/05/12 22:48:13 INFO CodecPool: Got brand-new compressor [.snappy]
26/05/12 22:48:13 INFO FileScanRDD: Reading File path: file:///C:/Curso-Scala/proyectos/ventas-spark/data/ventas_test.csv, range: 0-328, partition values: [empty row]
26/05/12 22:48:13 INFO HadoopLineRecordReader: Found UTF-8 BOM and skipped it
26/05/12 22:48:14 INFO FileOutputCommitter: Saved output of task 'attempt_202605122248131674415687203243251_0008_m_000000_6' to file:/C:/Curso-Scala/proyectos/ventas-spark/data/resultado_ventas/_temporary/0/task_202605122248131674415687203243251_0008_m_000000
26/05/12 22:48:14 INFO SparkHadoopMapRedUtil: attempt_202605122248131674415687203243251_0008_m_000000_6: Committed. Elapsed time: 3 ms.
26/05/12 22:48:14 INFO Executor: Finished task 0.0 in stage 8.0 (TID 6). 2855 bytes result sent to driver
26/05/12 22:48:14 INFO TaskSetManager: Finished task 0.0 in stage 8.0 (TID 6) in 986 ms on balrodjj (executor driver) (1/1)
26/05/12 22:48:14 INFO TaskSchedulerImpl: Removed TaskSet 8.0 whose tasks have all completed, from pool
26/05/12 22:48:14 INFO DAGScheduler: ResultStage 8 (parquet at VentasJob.scala:25) finished in 1054 ms
26/05/12 22:48:14 INFO DAGScheduler: Job 6 is finished. Cancelling potential speculative or zombie tasks for this job
26/05/12 22:48:14 INFO TaskSchedulerImpl: Canceling stage 8
26/05/12 22:48:14 INFO TaskSchedulerImpl: Killing all running tasks in stage 8: Stage finished
26/05/12 22:48:14 INFO DAGScheduler: Job 6 finished: parquet at VentasJob.scala:25, took 1060.698 ms
26/05/12 22:48:14 INFO FileFormatWriter: Start to commit write Job a14db246-1761-4a31-9698-2ac2ae7ecc7d.
26/05/12 22:48:14 INFO FileFormatWriter: Write Job a14db246-1761-4a31-9698-2ac2ae7ecc7d committed. Elapsed time: 37 ms.
26/05/12 22:48:14 INFO FileFormatWriter: Finished processing stats for write job a14db246-1761-4a31-9698-2ac2ae7ecc7d.
26/05/12 22:48:14 INFO VentasJob$: Resultado escrito en: data/resultado_ventas
26/05/12 22:48:14 INFO VentasJob$: === VentasJob completado ===
26/05/12 22:48:14 INFO SparkContext: SparkContext is stopping with exitCode 0 from stop at Main.scala:16.
26/05/12 22:48:14 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
26/05/12 22:48:14 INFO MemoryStore: MemoryStore cleared
26/05/12 22:48:14 INFO BlockManager: BlockManager stopped
26/05/12 22:48:14 INFO BlockManagerMaster: BlockManagerMaster stopped
26/05/12 22:48:14 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
26/05/12 22:48:14 INFO SparkContext: Successfully stopped SparkContext (Uptime: 12919 ms)
Ô£à Aplicaci├│n finalizada correctamente
[success] Total time: 20 s, completed 12 may 2026 22:48:14
PS C:\Curso-Scala\proyectos\ventas-spark>
```

**Comprueba que se creó el Parquet**

Ejecuta en PowerShell:

```
Get-ChildItem .\data\resultado_ventas
```

Salida esperada:

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2028.png)

**📝 P8 — Escribir y ejecutar tests con ScalaTest**

> El objetivo de este paso es crear tests unitarios para comprobar que las funciones de `LimpiezaDatos.scala` funcionan correctamente antes de ejecutar el pipeline completo. En la práctica, se prueban dos cosas: eliminar nulos y calcular el importe.
> 

Crea `src/test/scala/com.empresa.transformations:`

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2029.png)

Ahí haz:

```
Clic derecho sobre transformations
→ New
→ Scala Class/File
→ Class
```

Nombre:

```
LimpiezaDatosSpec
```

Debe quedar así:

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2030.png)

Enter y luego pega este código:

```
package com.empresa.transformations

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class LimpiezaDatosSpec extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("Tests-LimpiezaDatos")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }

  "eliminarNulos" should "reducir el numero de filas cuando hay valores nulos" in {
    val spark = sparkSession
    import spark.implicits._

    val dfEntrada = Seq(
      ("Laptop", Double.box(1200.0)),
      (null.asInstanceOf[String], Double.box(300.0)),
      ("Tablet", null.asInstanceOf[java.lang.Double])
    ).toDF("producto", "precio")

    val resultado = LimpiezaDatos.eliminarNulos(dfEntrada)

    resultado.count() shouldBe 1
    resultado.collect().head.getString(0) shouldBe "Laptop"
  }

  "agregarImporte" should "calcular precio multiplicado por cantidad" in {
    val spark = sparkSession
    import spark.implicits._

    val dfEntrada = Seq(
      ("Laptop", 1200.0, 2),
      ("Raton", 25.0, 4)
    ).toDF("producto", "precio", "cantidad")

    val resultado = LimpiezaDatos.agregarImporte(dfEntrada)
    val importes = resultado.select("importe").collect().map(_.getDouble(0))

    importes shouldBe Array(2400.0, 100.0)
  }
}
```

Guarda y compila:

```
sbt compile
```

Si te pregunta: Create a new server? y/n (default y) escribes `y` y presionas Enter.

Salida esperada:

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2031.png)

Si compila bien, ejecuta los tests:

```
sbt test
```

Salida esperada:

![image.png](https://github.com/rodbalza/IFCD0115-Procesamineto-BigData-con-Scala/raw/main/Clase_27-/Sesion_01/image%2032.png)

# **🟠 Vía 2 — Almond / Jupyter**

---

**🔧 Celda 0 — Inicialización**

> Ejecuta esta celda **antes que cualquier otra**. Espera el mensaje `✅`.
> 

```
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

println(s"✅ Spark${spark.version} listo | Scala${scala.util.Properties.versionString}")
```

**Salida esperada:**

```
✅ Spark 4.1.1 listo | Scala version 2.13.18
```

---

**📂 Celda 1 — Crear datos de práctica**

```
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

println(s"✅ Ficheros creados en$carpeta")
```

**Salida esperada:**

```
✅ Ficheros creados en C:/Curso-Scala/datos/dia23
```

---

**📝 P1 — Funciones de transformación reutilizables**

> **Objetivo:** practicar el patrón `DataFrame => DataFrame` para crear transformaciones componibles.
> 

**Celda Markdown:**

```
## P1 — Funciones de transformación reutilizables
Cada función hace UNA sola cosa y recibe/devuelve un DataFrame.
```

**Celda de código:**

```
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

**📝 P2 — Agrupar transformaciones en un `object`**

> **Objetivo:** simular la estructura de un fichero `.scala` real agrupando funciones relacionadas en un `object`.
> 

**Celda Markdown:**

```
## P2 — Agrupando transformaciones en un object
En un proyecto sbt, estas funciones vivirían en LimpiezaDatos.scala.
En Almond, las agrupamos en un object en su propia celda.
```

**Celda de código — definición del object (celda separada):**

```
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

```
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

**📝 P3 — Limpieza de datos: manejo de nulos**

> **Objetivo:** explorar el dataset de clientes con nulos y aplicar las dos estrategias principales de limpieza.
> 

**Celda Markdown:**

```
## P3 — Limpieza de datos: manejo de nulos
El DataFrame de clientes tiene filas con valores nulos.
Exploraremos el problema y aplicaremos dos estrategias: eliminar y rellenar.
```

**Celda de código:**

```
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
  println(f"$nombreCol%-12s →$nulos nulos")
}

// Estrategia 1: eliminar filas con cualquier nulo
val dfSinNulos = dfClientes.na.drop()
println(s"\nFilas tras eliminar nulos:${dfSinNulos.count()} (de${dfClientes.count()})")

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

**📝 P4 — Simular logging con mensajes de progreso**

> **Objetivo:** practicar el patrón de trazabilidad de un pipeline completo con mensajes de progreso estructurados.
> 

**Celda Markdown:**

```
## P4 — Logging manual con mensajes de progreso
En Almond simulamos el patrón de logging con funciones de ayuda.
En un proyecto sbt real usaríamos slf4j (ver Vía 1).
```

**Celda de código:**

```
// P4 — Simular logging de un pipeline completo
def logInfo(msg: String): Unit =
  println(s"[${java.time.LocalTime.now().toString.take(8)}] [INFO ]$msg")
def logWarn(msg: String): Unit =
  println(s"[${java.time.LocalTime.now().toString.take(8)}] [WARN ]$msg")

def ejecutarPipelineConLogs(rutaEntrada: String): Unit = {
  logInfo("=== Iniciando pipeline de ventas ===")
  logInfo(s"Leyendo datos de:$rutaEntrada")

  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(rutaEntrada)

  val totalOriginal = df.count()
  logInfo(s"Registros leídos:$totalOriginal")

  val nulos = df.filter(col("precio").isNull || col("cantidad").isNull).count()
  if (nulos > 0) logWarn(s"$nulos registros con precio o cantidad nulos")

  val dfLimpio = df.na.drop(Seq("precio", "cantidad"))
  logInfo(s"Registros tras limpieza:${dfLimpio.count()} (eliminados:${totalOriginal - dfLimpio.count()})")

  val dfConImporte = dfLimpio.withColumn("importe", col("precio") * col("cantidad"))
  val totalImporte = dfConImporte.agg(sum("importe")).collect()(0).getDouble(0)
  logInfo(f"Importe total:$totalImporte%.2f €")
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

**📝 P5 — Función genérica de resumen estadístico**

> **Objetivo:** demostrar que una función bien definida es reutilizable con cualquier DataFrame que tenga las columnas esperadas.
> 

**Celda Markdown:**

```
## P5 — Reutilización: el mismo pipeline para distintos datos
Una transformación no depende de los datos concretos, solo de la estructura.
```

**Celda de código:**

```
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

**📝 P6 — Ciclo completo: leer, transformar y escribir en Parquet**

> **Objetivo:** completar el ciclo lectura → transformación → escritura en el formato estándar de producción.
> 

**Celda Markdown:**

```
## P6 — Ciclo completo con escritura en Parquet
Parquet es el formato de salida estándar en pipelines Spark:
columnar, comprimido y con tipos preservados.
```

**Celda de código:**

```
// P6 — Pipeline completo con escritura en Parquet
val rutaSalida = "C:/Curso-Scala/datos/dia23/resultado_ventas"

val dfFinal = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("C:/Curso-Scala/datos/dia23/ventas.csv")
  .withColumn("importe", col("precio") * col("cantidad"))
  .na.drop()

dfFinal.coalesce(1).write.mode("overwrite").parquet(rutaSalida)
println(s"✅ Resultado escrito en:$rutaSalida")

// Verificar la lectura del Parquet generado
val dfVerif = spark.read.parquet(rutaSalida)
println(s"Registros en Parquet:${dfVerif.count()}")
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

# Sesión 2. Integración de Spark con el Ecosistema Big Data

---

---

## 🧠 BLOQUE TEÓRICO

### 1. El problema que resuelve la integración

<aside>
💡

Spark es un motor de procesamiento, no un sistema de almacenamiento. Funciona como la **cocina de un restaurante**: puede preparar cualquier plato, pero necesita que alguien le suministre los ingredientes (datos) y que alguien recoja los platos listos (resultados). Ese "alguien" es el ecosistema de herramientas que rodea a Spark.

</aside>

En un proyecto Big Data real raramente encontrarás Spark trabajando solo. Siempre estará conectado a uno o varios sistemas externos:

![image.png](image.png)

**Hive Metastore** es una base de datos (normalmente MySQL o PostgreSQL) que guarda el "catálogo" del data lake: nombres de tablas, columnas, tipos de datos y la ruta en HDFS donde están los ficheros reales. No almacena datos, solo la descripción de dónde están y cómo leerlos. Es como el índice de una biblioteca: el índice no tiene los libros, pero te dice en qué estantería está cada uno.

**HBase** es una base de datos NoSQL que vive sobre HDFS y permite leer o escribir una fila concreta en milisegundos, aunque la tabla tenga miles de millones de filas. Mientras Spark + Hive está pensado para analizar millones de filas a la vez, HBase está pensado para buscar o actualizar un registro individual muy rápido. La analogía: Hive es como hacer una auditoría de todo el almacén; HBase es como buscar un producto concreto por su código de barras.

**Hive (escritura)**  se refiere a guardar el resultado de un DataFrame de Spark como una tabla permanente en el Hive Metastore, usando `saveAsTable()` o `CREATE TABLE ... AS SELECT`. Después de escribirla, esa tabla es visible para cualquier herramienta conectada al mismo metastore (otro job de Spark, un cuadro de mando, un equipo de analistas con HiveQL). Es la diferencia entre guardar un fichero Parquet en un directorio sin nombre lógico y registrarlo con nombre, esquema y descripción para que otros lo encuentren.

---

### 2. Spark + HDFS: lectura y escritura distribuida

### ¿Qué es HDFS?

<aside>
💡

**HDFS** (Hadoop Distributed File System) es el sistema de ficheros distribuido de Apache Hadoop. Piénsalo como un disco duro gigante repartido entre decenas o cientos de máquinas. Sus características clave:

- **Distribuido:** los ficheros se dividen en bloques (por defecto 128 MB) y se reparten entre los nodos del clúster.
- **Replicado:** cada bloque se copia en 3 nodos (factor de replicación por defecto). Si un nodo cae, los datos no se pierden.
- **Diseñado para lecturas secuenciales largas:** óptimo para Big Data, no para acceso aleatorio.
</aside>

### La relación natural Spark ↔ HDFS

![image.png](image%201.png)

![image.png](image%202.png)

<aside>
💡

En un ordenador normal, cuando quieres procesar un fichero, lo traes a donde está el programa: el fichero viaja desde el disco (o la red) hasta la CPU que va a procesarlo. Eso funciona bien con ficheros pequeños.

En un clúster con HDFS, los datos están repartidos entre decenas de máquinas y pueden pesar terabytes. Si trajeras los datos al programa, estarías moviendo terabytes por la red interna del clúster en cada job. La red es el cuello de botella más severo en un clúster distribuido.

La solución de Spark + HDFS es la contraria: **el programa viaja a donde están los datos**. Cada nodo del clúster tiene almacenados algunos bloques de datos y también tiene un Executor de Spark corriendo. Cuando Spark planifica el job, intenta asignar cada tarea al Executor que está en el mismo nodo donde está el bloque que esa tarea necesita leer. El código (que son kilobytes) viaja al dato (que son gigabytes), no al revés.

El resultado es que en la mayoría de los casos cada Executor lee sus bloques directamente del disco local, sin tocar la red.

</aside>

![image.png](image%203.png)

> Lo que viaja por la red no son los 1 millón de pedidos, sino 9 filas de resumen. Eso es manejable.
> 
> 
> Cuando haces `.show()` en tu notebook, el Driver recoge esos resultados parciales, los combina y te los muestra. El Driver es el coordinador central que vive en tu máquina (o en el nodo maestro del clúster) y es el único que ve el resultado final completo.
> 
> La regla general es: **los datos crudos no se mueven, solo los resultados intermedios**. Y Spark está diseñado para que esos resultados intermedios sean lo más pequeños posible antes de moverlos, que es exactamente lo que hace el optimizador Catalyst cuando analiza tu consulta.
> 

![image.png](image%204.png)

---

### 3. Spark + Hive: consultas sobre el Hive Metastore

![image.png](image%205.png)

![image.png](image%206.png)

### Habilitar soporte Hive en Spark

```scala
// Al crear la SparkSession, añadir .enableHiveSupport()
// (requiere hive-site.xml o metastore Thrift corriendo)
val spark = SparkSession.builder()
  .appName("Spark + Hive")
  .master("local[*]")
  .enableHiveSupport()
  .getOrCreate()
```

### Leer y escribir tablas Hive

```scala
// Listar bases de datos disponibles en el metastore
spark.sql("SHOW DATABASES").show()

// Usar una base de datos
spark.sql("USE ventas_db")

// Leer una tabla Hive directamente (Spark se conecta al metastore)
val clientes = spark.table("clientes")
clientes.show(5)

// Equivalente con SQL
val resultado = spark.sql("""
  SELECT ciudad, COUNT(*) as total_clientes
  FROM clientes
  GROUP BY ciudad
  ORDER BY total_clientes DESC
""")
resultado.show()

// Escribir un DataFrame como tabla Hive permanente
resultado.write
  .mode("overwrite")
  .saveAsTable("resumen_clientes_por_ciudad")

// Crear tabla Hive desde Spark SQL directamente
spark.sql("""
  CREATE TABLE IF NOT EXISTS productos_procesados
  USING parquet
  AS SELECT id, nombre, precio * 1.21 as precio_con_iva
  FROM productos
""")
```

> 💡 **Concepto clave:** Spark + Hive no significa que Spark use el motor de Hive para ejecutar las consultas. Spark usa el **Hive Metastore solo para conocer el esquema y la ubicación de los datos**. La ejecución siempre la realiza Spark. Es como consultar el índice de un libro para saber en qué página está el capítulo, pero leer tú mismo el capítulo.
> 

---

### 4. Spark + Kafka: ingestión de datos en streaming

![image.png](image%207.png)

![image.png](image%208.png)

### Leer de Kafka con Spark Structured Streaming

```scala
// Dependencia necesaria en build.sbt (para referencia):
// "org.apache.spark" %% "spark-sql-kafka-0-10" % "4.1.1"

// Crear un stream que lee de Kafka
val kafkaStream = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "kafka-broker:9092")  // servidor Kafka
  .option("subscribe", "pagos-online")                      // topic a consumir
  .option("startingOffsets", "latest")                      // solo mensajes nuevos
  .load()

// El DataFrame tiene columnas fijas de Kafka:
// key (binary), value (binary), topic, partition, offset, timestamp

// Decodificar el valor (los mensajes en Kafka son bytes)
import org.apache.spark.sql.functions._

val pagos = kafkaStream
  .select(
    col("key").cast("string").as("id_pago"),
    col("value").cast("string").as("json_pago"),
    col("timestamp")
  )

// Parsear el JSON del campo value
val pagosParsed = pagos.select(
  col("id_pago"),
  col("timestamp"),
  get_json_object(col("json_pago"), "$.importe").cast("double").as("importe"),
  get_json_object(col("json_pago"), "$.ciudad").as("ciudad")
)

// Escribir el stream procesado (por ejemplo, a consola para debugging)
val query = pagosParsed.writeStream
  .format("console")
  .outputMode("append")
  .start()

query.awaitTermination()
```

> ⚠️ **En entorno de clase:** No tenemos un servidor Kafka corriendo localmente. Los ejercicios prácticos simularán el comportamiento de Kafka con ficheros JSON o con socket. En entornos productivos y en Databricks, la conexión a Kafka es exactamente como el código de arriba.
> 

---

### 5. Spark + HBase y Spark + Delta Lake

![image.png](image%209.png)

La integración técnica usa la librería `SHC` (Spark-HBase Connector) o `hbase-spark`:

```scala
// Ejemplo conceptual — requiere conector externo
// En producción:
val hbaseConf = Map(
  "hbase.zookeeper.quorum" -> "zookeeper-host:2181",
  "hbase.table" -> "perfiles_usuarios",
  "hbase.columns.mapping" -> "id STRING :key, nombre STRING cf:nombre"
)

val perfiles = spark.read
  .format("org.apache.hadoop.hbase.spark")
  .options(hbaseConf)
  .load()
```

### Spark + Delta Lake ⭐

![image.png](image%2010.png)

Delta Lake guarda un **transaction log** (carpeta `_delta_log/`) junto a los ficheros Parquet. Ese log registra cada operación, como el historial de commits de Git.

```scala
// Dependencia en Almond:
// import $ivy.`io.delta:delta-spark_2.13:3.3.0`

import io.delta.tables._

// Crear/escribir una tabla Delta
df.write
  .format("delta")
  .mode("overwrite")
  .save("C:/Curso-Scala/datos/tabla_delta/")

// Leer una tabla Delta
val deltaDF = spark.read
  .format("delta")
  .load("C:/Curso-Scala/datos/tabla_delta/")

// Time travel: leer una versión anterior
val version0 = spark.read
  .format("delta")
  .option("versionAsOf", "0")
  .load("C:/Curso-Scala/datos/tabla_delta/")

// UPDATE nativo (imposible con Parquet puro)
val deltaTable = DeltaTable.forPath(spark, "C:/Curso-Scala/datos/tabla_delta/")

deltaTable.update(
  condition = col("ciudad") === "Madrid",
  set = Map("precio" -> (col("precio") * lit(1.05)))
)

// Ver el historial de cambios
deltaTable.history().show(truncate = false)
```

> 💡 **Delta Lake e**s la tecnología es la mas utilizada. Un pipeline Big Data moderno lee de Kafka → procesa con Spark → escribe en Delta Lake → consulta con Spark SQL. Databricks fue el creador de Delta Lake y lo integra de forma nativa.
> 

![image.png](image%2011.png)

---

### 6. Resumen visual

![image.png](image%2012.png)

---

## 💻 BLOQUE PRÁCTICO (Opcional)

### Configuración del notebook

> ⚠️ **Recordatorio patrón Almond:** si usas `case class` con `.toDS()`, defínela en una celda separada antes de usarla. En esta sesión trabajaremos principalmente con DataFrames, pero el patrón sigue aplicando si aparecen Datasets tipados.
> 

---

**Celda Code — Inicialización:**

```scala
import $ivy.`org.apache.spark::spark-sql:4.1.1`
import $ivy.`io.delta:delta-spark_2.13:3.3.0`

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("Dia25-Ecosistema")
  .master("local[*]")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .config("spark.ui.enabled", "false")
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

import spark.implicits._

println(s"Spark ${spark.version} listo — Scala ${scala.util.Properties.versionString}")
```

**Salida esperada:**

```
Spark 4.1.1 listo — Scala version 2.13.18 (...)
```

---

### P1 — Leer un CSV simulando datos de HDFS

En producción leerías `hdfs://namenode:9000/datos/pedidos.csv`. En clase usamos una ruta local con exactamente el mismo código Spark (solo cambia el prefijo de la URI).

```
## P1 — Lectura de datos (simulando HDFS con ruta local)
```

**Celda Code:**

```scala
// Crear dataset de pedidos en memoria y guardarlo como CSV
val pedidosData = Seq(
  (1, "2024-01-10", "Madrid",    "electronica", 299.99, 2),
  (2, "2024-01-10", "Barcelona", "ropa",         45.00, 3),
  (3, "2024-01-11", "Madrid",    "electronica", 899.00, 1),
  (4, "2024-01-11", "Sevilla",   "hogar",        120.50, 4),
  (5, "2024-01-12", "Barcelona", "electronica", 199.99, 2),
  (6, "2024-01-12", "Madrid",    "ropa",          55.00, 5),
  (7, "2024-01-13", "Valencia",  "hogar",         88.75, 2),
  (8, "2024-01-13", "Madrid",    "electronica", 450.00, 1),
  (9, "2024-01-14", "Sevilla",   "ropa",          32.00, 3),
  (10,"2024-01-14", "Barcelona", "hogar",         210.00, 2)
).toDF("id", "fecha", "ciudad", "categoria", "precio_unitario", "cantidad")

// Guardar como CSV (simula lo que encontrarías en HDFS o S3)
val rutaCSV = "C:/Curso-Scala/datos/pedidos_raw/"
pedidosData.coalesce(1)
  .write
  .mode("overwrite")
  .option("header", "true")
  .csv(rutaCSV)

println(s"Datos guardados en: $rutaCSV")
```

**Celda Code:**

```scala
// Leer el CSV (misma API que hdfs://namenode:9000/datos/pedidos_raw/)
val pedidos = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(rutaCSV)

pedidos.printSchema()
pedidos.show()
println(s"Total pedidos: ${pedidos.count()}")
```

**Salida esperada:**

```
root
 |-- id: integer (nullable = true)
 |-- fecha: string (nullable = true)
 |-- ciudad: string (nullable = true)
 |-- categoria: string (nullable = true)
 |-- precio_unitario: double (nullable = true)
 |-- cantidad: integer (nullable = true)

+---+----------+---------+------------+---------------+--------+
| id|     fecha|   ciudad|   categoria|precio_unitario|cantidad|
+---+----------+---------+------------+---------------+--------+
|  1|2024-01-10|   Madrid| electronica|         299.99|       2|
...
+---+----------+---------+------------+---------------+--------+
Total pedidos: 10
```

---

### P2 — Transformaciones y escritura en Parquet (formato HDFS nativo)

```
## P2 — Calcular importe total y escribir en Parquet
```

**Celda Code:**

```scala
// Añadir columna importe_total = precio_unitario * cantidad
val pedidosConImporte = pedidos
  .withColumn("importe_total", round($"precio_unitario" * $"cantidad", 2))
  .withColumn("fecha", to_date($"fecha", "yyyy-MM-dd"))

pedidosConImporte.show()

// Escribir en Parquet particionado por ciudad (como harías en HDFS)
val rutaParquet = "C:/Curso-Scala/datos/pedidos_parquet/"
pedidosConImporte.write
  .mode("overwrite")
  .partitionBy("ciudad")
  .parquet(rutaParquet)

println(s"Escrito en Parquet particionado por ciudad: $rutaParquet")
```

**Salida esperada:**

```
+---+----------+---------+------------+---------------+--------+-------------+
| id|     fecha|   ciudad|   categoria|precio_unitario|cantidad|importe_total|
+---+----------+---------+------------+---------------+--------+-------------+
|  1|2024-01-10|   Madrid| electronica|         299.99|       2|       599.98|
|  2|2024-01-10|Barcelona|        ropa|           45.0|       3|        135.0|
...
```

**Celda Code:**

```scala
// Verificar la estructura de particiones creada
import java.nio.file.{Paths, Files}
import scala.jdk.CollectionConverters._

val dirs = Files.list(Paths.get("C:/Curso-Scala/datos/pedidos_parquet/"))
  .iterator().asScala
  .filter(Files.isDirectory(_))
  .map(_.getFileName.toString)
  .toList
  .sorted

println("Particiones creadas:")
dirs.foreach(d => println(s"  └── $d"))
```

**Salida esperada:**

```
Particiones creadas:
  └── ciudad=Barcelona
  └── ciudad=Madrid
  └── ciudad=Sevilla
  └── ciudad=Valencia
```

---

### P3 — Simular consulta tipo Hive con vistas temporales

Como no tenemos un servidor Hive Metastore, simularemos su función con **vistas temporales**. La mecánica SQL es idéntica a lo que usarías con `spark.sql("SELECT ... FROM tabla_hive")`.

```
## P3 — Spark SQL sobre vistas temporales (equivalente a Hive)
```

**Celda Code:**

```scala
// Registrar el DataFrame como vista temporal (equivale a una tabla Hive)
pedidosConImporte.createOrReplaceTempView("pedidos")

// Ahora podemos hacer exactamente las mismas consultas que con Hive
val ventasPorCategoria = spark.sql("""
  SELECT
    categoria,
    COUNT(*)                        AS num_pedidos,
    ROUND(SUM(importe_total), 2)    AS total_ventas,
    ROUND(AVG(importe_total), 2)    AS ticket_medio
  FROM pedidos
  GROUP BY categoria
  ORDER BY total_ventas DESC
""")

println("=== Ventas por categoría ===")
ventasPorCategoria.show()
```

**Salida esperada:**

```
=== Ventas por categoría ===
+------------+-----------+------------+------------+
|   categoria|num_pedidos|total_ventas|ticket_medio|
+------------+-----------+------------+------------+
| electronica|          4|     2889.96|      722.49|
|        ropa|          3|      311.0 |      103.67|
|       hogar|          3|      838.5 |      279.5 |
+------------+-----------+------------+------------+
```

**Celda Code:**

```scala
// Consulta más compleja: top ciudad por categoría
val topCiudadCategoria = spark.sql("""
  SELECT ciudad, categoria, ROUND(SUM(importe_total), 2) AS total
  FROM pedidos
  GROUP BY ciudad, categoria
  ORDER BY ciudad, total DESC
""")

println("=== Ventas por ciudad y categoría ===")
topCiudadCategoria.show()
```

---

### P4 — Simular ingestión de streaming desde ficheros JSON (estilo Kafka)

Kafka envía mensajes en formato JSON. Simularemos este flujo creando ficheros JSON que Spark leerá como stream.

```
## P4 — Micro-batch streaming desde ficheros JSON (simula Kafka)
```

**Celda Code:**

```scala
import org.apache.spark.sql.types._

// Definir el esquema de los mensajes (en Kafka, vendrían como bytes → JSON)
val schemaMensajes = StructType(Array(
  StructField("id_evento",  StringType,  nullable = false),
  StructField("timestamp",  LongType,    nullable = false),
  StructField("tipo",       StringType,  nullable = false),
  StructField("ciudad",     StringType,  nullable = false),
  StructField("importe",    DoubleType,  nullable = true)
))

// Crear mensajes de ejemplo (simulan lo que Kafka enviaría)
val mensajes = Seq(
  """{"id_evento":"evt_001","timestamp":1704844800,"tipo":"pago","ciudad":"Madrid","importe":150.0}""",
  """{"id_evento":"evt_002","timestamp":1704844860,"tipo":"pago","ciudad":"Barcelona","importe":89.5}""",
  """{"id_evento":"evt_003","timestamp":1704844920,"tipo":"devolucion","ciudad":"Madrid","importe":45.0}""",
  """{"id_evento":"evt_004","timestamp":1704844980,"tipo":"pago","ciudad":"Sevilla","importe":210.0}""",
  """{"id_evento":"evt_005","timestamp":1704845040,"tipo":"pago","ciudad":"Madrid","importe":320.0}"""
)

// Parsear los JSON (en Kafka haríamos .cast("string") sobre la columna "value")
val dfEventos = spark.read.json(mensajes.toDS())

println("=== Eventos recibidos (simulación Kafka) ===")
dfEventos.show(truncate = false)
dfEventos.printSchema()
```

**Salida esperada:**

```
=== Eventos recibidos (simulación Kafka) ===
+---------+----------+------+---------+----------+
|   ciudad|id_evento |importe|timestamp|    tipo  |
+---------+----------+------+---------+----------+
|   Madrid|  evt_001 | 150.0|1704844800|    pago  |
|Barcelona|  evt_002 |  89.5|1704844860|    pago  |
|   Madrid|  evt_003 |  45.0|1704844920|devolucion|
...
```

**Celda Code:**

```scala
// Procesar: filtrar solo pagos y agregar por ciudad
val resumenPagos = dfEventos
  .filter($"tipo" === "pago")
  .groupBy("ciudad")
  .agg(
    count("*").as("num_pagos"),
    round(sum("importe"), 2).as("total_recaudado")
  )
  .orderBy($"total_recaudado".desc)

println("=== Resumen de pagos (resultado del pipeline de streaming) ===")
resumenPagos.show()
```

**Salida esperada:**

```
=== Resumen de pagos (resultado del pipeline de streaming) ===
+---------+---------+---------------+
|   ciudad|num_pagos|total_recaudado|
+---------+---------+---------------+
|   Madrid|        2|          470.0|
|  Sevilla|        1|          210.0|
|Barcelona|        1|           89.5|
+---------+---------+---------------+
```

---

### P5 — Introducción a Delta Lake: escritura y lectura

```
## P5 — Delta Lake: escritura con ACID transactions
```

**Celda Code:**

```scala
val rutaDelta = "C:/Curso-Scala/datos/pedidos_delta/"

// Escribir en formato Delta (en lugar de Parquet puro)
pedidosConImporte.write
  .format("delta")
  .mode("overwrite")
  .save(rutaDelta)

println(s"Tabla Delta creada en: $rutaDelta")

// Leer la tabla Delta
val deltaDF = spark.read
  .format("delta")
  .load(rutaDelta)

deltaDF.show()
println(s"Filas en tabla Delta: ${deltaDF.count()}")
```

---

### P6 — Delta Lake: actualización de registros (imposible en Parquet puro)

```
## P6 — Delta Lake: UPDATE (operación ACID)
```

**Celda Code:**

```scala
import io.delta.tables._

val deltaTable = DeltaTable.forPath(spark, rutaDelta)

// Aplicar un descuento del 10% a todos los pedidos de electronica en Madrid
deltaTable.update(
  condition = $"categoria" === "electronica" && $"ciudad" === "Madrid",
  set = Map(
    "precio_unitario" -> ($"precio_unitario" * lit(0.9)),
    "importe_total"   -> (round($"precio_unitario" * lit(0.9) * $"cantidad", 2))
  )
)

println("=== Tabla después del UPDATE (descuento electronica Madrid) ===")
deltaTable.toDF
  .filter($"categoria" === "electronica")
  .orderBy("ciudad")
  .show()
```

---

### P7 — Delta Lake: Time Travel

```
## P7 — Delta Lake: Time Travel (leer versión anterior)
```

**Celda Code:**

```scala
// Ver el historial de operaciones (como 'git log')
println("=== Historial de la tabla Delta ===")
deltaTable.history().select("version", "timestamp", "operation").show(truncate = false)
```

**Celda Code:**

```scala
// Leer la versión original (antes del UPDATE)
val versionOriginal = spark.read
  .format("delta")
  .option("versionAsOf", "0")
  .load(rutaDelta)

println("=== Versión 0 (antes del descuento) — pedidos electronica Madrid ===")
versionOriginal
  .filter($"categoria" === "electronica" && $"ciudad" === "Madrid")
  .select("id", "ciudad", "categoria", "precio_unitario", "importe_total")
  .show()

println("=== Versión actual (con descuento aplicado) ===")
deltaTable.toDF
  .filter($"categoria" === "electronica" && $"ciudad" === "Madrid")
  .select("id", "ciudad", "categoria", "precio_unitario", "importe_total")
  .show()
```

**Salida esperada (versión 0):**

```
+---+------+------------+---------------+-------------+
| id|ciudad|   categoria|precio_unitario|importe_total|
+---+------+------------+---------------+-------------+
|  1|Madrid| electronica|         299.99|       599.98|
|  3|Madrid| electronica|         899.00|       899.00|
|  8|Madrid| electronica|         450.00|       450.00|
+---+------+------------+---------------+-------------+
```

---

### P8 — Pipeline completo: CSV → transformación → Delta Lake → consulta SQL

```
## P8 — Pipeline end-to-end: ingesta → procesado → Delta → análisis
```

**Celda Code:**

```scala
// Paso 1: Leer CSV (simula HDFS/S3)
val rawData = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(rutaCSV)

// Paso 2: Transformar (lógica de negocio)
val transformado = rawData
  .withColumn("importe_total", round($"precio_unitario" * $"cantidad", 2))
  .withColumn("fecha",         to_date($"fecha", "yyyy-MM-dd"))
  .withColumn("es_premium",    ($"importe_total" > 400).cast("boolean"))
  .filter($"importe_total" > 0)

// Paso 3: Escribir en Delta Lake
val rutaPipeline = "C:/Curso-Scala/datos/pipeline_resultado/"
transformado.write
  .format("delta")
  .mode("overwrite")
  .partitionBy("ciudad")
  .save(rutaPipeline)

println("Pipeline completado. Registrando como vista temporal...")

// Paso 4: Registrar como vista y consultar con SQL (simula Hive)
spark.read.format("delta").load(rutaPipeline)
  .createOrReplaceTempView("resultados_pipeline")

val analisis = spark.sql("""
  SELECT
    ciudad,
    categoria,
    COUNT(*)                              AS num_pedidos,
    ROUND(SUM(importe_total), 2)          AS total_ventas,
    SUM(CASE WHEN es_premium THEN 1 ELSE 0 END) AS pedidos_premium
  FROM resultados_pipeline
  GROUP BY ciudad, categoria
  ORDER BY total_ventas DESC
""")

println("=== Análisis final del pipeline ===")
analisis.show()
```

---