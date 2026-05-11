# Tutorial completo: IntelliJ IDEA + Java 17 + Coursier + Scala 2.13.18 + Spark 4.1.1 en Windows

> **Contexto**: este tutorial está pensado para equipos Windows donde el usuario tiene una ruta problemática, por ejemplo `C:\Users\Imp_06 - Mañana`. Para evitar errores con espacios, acentos o `ñ`, instalaremos y configuraremos las partes conflictivas en rutas limpias bajo `C:\`, principalmente `C:\SparkDev` y `C:\ScalaProjects`.

---

## 1. Objetivo final

Al terminar, el equipo tendrá validado:

| Componente | Versión / ruta recomendada |
|---|---|
| Java | Eclipse Temurin JDK 17 |
| Scala | 2.13.18 |
| sbt | 1.12.x |
| Spark | 4.1.1 |
| IntelliJ IDEA | versión gratuita / Community si aparece como opción |
| Coursier | `C:\SparkDev\Coursier` |
| Caché Coursier | `C:\SparkDev\CoursierCache` |
| Caché sbt/Ivy | `C:\SparkDev\SbtData` |
| Temporales Java/Spark | `C:\SparkDev\Temp` |
| Hadoop local Windows | `C:\SparkDev\Hadoop` |
| Proyectos | `C:\ScalaProjects` |

Validaremos:

1. Hola Mundo en Scala.
2. Proyecto Spark 4.1.1 con Scala 2.13.18.
3. DataFrame desde `Seq`.
4. Lectura CSV.
5. Lectura JSON Lines.
6. Lectura JSON multilínea.
7. Escritura y lectura Parquet.

---

## 2. Enlaces oficiales y ejecutables

### Java 17

Descarga Eclipse Temurin JDK 17 desde Adoptium:

```text
https://adoptium.net/temurin/releases?arch=x64&mode=filter&os=windows&version=17
```

Página general:

```text
https://adoptium.net/temurin/releases
```

Instalación en Windows:

```text
https://adoptium.net/installation/windows
```

### IntelliJ IDEA

Página oficial:

```text
https://www.jetbrains.com/idea/download/
```

Otras versiones:

```text
https://www.jetbrains.com/idea/download/other/
```

Guía oficial:

```text
https://www.jetbrains.com/help/idea/installation-guide.html
```

En las pruebas se usó:

```text
idea-2026.1.1.exe
```

### Coursier

Documentación oficial:

```text
https://get-coursier.io/docs/cli-installation
```

Launcher usado:

```text
https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-win32.zip
```

### winutils para Hadoop en Windows

Repositorio usado:

```text
https://github.com/cdarlint/winutils
```

Archivos descargados:

```text
https://github.com/cdarlint/winutils/raw/refs/heads/master/hadoop-3.3.6/bin/winutils.exe
https://github.com/cdarlint/winutils/raw/refs/heads/master/hadoop-3.3.6/bin/hadoop.dll
```

> Aunque Spark 4.1.1 carga Hadoop 3.4.2, en el entorno probado funcionaron correctamente los binarios de `hadoop-3.3.6` para prácticas locales en Windows.

---

## 3. Crear carpetas limpias en `C:\`

Abre **PowerShell como administrador** y ejecuta:

```powershell
New-Item -ItemType Directory -Force -Path "C:\SparkDev"
New-Item -ItemType Directory -Force -Path "C:\SparkDev\Temp"
New-Item -ItemType Directory -Force -Path "C:\SparkDev\Coursier"
New-Item -ItemType Directory -Force -Path "C:\SparkDev\Coursier\bin"
New-Item -ItemType Directory -Force -Path "C:\SparkDev\CoursierCache"
New-Item -ItemType Directory -Force -Path "C:\SparkDev\SbtData"
New-Item -ItemType Directory -Force -Path "C:\ScalaProjects"
```

---

## 4. Instalar Java 17

Descarga desde:

```text
https://adoptium.net/temurin/releases?arch=x64&mode=filter&os=windows&version=17
```

Selecciona:

```text
Windows x64
JDK 17
MSI Installer
```

Ruta observada en el equipo probado:

```text
C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot
```

---

## 5. Verificar Java

Abre PowerShell y ejecuta:

```powershell
java -version
javac -version
where.exe java
where.exe javac
echo $env:JAVA_HOME
```

Salida correcta esperada:

```text
openjdk version "17.0.18"
javac 17.0.18
```

Y `where.exe java` debería mostrar primero algo como:

```text
C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot\bin\java.exe
```

---

## 6. Corregir `JAVA_HOME` si hay otras versiones de Java

En muchos equipos puede haber Java 8, Java de Oracle u otras versiones. Un caso detectado fue:

```powershell
echo $env:JAVA_HOME
```

Resultado incorrecto:

```text
C:\Program Files\Java\jdk1.8.0_202
```

Pero `java -version` devolvía Java 17. Eso significa que el `PATH` usa Java 17, pero `JAVA_HOME` sigue apuntando a Java 8.

Comprueba variables de usuario y sistema:

```powershell
[Environment]::GetEnvironmentVariable("JAVA_HOME", "User")
[Environment]::GetEnvironmentVariable("JAVA_HOME", "Machine")
```

Elimina `JAVA_HOME` de usuario si apunta a Java 8:

```powershell
[Environment]::SetEnvironmentVariable("JAVA_HOME", $null, "User")
```

Configura `JAVA_HOME` de sistema:

```powershell
[Environment]::SetEnvironmentVariable(
  "JAVA_HOME",
  "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot",
  "Machine"
)
```

Cierra todas las terminales y abre una nueva.

Comprueba:

```powershell
echo $env:JAVA_HOME
java -version
javac -version
where.exe java
where.exe javac
```

Esperado:

```text
C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot
openjdk version "17.0.18"
javac 17.0.18
```

---

## 7. Configurar variables limpias para Spark, sbt y Coursier

Abre **PowerShell como administrador**.

Configura temporales:

```powershell
[Environment]::SetEnvironmentVariable("TEMP", "C:\SparkDev\Temp", "User")
[Environment]::SetEnvironmentVariable("TMP", "C:\SparkDev\Temp", "User")
[Environment]::SetEnvironmentVariable("TEMP", "C:\SparkDev\Temp", "Machine")
[Environment]::SetEnvironmentVariable("TMP", "C:\SparkDev\Temp", "Machine")
```

Configura caché de Coursier:

```powershell
[Environment]::SetEnvironmentVariable(
  "COURSIER_CACHE",
  "C:\SparkDev\CoursierCache",
  "Machine"
)
```

Configura `SBT_OPTS`:

```powershell
$SbtOpts = "-Dsbt.global.base=C:\SparkDev\SbtData\.sbt -Dsbt.boot.directory=C:\SparkDev\SbtData\.sbt\boot -Dsbt.ivy.home=C:\SparkDev\SbtData\.ivy2 -Djava.io.tmpdir=C:\SparkDev\Temp -Dcoursier.cache=C:\SparkDev\CoursierCache"

[Environment]::SetEnvironmentVariable("SBT_OPTS", $SbtOpts, "Machine")
```

Elimina `_JAVA_OPTIONS` global, si existe:

```powershell
[Environment]::SetEnvironmentVariable("_JAVA_OPTIONS", $null, "User")
[Environment]::SetEnvironmentVariable("_JAVA_OPTIONS", $null, "Machine")
```

Cierra PowerShell y abre uno nuevo.

Comprueba:

```powershell
echo $env:TEMP
echo $env:TMP
echo $env:COURSIER_CACHE
echo $env:SBT_OPTS
echo $env:_JAVA_OPTIONS
```

Esperado:

```text
C:\SparkDev\Temp
C:\SparkDev\Temp
C:\SparkDev\CoursierCache
-Dsbt.global.base=C:\SparkDev\SbtData\.sbt ...
```

`_JAVA_OPTIONS` debería aparecer vacío.

---

## 8. Instalar Coursier en `C:\SparkDev\Coursier`

Abre **PowerShell como administrador**:

```powershell
cd C:\SparkDev\Coursier
```

Descarga Coursier:

```powershell
Invoke-WebRequest `
  -Uri "https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-win32.zip" `
  -OutFile "cs-x86_64-pc-win32.zip"
```

Descomprime:

```powershell
Expand-Archive -Path "cs-x86_64-pc-win32.zip" -DestinationPath .
```

Renombra:

```powershell
Rename-Item -Path "cs-x86_64-pc-win32.exe" -NewName "cs.exe"
```

Elimina ZIP:

```powershell
Remove-Item -Path "cs-x86_64-pc-win32.zip"
```

Prueba:

```powershell
C:\SparkDev\Coursier\cs.exe --help
```

---

## 9. Ejecutar `cs setup`

Ejecuta:

```powershell
C:\SparkDev\Coursier\cs.exe setup
```

Cuando pregunte si deseas añadir Coursier al `PATH`, responde:

```text
Y
```

Puede instalar:

```text
scala
scalac
sbt
scala-cli
scalafmt
```

> Nota: `cs setup` puede instalar accesos en `C:\Users\<usuario>\AppData\Local\Coursier\data\bin`. Para no depender del usuario, también instalaremos launchers en `C:\SparkDev\Coursier\bin`.

---

## 10. Añadir Coursier limpio al `PATH` del sistema

PowerShell como administrador:

```powershell
$machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
$newPath = "C:\SparkDev\Coursier;C:\SparkDev\Coursier\bin;" + $machinePath
[Environment]::SetEnvironmentVariable("Path", $newPath, "Machine")
```

Cierra PowerShell y abre uno nuevo.

Comprueba:

```powershell
where.exe cs
```

Debe aparecer primero:

```text
C:\SparkDev\Coursier\cs.exe
```

---

## 11. Instalar launchers de Scala 2.13.18 y sbt en ruta limpia

```powershell
New-Item -ItemType Directory -Force -Path "C:\SparkDev\Coursier\bin"
cs install --install-dir "C:\SparkDev\Coursier\bin" scala:2.13.18 scalac:2.13.18 sbt
```

Comprueba:

```powershell
where.exe scala
where.exe scalac
where.exe sbt
```

Prueba Scala 2.13.18 explícitamente:

```powershell
cs launch scala:2.13.18 -- -version
```

Salida esperada:

```text
Scala code runner version 2.13.18
```

---

## 12. Instalar IntelliJ IDEA

Descarga desde:

```text
https://www.jetbrains.com/idea/download/
```

Ejecuta el instalador, por ejemplo:

```text
idea-2026.1.1.exe
```

Ruta recomendada:

```text
C:\Program Files\JetBrains\IntelliJ IDEA 2026.1.1
```

Durante la instalación puedes marcar:

```text
Create Desktop Shortcut
Add "bin" folder to PATH
Add "Open Folder as Project"
```

---

## 13. Instalar plugin Scala en IntelliJ

En IntelliJ:

```text
Plugins > Marketplace
```

Busca:

```text
Scala
```

Instala el plugin oficial:

```text
Scala - JetBrains
```

Reinicia IntelliJ.

Verifica:

```text
Plugins > Installed > Scala
```

Si aparece `Disable`, significa que está instalado y habilitado.

---

## 14. Instalar Big Data Tools 

Para este tutorial local con Spark no es obligatorio, pero para un entorno mas profesional
 de Big Data puede instalarse:

```text
Plugins > Marketplace > Big Data Tools
```

Reinicia IntelliJ.

> Los ejemplos de este tutorial no dependen de Big Data Tools. Funcionan con Scala, sbt y dependencias Spark.

---

## 15. Crear proyecto de prueba `HolaMundoScala`

En IntelliJ:

```text
New Project
```

Configura:

```text
Name: HolaMundoScala
Location: C:\ScalaProjects\HolaMundoScala
Language: Scala
Build system: sbt
JDK: Eclipse Temurin 17.0.18
sbt: 1.12.x
Scala: 2.13.18
```

Pulsa:

```text
Create
```

Espera a que sbt sincronice.

---

## 16. Crear `Main.scala` para Hola Mundo

Ruta:

```text
src/main/scala
```

Clic derecho:

```text
New > Scala Class/File > Object
```

Nombre:

```text
Main
```

Código:

```scala
object Main {
  def main(args: Array[String]): Unit = {
    println("Hola mundo desde Scala 2.13.18")
    println(s"Versión de Scala: ${scala.util.Properties.versionNumberString}")
    println(s"Versión de Java: ${System.getProperty("java.version")}")
    println(s"Java Home: ${System.getProperty("java.home")}")
  }
}
```

Ejecuta:

```text
Run 'Main'
```

Salida esperada:

```text
Hola mundo desde Scala 2.13.18
Versión de Scala: 2.13.18
Versión de Java: 17.0.18
Java Home: C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot
Process finished with exit code 0
```

---

## 17. Crear proyecto Spark `Spark411BigData`

Cierra el proyecto anterior:

```text
File > Close Project
```

Crea nuevo proyecto:

```text
New Project
```

Configura:

```text
Name: Spark411BigData
Location: C:\ScalaProjects\Spark411BigData
Language: Scala
Build system: sbt
JDK: Eclipse Temurin 17.0.18
sbt: 1.12.x
Scala: 2.13.18
Download sources: marcado
```

Pulsa:

```text
Create
```

Ruta observada:

```text
C:\ScalaProjects\Spark411BigData\Spark411BigData
```

---

## 18. Configurar `build.sbt` para Spark 4.1.1

Abre `build.sbt` y déjalo así:

```scala
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "Spark411BigData",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"  % "4.1.1",
      "org.apache.spark" %% "spark-sql"   % "4.1.1",
      "org.apache.spark" %% "spark-mllib" % "4.1.1",
      "io.dropwizard.metrics" % "metrics-core" % "4.2.37"
    )
  )
```

La dependencia `metrics-core` evita el error:

```text
NoClassDefFoundError: com/codahale/metrics/MetricRegistry
```

---

## 19. Sincronizar sbt en IntelliJ

Puede aparecer como:

```text
Load sbt Changes
```

O:

```text
Sync sbt Changes
```

También puedes usar:

```text
Ctrl + Shift + O
```

En la ventana lateral de sbt, pulsa el icono de recarga.

Una sincronización correcta muestra:

```text
[info] Done.
[success] Total time: ...
```

---

## 20. Resolver bloqueo de servidor sbt

Puede aparecer:

```text
sbt thinks that server is already booting
Could not create lock for \\.\pipe\sbt-load..._lock, error 5
Create a new server? y/n
```

Responde:

```text
y
```

Si sigue fallando:

```powershell
taskkill /F /IM java.exe
```

Y limpia generados:

```powershell
cd C:\ScalaProjects\Spark411BigData\Spark411BigData
Remove-Item -Recurse -Force ".bsp" -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force "target" -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force "project\target" -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force "project\project" -ErrorAction SilentlyContinue
```

Después abre IntelliJ y sincroniza de nuevo.

También puedes validar desde terminal:

```powershell
sbt clean update compile
```

---

## 21. Primer ejemplo Spark: DataFrame desde `Seq`

Crea `src/main/scala/Main.scala` como `Object Main`.

```scala
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark 4.1.1 Big Data con Scala 2.13.18")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("======================================")
    println(s"Spark version: ${spark.version}")
    println(s"Scala version: ${scala.util.Properties.versionNumberString}")
    println(s"Java version: ${System.getProperty("java.version")}")
    println(s"Java home: ${System.getProperty("java.home")}")
    println("======================================")

    import spark.implicits._

    val ventasDF = Seq(
      ("Madrid", "Portatil", 1200.0),
      ("Madrid", "Monitor", 300.0),
      ("Barcelona", "Portatil", 1500.0),
      ("Valencia", "Tablet", 450.0),
      ("Barcelona", "Monitor", 250.0),
      ("Madrid", "Tablet", 500.0)
    ).toDF("ciudad", "producto", "importe")

    println("Datos originales:")
    ventasDF.show(false)

    println("Ventas totales por ciudad:")
    ventasDF
      .groupBy("ciudad")
      .sum("importe")
      .show(false)

    spark.stop()
  }
}
```

Salida esperada:

```text
Spark version: 4.1.1
Scala version: 2.13.18
Java version: 17.0.18
Process finished with exit code 0
```

---

## 22. Crear carpeta `data`

```powershell
New-Item -ItemType Directory -Force -Path "C:\ScalaProjects\Spark411BigData\Spark411BigData\data"
```

---

## 23. Ejemplo: leer CSV

Crea `ventas.csv`:

```powershell
@"
ciudad,producto,importe
Madrid,Portatil,1200
Madrid,Monitor,300
Barcelona,Portatil,1500
Valencia,Tablet,450
Barcelona,Monitor,250
Madrid,Tablet,500
Sevilla,Teclado,80
Sevilla,Monitor,220
Madrid,Teclado,90
Valencia,Portatil,1300
"@ | Set-Content -Encoding UTF8 "C:\ScalaProjects\Spark411BigData\Spark411BigData\data\ventas.csv"
```

Código:

```scala
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Lectura CSV con Spark 4.1.1 y Scala 2.13.18")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val rutaCSV = "C:/ScalaProjects/Spark411BigData/Spark411BigData/data/ventas.csv"

    val ventasDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(rutaCSV)

    println("Datos leídos desde CSV:")
    ventasDF.show(false)

    println("Esquema detectado:")
    ventasDF.printSchema()

    println("Ventas totales por ciudad:")
    ventasDF.groupBy("ciudad").sum("importe").show(false)

    println("Ventas totales por producto:")
    ventasDF.groupBy("producto").sum("importe").show(false)

    println("Ventas superiores a 500 euros:")
    ventasDF.filter("importe > 500").show(false)

    spark.stop()
  }
}
```

---

## 24. Ejemplo: leer JSON Lines

Crea `ventas.json`:

```powershell
@"
{"ciudad":"Madrid","producto":"Portatil","importe":1200}
{"ciudad":"Madrid","producto":"Monitor","importe":300}
{"ciudad":"Barcelona","producto":"Portatil","importe":1500}
{"ciudad":"Valencia","producto":"Tablet","importe":450}
{"ciudad":"Barcelona","producto":"Monitor","importe":250}
{"ciudad":"Madrid","producto":"Tablet","importe":500}
{"ciudad":"Sevilla","producto":"Teclado","importe":80}
{"ciudad":"Sevilla","producto":"Monitor","importe":220}
{"ciudad":"Madrid","producto":"Teclado","importe":90}
{"ciudad":"Valencia","producto":"Portatil","importe":1300}
"@ | Set-Content -Encoding UTF8 "C:\ScalaProjects\Spark411BigData\Spark411BigData\data\ventas.json"
```

Código:

```scala
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Lectura JSON con Spark 4.1.1 y Scala 2.13.18")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val rutaJSON = "C:/ScalaProjects/Spark411BigData/Spark411BigData/data/ventas.json"

    val ventasDF = spark.read
      .option("inferSchema", "true")
      .json(rutaJSON)

    println("Datos leídos desde JSON:")
    ventasDF.show(false)

    println("Esquema detectado:")
    ventasDF.printSchema()

    ventasDF.groupBy("ciudad").sum("importe").show(false)
    ventasDF.filter("importe > 500").show(false)

    spark.stop()
  }
}
```

---

## 25. Ejemplo: leer JSON multilínea

Crea `ventas_multilinea.json`:

```powershell
@"
[
  { "ciudad": "Madrid", "producto": "Portatil", "importe": 1200 },
  { "ciudad": "Madrid", "producto": "Monitor", "importe": 300 },
  { "ciudad": "Barcelona", "producto": "Portatil", "importe": 1500 },
  { "ciudad": "Valencia", "producto": "Tablet", "importe": 450 },
  { "ciudad": "Barcelona", "producto": "Monitor", "importe": 250 },
  { "ciudad": "Madrid", "producto": "Tablet", "importe": 500 },
  { "ciudad": "Sevilla", "producto": "Teclado", "importe": 80 },
  { "ciudad": "Sevilla", "producto": "Monitor", "importe": 220 },
  { "ciudad": "Madrid", "producto": "Teclado", "importe": 90 },
  { "ciudad": "Valencia", "producto": "Portatil", "importe": 1300 }
]
"@ | Set-Content -Encoding UTF8 "C:\ScalaProjects\Spark411BigData\Spark411BigData\data\ventas_multilinea.json"
```

Código:

```scala
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Lectura JSON multilínea con Spark 4.1.1 y Scala 2.13.18")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val rutaJSON = "C:/ScalaProjects/Spark411BigData/Spark411BigData/data/ventas_multilinea.json"

    val ventasDF = spark.read
      .option("multiLine", "true")
      .option("inferSchema", "true")
      .json(rutaJSON)

    println("Datos leídos desde JSON multilínea:")
    ventasDF.show(false)

    println("Esquema detectado:")
    ventasDF.printSchema()

    ventasDF.groupBy("ciudad").sum("importe").show(false)
    ventasDF.filter("importe > 500").show(false)

    spark.stop()
  }
}
```

La clave es:

```scala
.option("multiLine", "true")
```

---

## 26. Configurar Hadoop local para escribir Parquet en Windows

Al escribir Parquet puede aparecer:

```text
java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset
```

Esto ocurre porque Spark usa componentes de Hadoop para escribir archivos.

PowerShell como administrador:

```powershell
New-Item -ItemType Directory -Force -Path "C:\SparkDev\Hadoop\bin"
```

Descarga `winutils.exe`:

```powershell
Invoke-WebRequest `
  -Uri "https://github.com/cdarlint/winutils/raw/refs/heads/master/hadoop-3.3.6/bin/winutils.exe" `
  -OutFile "C:\SparkDev\Hadoop\bin\winutils.exe"
```

Descarga `hadoop.dll`:

```powershell
Invoke-WebRequest `
  -Uri "https://github.com/cdarlint/winutils/raw/refs/heads/master/hadoop-3.3.6/bin/hadoop.dll" `
  -OutFile "C:\SparkDev\Hadoop\bin\hadoop.dll"
```

Verifica:

```powershell
Get-ChildItem "C:\SparkDev\Hadoop\bin"
```

Debe aparecer:

```text
hadoop.dll
winutils.exe
```

Configura `HADOOP_HOME`:

```powershell
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\SparkDev\Hadoop", "Machine")
```

Añade Hadoop al `PATH`:

```powershell
$machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")

if ($machinePath -notlike "*C:\SparkDev\Hadoop\bin*") {
    [Environment]::SetEnvironmentVariable(
        "Path",
        "C:\SparkDev\Hadoop\bin;" + $machinePath,
        "Machine"
    )
}
```

Cierra IntelliJ y PowerShell. Abre PowerShell de nuevo y comprueba:

```powershell
echo $env:HADOOP_HOME
where.exe winutils
winutils.exe
```

Salida esperada:

```text
C:\SparkDev\Hadoop
C:\SparkDev\Hadoop\bin\winutils.exe
Usage: C:\SparkDev\Hadoop\bin\winutils.exe [command] ...
```

---

## 27. Ejemplo: escribir y leer Parquet

Crea carpeta de salida:

```powershell
New-Item -ItemType Directory -Force -Path "C:\ScalaProjects\Spark411BigData\Spark411BigData\output"
```

Código:

```scala
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Escritura y lectura Parquet con Spark 4.1.1")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("======================================")
    println(s"Spark version: ${spark.version}")
    println(s"Scala version: ${scala.util.Properties.versionNumberString}")
    println(s"Java version: ${System.getProperty("java.version")}")
    println(s"Java home: ${System.getProperty("java.home")}")
    println("======================================")

    val rutaJSON = "C:/ScalaProjects/Spark411BigData/Spark411BigData/data/ventas_multilinea.json"
    val rutaParquet = "C:/ScalaProjects/Spark411BigData/Spark411BigData/output/ventas_parquet"

    val ventasDF = spark.read
      .option("multiLine", "true")
      .option("inferSchema", "true")
      .json(rutaJSON)

    println("Datos originales leídos desde JSON multilínea:")
    ventasDF.show(false)

    println("Esquema original:")
    ventasDF.printSchema()

    ventasDF.write
      .mode("overwrite")
      .parquet(rutaParquet)

    println(s"Datos escritos correctamente en Parquet en: $rutaParquet")

    val ventasParquetDF = spark.read
      .parquet(rutaParquet)

    println("Datos leídos desde Parquet:")
    ventasParquetDF.show(false)

    println("Esquema leído desde Parquet:")
    ventasParquetDF.printSchema()

    println("Ventas totales por ciudad desde Parquet:")
    ventasParquetDF.groupBy("ciudad").sum("importe").show(false)

    println("Ventas superiores a 500 euros desde Parquet:")
    ventasParquetDF.filter("importe > 500").show(false)

    spark.stop()
  }
}
```

Salida correcta:

```text
Datos escritos correctamente en Parquet en: C:/ScalaProjects/Spark411BigData/Spark411BigData/output/ventas_parquet
Process finished with exit code 0
```

---

## 28. Verificar archivos Parquet

PowerShell:

```powershell
Get-ChildItem "C:\ScalaProjects\Spark411BigData\Spark411BigData\output\ventas_parquet"
```

Salida esperada:

```text
part-00000-....snappy.parquet
_SUCCESS
.part-00000-....crc
._SUCCESS.crc
```

Interpretación:

| Archivo | Significado |
|---|---|
| `part-00000-....snappy.parquet` | Archivo real de datos Parquet |
| `_SUCCESS` | Marca de escritura correcta |
| `.crc` | Archivos checksum de Hadoop |
| Carpeta `ventas_parquet` | Dataset Parquet completo |

Spark normalmente no escribe un único archivo llamado `ventas.parquet`; escribe una carpeta con archivos `part-...`.

---

## 29. Avisos normales en Windows

Pueden aparecer:

```text
WARN Shell: Did not find winutils.exe
WARN NativeCodeLoader: Unable to load native-hadoop library
```

Después de configurar Hadoop local con:

```text
HADOOP_HOME = C:\SparkDev\Hadoop
PATH incluye C:\SparkDev\Hadoop\bin
```

el ejemplo Parquet debe funcionar.

También pueden aparecer mensajes `INFO` iniciales aunque se use:

```scala
spark.sparkContext.setLogLevel("WARN")
```

Esto ocurre porque esos mensajes aparecen durante la creación del `SparkContext`, antes de que se aplique el nivel de log.

---

## 30. Estructura final recomendada

```text
C:\
├── SparkDev
│   ├── Coursier
│   │   ├── cs.exe
│   │   └── bin
│   ├── CoursierCache
│   ├── Hadoop
│   │   └── bin
│   │       ├── hadoop.dll
│   │       └── winutils.exe
│   ├── SbtData
│   └── Temp
│
└── ScalaProjects
    ├── HolaMundoScala
    └── Spark411BigData
        └── Spark411BigData
            ├── build.sbt
            ├── src
            │   └── main
            │       └── scala
            │           └── Main.scala
            ├── data
            │   ├── ventas.csv
            │   ├── ventas.json
            │   └── ventas_multilinea.json
            └── output
                └── ventas_parquet
```

---

## 31. Validación final

| Paso | Estado esperado |
|---|---|
| Java 17 | Correcto |
| `JAVA_HOME` | Apunta a JDK 17 |
| Coursier | Correcto |
| Scala 2.13.18 | Correcto |
| sbt 1.12.x | Correcto |
| IntelliJ IDEA | Correcto |
| Plugin Scala | Correcto |
| Proyecto `HolaMundoScala` | Correcto |
| Proyecto `Spark411BigData` | Correcto |
| Spark 4.1.1 | Correcto |
| DataFrame desde `Seq` | Correcto |
| Lectura CSV | Correcto |
| Lectura JSON Lines | Correcto |
| Lectura JSON multilínea | Correcto |
| Hadoop local Windows | Correcto |
| Escritura Parquet | Correcto |
| Lectura Parquet | Correcto |
| Temporales en `C:\SparkDev\Temp` | Correcto |
| Caché Coursier en `C:\SparkDev\CoursierCache` | Correcto |
| Caché sbt/Ivy en `C:\SparkDev\SbtData` | Correcto |

---

## 32. Conclusión

Con esta configuración, se puede trabajar desde usuarios Windows problemáticos como:

```text
C:\Users\Imp_06 - Mañana
```

sin que Scala, sbt, Spark o Hadoop dependan de rutas con espacios, acentos o `ñ`.

La configuración centraliza los elementos conflictivos en:

```text
C:\SparkDev
C:\ScalaProjects
```

Esto reduce errores en:

- sbt
- Coursier
- Spark
- Hadoop local
- escritura Parquet
- temporales Java/Spark
- cachés de dependencias
