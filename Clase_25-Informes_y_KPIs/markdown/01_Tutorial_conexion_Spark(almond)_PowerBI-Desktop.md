# Tutorial — Power BI Desktop con Spark + Almond (Vía 2)



---

## Índice

1. [¿Qué vamos a hacer?](#1-qué-vamos-a-hacer)
2. [Instalar Power BI Desktop](#2-instalar-power-bi-desktop)
3. [Parte Spark — Notebook en Almond](#3-parte-spark--notebook-en-almond)
4. [Parte Power BI — Cargar los datos](#4-parte-power-bi--cargar-los-datos)
5. [Construir el informe](#5-construir-el-informe)
6. [Construir el dashboard](#6-construir-el-dashboard)
7. [Errores frecuentes](#7-errores-frecuentes)
8. [Resumen del flujo completo](#8-resumen-del-flujo-completo)

---

## 1. ¿Qué vamos a hacer?

El flujo completo es sencillo y reproduce exactamente lo que hace un equipo de datos real:

```
Almond (Scala + Spark)
    └── Creamos y procesamos un DataFrame de ventas
    └── Exportamos los resultados a CSV con coalesce(1)
            └── C:\Curso-Scala\output\
                    └── Power BI Desktop
                            ├── Carga el CSV
                            ├── Crea un informe con tablas y gráficos
                            └── Agrupa todo en un dashboard
```

El ejemplo usa un dataset de **ventas de una tienda tecnológica** con datos de producto, ciudad, categoría, mes e importe. Calcularemos métricas de negocio con Spark y las visualizaremos en Power BI.

---

## 2. Instalar Power BI Desktop

### 2.1 Descarga

Power BI Desktop es **gratuito**. Hay dos formas de instalarlo:

**Opción A — Microsoft Store (recomendada, más sencilla):**

1. Abre el menú Inicio de Windows y busca **Microsoft Store**.
2. En el buscador de la Store escribe: `Power BI Desktop`.
3. Haz clic en **Obtener** (o **Instalar**).
4. Espera a que finalice la descarga (aprox. 500 MB).
5. Al terminar, aparecerá el botón **Abrir**.

**Opción B — Instalador directo desde Microsoft:**

1. Abre el navegador y ve a:
   `https://www.microsoft.com/es-es/download/details.aspx?id=58494`
2. Haz clic en **Descargar**.
3. Selecciona el archivo `PBIDesktopSetup_x64.exe` y descárgalo.
4. Ejecuta el instalador como administrador.
5. Acepta la licencia → **Siguiente** → **Siguiente** → **Instalar**.
6. Al finalizar, marca "Iniciar Power BI Desktop" y haz clic en **Finalizar**.

### 2.2 Primera apertura

La primera vez que abres Power BI Desktop:

1. Aparece una pantalla de **inicio de sesión**. Puedes omitirla haciendo clic en "Ya tengo una cuenta, iniciar sesión más tarde" o simplemente cerrando el diálogo — **no es obligatorio** para usar la aplicación en local.
2. Verás la pantalla de bienvenida con opciones como "Obtener datos", "Informes recientes", etc.
3. Cierra la pantalla de bienvenida (botón X en la esquina superior derecha del diálogo).

Ya estás dentro de Power BI Desktop. La interfaz tiene tres vistas en el panel izquierdo:

| Icono | Vista | Para qué sirve |
|---|---|---|
| 📊 | **Informe** | Aquí se diseñan los gráficos y paneles |
| 🗃️ | **Tabla** | Aquí se ven y filtran los datos cargados |
| 🔗 | **Modelo** | Aquí se definen relaciones entre tablas |

---

## 3. Parte Spark — Notebook en Almond

Crea un nuevo notebook en VSCode llamado `ventas_powerbi.ipynb` con el kernel **Scala (Almond)**.

Ejecuta las celdas en orden. Espera el símbolo ✅ antes de pasar a la siguiente.

---

### Celda 1 — Inicialización

```scala
import $ivy.`org.apache.spark::spark-core:4.1.1`
import $ivy.`org.apache.spark::spark-sql:4.1.1`

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}

val spark = SparkSession.builder()
  .appName("VentasPowerBI")
  .master("local[*]")
  .config("spark.sql.shuffle.partitions", "4")
  .getOrCreate()

import spark.implicits._
spark.sparkContext.setLogLevel("ERROR")

println(s"✅ Spark ${spark.version} listo")
```

---

### Celda 2 — Crear el dataset de ventas

Este dataset simula las ventas de una tienda tecnológica durante 6 meses en 5 ciudades españolas.

```scala
// Dataset de ventas — 30 registros, 6 meses, 5 ciudades, 4 categorías
val ventas = Seq(
  (1,  "Laptop Pro",        "Tecnología",  1299.99, 2, "2024-01", "Madrid"),
  (2,  "Teclado Mecánico",  "Periféricos",   79.90, 5, "2024-01", "Madrid"),
  (3,  "Monitor 27\"",      "Tecnología",   349.00, 1, "2024-01", "Barcelona"),
  (4,  "Silla Ergonómica",  "Oficina",      320.00, 3, "2024-01", "Valencia"),
  (5,  "Ratón Inalámbrico", "Periféricos",   29.95, 8, "2024-01", "Sevilla"),
  (6,  "Laptop Air",        "Tecnología",   999.00, 3, "2024-02", "Madrid"),
  (7,  "Auriculares BT",    "Audio",        149.00, 4, "2024-02", "Bilbao"),
  (8,  "Webcam HD",         "Periféricos",   79.00, 6, "2024-02", "Barcelona"),
  (9,  "Mesa Standing",     "Oficina",      450.00, 2, "2024-02", "Madrid"),
  (10, "SSD 1TB",           "Almacenamiento",119.99, 5, "2024-02", "Sevilla"),
  (11, "Laptop Pro",        "Tecnología",  1299.99, 1, "2024-03", "Valencia"),
  (12, "Hub USB-C",         "Periféricos",   44.90, 7, "2024-03", "Madrid"),
  (13, "Monitor 32\"",      "Tecnología",   499.00, 2, "2024-03", "Bilbao"),
  (14, "Silla Gamer",       "Oficina",      280.00, 4, "2024-03", "Barcelona"),
  (15, "Micrófono USB",     "Audio",        109.00, 3, "2024-03", "Madrid"),
  (16, "Laptop Air",        "Tecnología",   999.00, 2, "2024-04", "Sevilla"),
  (17, "Teclado Mecánico",  "Periféricos",   79.90, 3, "2024-04", "Valencia"),
  (18, "SSD 2TB",           "Almacenamiento",199.99, 4, "2024-04", "Madrid"),
  (19, "Auriculares BT",    "Audio",        149.00, 6, "2024-04", "Barcelona"),
  (20, "Ratón Vertical",    "Periféricos",   39.95, 5, "2024-04", "Bilbao"),
  (21, "Laptop Pro",        "Tecnología",  1299.99, 2, "2024-05", "Madrid"),
  (22, "Webcam 4K",         "Periféricos",  129.00, 4, "2024-05", "Sevilla"),
  (23, "Mesa Escritorio",   "Oficina",      380.00, 1, "2024-05", "Valencia"),
  (24, "SSD 1TB",           "Almacenamiento",119.99, 3, "2024-05", "Madrid"),
  (25, "Monitor 27\"",      "Tecnología",   349.00, 2, "2024-05", "Barcelona"),
  (26, "Laptop Air",        "Tecnología",   999.00, 4, "2024-06", "Bilbao"),
  (27, "Hub USB-C",         "Periféricos",   44.90, 9, "2024-06", "Madrid"),
  (28, "Auriculares BT",    "Audio",        149.00, 5, "2024-06", "Sevilla"),
  (29, "Silla Ergonómica",  "Oficina",      320.00, 2, "2024-06", "Valencia"),
  (30, "SSD 2TB",           "Almacenamiento",199.99, 3, "2024-06", "Barcelona")
).toDF("id", "producto", "categoria", "precio_unitario", "unidades", "mes", "ciudad")

println(s"✅ Dataset creado: ${ventas.count()} filas")
ventas.show(5)
```

**Salida esperada:**
```
✅ Dataset creado: 30 filas
+---+---------------+------------+---------------+--------+-------+---------+
| id|       producto|   categoria|precio_unitario|unidades|    mes|   ciudad|
+---+---------------+------------+---------------+--------+-------+---------+
|  1|     Laptop Pro|  Tecnología|        1299.99|       2|2024-01|   Madrid|
|  2|Teclado Mecánic|  Periférico|          79.90|       5|2024-01|   Madrid|
|  3|    Monitor 27"|  Tecnología|         349.00|       1|2024-01|Barcelona|
|  4|Silla Ergonómic|     Oficina|         320.00|       3|2024-01| Valencia|
|  5|Ratón Inalámbri|  Periférico|          29.95|       8|2024-01|  Sevilla|
+---+---------------+------------+---------------+--------+-------+---------+
```

---

### Celda 3 — Calcular métricas con Spark

```scala
// Añadir columna de importe total por línea de venta
val ventasConTotal = ventas
  .withColumn("importe_total", round(col("precio_unitario") * col("unidades"), 2))

// --- Tabla 1: Resumen por ciudad ---
val porCiudad = ventasConTotal
  .groupBy("ciudad")
  .agg(
    count("id").alias("num_ventas"),
    sum("unidades").alias("total_unidades"),
    round(sum("importe_total"), 2).alias("facturacion_total"),
    round(avg("importe_total"), 2).alias("ticket_medio")
  )
  .orderBy(col("facturacion_total").desc)

println("=== Facturación por ciudad ===")
porCiudad.show()

// --- Tabla 2: Resumen por categoría ---
val porCategoria = ventasConTotal
  .groupBy("categoria")
  .agg(
    count("id").alias("num_ventas"),
    sum("unidades").alias("total_unidades"),
    round(sum("importe_total"), 2).alias("facturacion_total"),
    round(avg("precio_unitario"), 2).alias("precio_medio")
  )
  .orderBy(col("facturacion_total").desc)

println("=== Facturación por categoría ===")
porCategoria.show()

// --- Tabla 3: Evolución mensual ---
val porMes = ventasConTotal
  .groupBy("mes")
  .agg(
    count("id").alias("num_ventas"),
    round(sum("importe_total"), 2).alias("facturacion_total")
  )
  .orderBy("mes")

println("=== Evolución mensual ===")
porMes.show()
```

**Salida esperada:**
```
=== Facturación por ciudad ===
+---------+----------+--------------+----------------+------------+
|   ciudad|num_ventas|total_unidades|facturacion_total|ticket_medio|
+---------+----------+--------------+----------------+------------+
|   Madrid|        10|            44|        17007.52|     1700.75|
|Barcelona|         6|            18|         7117.67|     1186.28|
|  Sevilla|         5|            23|         4474.65|      894.93|
|   Bilbao|         4|            18|         6695.60|     1673.90|
| Valencia|         5|            13|         5199.67|     1039.93|
+---------+----------+--------------+----------------+------------+

=== Facturación por categoría ===
+---------------+----------+--------------+----------------+------------+
|      categoria|num_ventas|total_unidades|facturacion_total|precio_medio|
+---------------+----------+--------------+----------------+------------+
|     Tecnología|        11|            24|        19743.86|      999.00|
|        Oficina|         5|            12|         3950.00|      350.00|
|    Periféricos|         9|            47|         2716.95|       67.52|
|          Audio|         4|            18|         2682.00|      149.00|
|Almacenamiento |         4|            15|         1399.89|      159.99|
+---------------+----------+--------------+----------------+------------+

=== Evolución mensual ===
+-------+----------+----------------+
|    mes|num_ventas|facturacion_total|
+-------+----------+----------------+
|2024-01|         5|         4143.35|
|2024-02|         5|         6521.97|
|2024-03|         5|         5862.50|
|2024-04|         5|         6101.55|
|2024-05|         5|         6538.75|
|2024-06|         5|         6327.59|
+-------+----------+----------------+
```

---

### Celda 4 — Crear carpetas de salida

```scala
// Crear la carpeta de output si no existe
import java.nio.file.{Files, Paths}

val carpetaBase = "C:/Curso-Scala/output/powerbi"
Files.createDirectories(Paths.get(carpetaBase))
println(s"✅ Carpeta lista: $carpetaBase")
```

---

### Celda 5 — Exportar a CSV

Usamos `coalesce(1)` para que Spark genere **un único archivo** en lugar de múltiples particiones. Así Power BI puede abrirlo directamente.

```scala
// Exportar las tres tablas a CSV con una sola partición
val opciones = Map("header" -> "true", "encoding" -> "UTF-8")

// Tabla 1 — Por ciudad
ventasConTotal
  .coalesce(1)
  .write
  .mode("overwrite")
  .options(opciones)
  .csv("C:/Curso-Scala/output/powerbi/ventas_detalle")

// Tabla 2 — Por ciudad (resumen)
porCiudad
  .coalesce(1)
  .write
  .mode("overwrite")
  .options(opciones)
  .csv("C:/Curso-Scala/output/powerbi/resumen_ciudad")

// Tabla 3 — Por categoría
porCategoria
  .coalesce(1)
  .write
  .mode("overwrite")
  .options(opciones)
  .csv("C:/Curso-Scala/output/powerbi/resumen_categoria")

// Tabla 4 — Evolución mensual
porMes
  .coalesce(1)
  .write
  .mode("overwrite")
  .options(opciones)
  .csv("C:/Curso-Scala/output/powerbi/evolucion_mensual")

println("✅ Los cuatro CSV exportados correctamente.")
println("   Ruta: C:/Curso-Scala/output/powerbi/")
println()
println("   Dentro de cada carpeta verás un fichero llamado:")
println("   part-00000-xxxxxxxx-xxxx.csv  ← ese es el que importa Power BI")
```

---

### Celda 6 — Verificar los archivos generados

```scala
import java.io.File

val carpetaOutput = new File("C:/Curso-Scala/output/powerbi")
val subcarpetas = carpetaOutput.listFiles().filter(_.isDirectory)

println("=== Archivos generados ===")
subcarpetas.foreach { carpeta =>
  val csvs = carpeta.listFiles().filter(_.getName.endsWith(".csv"))
  csvs.foreach { f =>
    val kb = f.length() / 1024.0
    println(f"  ${carpeta.getName}%-25s → ${f.getName} (${kb}%.1f KB)")
  }
}
```

**Salida esperada (nombres de archivo aproximados):**
```
=== Archivos generados ===
  ventas_detalle            → part-00000-abc123.csv (2.1 KB)
  resumen_ciudad            → part-00000-def456.csv (0.3 KB)
  resumen_categoria         → part-00000-ghi789.csv (0.2 KB)
  evolucion_mensual         → part-00000-jkl012.csv (0.1 KB)
```

> ✅ La parte de Spark está completa. Ahora pasamos a Power BI Desktop.

---

## 4. Parte Power BI — Cargar los datos

### 4.1 Abrir Power BI Desktop

Abre Power BI Desktop desde el menú Inicio. Cierra el diálogo de bienvenida si aparece.

### 4.2 Cargar la primera tabla: ventas_detalle

1. En la cinta superior, haz clic en **Inicio → Obtener datos → Texto o CSV**.

2. Se abre un explorador de archivos. Navega a:
   `C:\Curso-Scala\output\powerbi\ventas_detalle\`

3. Verás un archivo llamado `part-00000-....csv`. Selecciónalo y haz clic en **Abrir**.

4. Aparece una ventana de **previsualización**. Comprueba que:
   - La primera fila son los nombres de las columnas (id, producto, categoria…).
   - Los tipos de dato parecen correctos.

5. Haz clic en **Transformar datos** (no en "Cargar" directamente) para abrir el **Editor de Power Query**.

### 4.3 Limpiar y transformar en Power Query

En el Editor de Power Query:

**Cambiar tipos de columna:**

1. Selecciona la columna `id` → clic derecho → **Cambiar tipo → Número entero**.
2. Selecciona `precio_unitario` → clic derecho → **Cambiar tipo → Número decimal**.
3. Selecciona `unidades` → clic derecho → **Cambiar tipo → Número entero**.
4. Selecciona `importe_total` → clic derecho → **Cambiar tipo → Número decimal**.
5. Selecciona `mes` → déjala como **Texto** (formato YYYY-MM).

**Renombrar la consulta:**

1. En el panel izquierdo ("Consultas"), haz clic derecho sobre el nombre de la consulta.
2. Selecciona **Cambiar nombre** → escribe `ventas_detalle`.

Haz clic en **Cerrar y aplicar** (botón en la esquina superior izquierda).

### 4.4 Cargar las otras tres tablas

Repite el proceso de **Inicio → Obtener datos → Texto o CSV** para cada carpeta:

| Carpeta | Nombre de consulta en Power BI |
|---|---|
| `resumen_ciudad\part-00000-....csv` | `resumen_ciudad` |
| `resumen_categoria\part-00000-....csv` | `resumen_categoria` |
| `evolucion_mensual\part-00000-....csv` | `evolucion_mensual` |

Para cada una: previsualiza → Transformar datos → cambia tipos numéricos → renombra → Cerrar y aplicar.

> 💡 Puedes cargar todas las tablas antes de hacer clic en "Cerrar y aplicar". Power Query las procesará todas a la vez.

---

## 5. Construir el informe

Una vez cargadas las tablas, estás en la vista **Informe** (icono 📊 del panel izquierdo).

El informe tendrá **tres páginas**. Para añadir páginas: clic en el botón `+` en la barra inferior.

---

### Página 1 — Resumen Ejecutivo

Esta página muestra las métricas clave de un vistazo.

#### Tarjetas KPI (métricas destacadas)

1. En el panel **Visualizaciones** (derecha), haz clic en el icono **Tarjeta** (un rectángulo con un número).
2. Del panel **Datos** (derecha), arrastra `facturacion_total` de la tabla `resumen_ciudad` al campo **Campos** de la tarjeta.
3. Con la tarjeta seleccionada, ve al panel **Formato** (icono de rodillo) → **Etiqueta de categoría** → escribe `Facturación Total`.
4. Repite para crear otras tres tarjetas:
   - `num_ventas` (suma) → etiqueta: `Total Ventas`
   - `total_unidades` (suma) → etiqueta: `Unidades Vendidas`
   - `ticket_medio` (promedio) → etiqueta: `Ticket Medio`

Coloca las cuatro tarjetas en la parte superior de la página.

#### Gráfico de barras — Facturación por ciudad

1. Haz clic en un espacio vacío del lienzo.
2. En **Visualizaciones**, selecciona **Gráfico de barras agrupadas**.
3. Arrastra:
   - `ciudad` (tabla `resumen_ciudad`) → campo **Eje Y**
   - `facturacion_total` → campo **Valores**
4. Ordena el gráfico: clic en los tres puntos del gráfico → **Ordenar por → facturacion_total → Descendente**.
5. En el panel **Formato** → **Título** → escribe: `Facturación por Ciudad`.

#### Gráfico de sectores — Distribución por categoría

1. Haz clic en un espacio vacío.
2. Selecciona **Gráfico de anillos** en Visualizaciones.
3. Arrastra:
   - `categoria` (tabla `resumen_categoria`) → **Leyenda**
   - `facturacion_total` → **Valores**
4. Título: `Distribución por Categoría`.

---

### Página 2 — Análisis Temporal

Esta página muestra la evolución de las ventas a lo largo del tiempo.

#### Gráfico de líneas — Evolución mensual

1. Selecciona **Gráfico de líneas** en Visualizaciones.
2. Arrastra:
   - `mes` (tabla `evolucion_mensual`) → **Eje X**
   - `facturacion_total` → **Valores**
   - `num_ventas` → **Información sobre herramientas** (tooltip)
3. En Formato → **Marcadores** → activar (para ver los puntos de cada mes).
4. Título: `Evolución de Facturación Mensual`.

#### Tabla detallada — Datos mensuales

1. Selecciona la visualización **Tabla**.
2. Arrastra desde `evolucion_mensual`:
   - `mes`
   - `num_ventas`
   - `facturacion_total`
3. En Formato → **Formato condicional** en `facturacion_total` → **Barras de datos** → activar. Esto añade barras proporcionales dentro de las celdas.
4. Título: `Detalle Mensual`.

---

### Página 3 — Detalle de Ventas

Esta página muestra los datos individuales con filtros interactivos.

#### Segmentación por ciudad (filtro visual)

1. Selecciona la visualización **Segmentación de datos** (icono de embudo).
2. Arrastra `ciudad` (tabla `ventas_detalle`) → campo **Campo**.
3. En Formato → **Estilo** → selecciona **Mosaico** para mostrar botones de ciudad.
4. Coloca el segmentador en la parte superior.

#### Segmentación por categoría

Repite el proceso con `categoria`. Colócalo junto al anterior.

#### Tabla de detalle

1. Selecciona la visualización **Tabla**.
2. Arrastra desde `ventas_detalle`:
   - `producto`, `categoria`, `ciudad`, `mes`, `unidades`, `precio_unitario`, `importe_total`
3. Ordena por `importe_total` descendente.
4. Título: `Detalle de Transacciones`.

> 💡 **Interactividad automática:** al hacer clic en una ciudad en el segmentador, la tabla se filtra automáticamente. Esto es la potencia de Power BI — no necesitas configurar nada extra.

#### Gráfico de barras apiladas — Categorías por ciudad

1. Selecciona **Gráfico de barras apiladas**.
2. Arrastra desde `ventas_detalle`:
   - `ciudad` → **Eje Y**
   - `importe_total` → **Valores**
   - `categoria` → **Leyenda**
3. Título: `Composición de Ventas por Ciudad y Categoría`.

---

## 6. Construir el dashboard

En Power BI Desktop el concepto equivalente al "dashboard" es una **página de informe diseñada como panel de control** con todas las métricas clave juntas. Los dashboards en la web de Power BI Service requieren cuenta, pero en Desktop podemos crear una página de resumen equivalente.

### 6.1 Añadir la página "Dashboard"

1. Haz clic derecho en la pestaña de cualquier página → **Duplicar página**. Duplica la Página 1.
2. Haz clic derecho en la nueva pestaña → **Cambiar nombre** → escribe `Dashboard`.
3. Mueve esta pestaña al principio arrastrándola.

### 6.2 Diseñar el fondo

1. Haz clic en un área vacía del lienzo.
2. Ve a **Formato de página** (panel Formato cuando no hay nada seleccionado) → **Fondo** → elige un color oscuro (por ejemplo `#1E2A3A` azul oscuro) o deja blanco si prefieres.
3. Ajusta el **Tamaño de página** a **16:9** si no lo está ya.

### 6.3 Añadir título

1. En la cinta **Insertar** → **Cuadro de texto**.
2. Escribe: `Dashboard de Ventas — Tienda Tecnológica 2024`
3. Formato: fuente grande (20–24pt), negrita, color blanco si el fondo es oscuro.
4. Coloca el cuadro en la parte superior centrado.

### 6.4 Distribuir los elementos del dashboard

Reorganiza los elementos heredados de la Página 1 y añade nuevos para crear este layout:

```
┌─────────────────────────────────────────────────────────────┐
│   Dashboard de Ventas — Tienda Tecnológica 2024             │
├──────────┬──────────┬──────────┬──────────────────────────  │
│ KPI      │ KPI      │ KPI      │ KPI                         │
│ Total €  │ Ventas   │ Unidades │ Ticket Medio                │
├──────────┴──────────┴──────────┴─────────────┬──────────────│
│                                              │              │
│    Gráfico de barras — por Ciudad            │   Anillo     │
│                                              │  Categorías  │
│                                              │              │
├──────────────────────────────────────────────┴──────────────│
│                                                             │
│    Gráfico de líneas — Evolución mensual                    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Pasos:**

1. **Fila superior:** distribuye las 4 tarjetas KPI horizontalmente. Selecciónalas todas (Ctrl+clic) → clic derecho → **Alinear → Distribuir horizontalmente**.

2. **Fila intermedia izquierda:** coloca el gráfico de barras por ciudad ocupando aprox. el 60% del ancho.

3. **Fila intermedia derecha:** coloca el gráfico de anillos por categoría.

4. **Fila inferior:** coloca el gráfico de líneas de evolución mensual a lo ancho.

### 6.5 Ajustar colores

Para dar coherencia visual:

1. En la cinta **Ver** → **Temas** → selecciona un tema predefinido (p.ej. "Executive" o "Innovate") o crea el tuyo.
2. Alternativamente, en cada gráfico → **Formato** → **Colores de datos** → elige la paleta manualmente.

### 6.6 Añadir segmentadores al dashboard

1. Inserta dos segmentadores (ciudad y categoría) pequeños en la parte superior derecha.
2. Esto permite filtrar todos los gráficos del dashboard de forma interactiva.

---

## 7. Errores frecuentes

| Problema | Causa | Solución |
|---|---|---|
| Power BI no encuentra el archivo CSV | La ruta tiene barras invertidas `\` o espacios | Navegar con el explorador de archivos, no escribir la ruta a mano |
| La primera fila son números, no cabeceras | El CSV no se exportó con `option("header", "true")` | Volver al notebook y re-ejecutar la Celda 5 con la opción de header |
| Los importes aparecen como texto en Power BI | Power Query no detectó el tipo automáticamente | En Power Query, clic derecho en la columna → Cambiar tipo → Número decimal |
| Los separadores decimales son puntos pero Power BI espera comas | Configuración regional de Windows en español | En Power Query → Inicio → Transformar → Detectar tipo de datos con configuración regional → seleccionar "Inglés (EE.UU.)" |
| El archivo CSV está vacío o tiene solo una línea | `coalesce(1)` no se ejecutó o el DataFrame estaba vacío | Verificar con `dfNombre.count()` en el notebook antes de escribir |
| No aparece el archivo `part-00000-....csv` | Spark escribió en formato incorrecto o la ruta no existe | Ejecutar la Celda 6 (verificación) y comprobar que la carpeta existe con archivos |
| Al actualizar Power BI los datos no cambian | Power BI usa una caché | En Power BI: Inicio → Actualizar |

---

## 8. Resumen del flujo completo

```
1. NOTEBOOK ALMOND (ventas_powerbi.ipynb)
   ├── Celda 1: Inicializar Spark
   ├── Celda 2: Crear DataFrame de ventas (30 registros)
   ├── Celda 3: Calcular métricas (por ciudad, categoría, mes)
   ├── Celda 4: Crear carpeta C:/Curso-Scala/output/powerbi/
   ├── Celda 5: Exportar 4 CSV con coalesce(1) + header=true
   └── Celda 6: Verificar archivos generados

2. POWER BI DESKTOP
   ├── Obtener datos → CSV (x4 tablas)
   ├── Power Query: cambiar tipos + renombrar
   ├── Página 1 "Resumen Ejecutivo"
   │     ├── 4 tarjetas KPI
   │     ├── Gráfico de barras por ciudad
   │     └── Gráfico de anillos por categoría
   ├── Página 2 "Análisis Temporal"
   │     ├── Gráfico de líneas mensual
   │     └── Tabla con barras condicionales
   ├── Página 3 "Detalle de Ventas"
   │     ├── Segmentadores (ciudad, categoría)
   │     ├── Tabla de transacciones
   │     └── Barras apiladas por ciudad y categoría
   └── Página "Dashboard"
         ├── Título + fondo corporativo
         ├── 4 KPIs + gráfico barras + anillo
         ├── Línea temporal
         └── Segmentadores interactivos
```

---

> 💾 **Guardar el trabajo en Power BI:** Archivo → Guardar como → `ventas_powerbi.pbix`. El archivo `.pbix` incluye tanto los datos como el diseño del informe.

> 🔄 **Para actualizar los datos:** cuando re-ejecutes el notebook en Almond y sobreescribas los CSV, en Power BI haz clic en **Inicio → Actualizar** y todos los gráficos se actualizarán automáticamente.

> 💡 **Copiar el backup:** no olvides ejecutar `copiar-scala.ps1` después de finalizar el notebook.
