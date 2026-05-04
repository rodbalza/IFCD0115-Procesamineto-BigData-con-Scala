# 💻Clase 20 - Datasets en Spark

---

# Agenda:

<aside>
💡

#### 9:00 - 9:50    →  Examen Módulo 3

#### 9:50 - 11:20   →  Examen Módulo 3

#### **11:20 - 11:40  →  Descanso**

#### 11:40 - 12:40  → Datasets en Spark

#### 12:40 - 14:00  → Ejercicios

</aside>

---

# **💻 Sesión 2: Datasets en Spark**

# 0. Celda de inicialización del entorno Spark

> Ejecuta esta celda una sola vez al principio del notebook. A partir de la sección **2. ¿Qué es un `Dataset[T]`?**, todos los ejemplos asumen que esta celda ya se ha ejecutado.
> 

```scala
import $ivy.`org.apache.spark::spark-core:4.1.1`
import $ivy.`org.apache.spark::spark-sql:4.1.1`

import org.apache.spark.sql.{Dataset, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder()
  .appName("Clase20")
  .master("local[*]")
  .config("spark.ui.showConsoleProgress", "false")
  .getOrCreate()

import spark.implicits._

spark.sparkContext.setLogLevel("ERROR")

println(s"✅ Spark${spark.version} listo")
println(s"✅ Scala${scala.util.Properties.versionNumberString}")
println(s"✅ Java${System.getProperty("java.version")}")
```

Salida esperada aproximada:

```
✅ Spark 4.1.1 listo
✅ Scala 2.13.18
✅ Java  17.x.x
```

<aside>

💡 Para evitar errores de `Encoder` con `case class`, seguiremos esta regla:

1. Definir la `case class` en una celda independiente.
2. Crear el `Dataset` en la celda siguiente.
3. Ejecutar filtros, mapas, agregaciones y acciones en celdas posteriores.
</aside>

# 1. Contexto: de RDD a DataFrame y Dataset

Hasta ahora hemos trabajado con tres niveles de abstracción en Spark:

| Nivel | Estructura | Característica principal |
| --- | --- | --- |
| Bajo nivel | `RDD[T]` | Muy flexible, pero Spark sabe poco sobre la estructura interna de los datos |
| Alto nivel no tipado | `DataFrame` | Datos en columnas con schema, optimizado por Catalyst |
| Alto nivel tipado | `Dataset[T]` | Combina schema + objetos Scala tipados |

> Un `RDD` permite trabajar con objetos Scala de cualquier tipo, pero Spark no puede optimizar tan bien porque no conoce la estructura de los datos.
> 
> 
> Un `DataFrame` sí tiene estructura: columnas con nombre y tipo. Esto permite optimización automática, pero muchas operaciones se escriben con nombres de columnas como texto.
> 
> Un `Dataset[T]` intenta combinar lo mejor de ambos mundos:
> 
> ```
> Dataset[T] = Datos distribuidos + Schema + Tipo Scala T
> ```
> 

---

# 2. ¿Qué es un `Dataset[T]`?

Un `Dataset[T]` es una colección distribuida de objetos de tipo `T`, donde `T` suele ser una `case class` de Scala. Por ejemplo, si tenemos esta clase, en Almond conviene definirla en una celda independiente.

### Celda 1 — Definir la `case class`

```scala
case class Cliente(id: Int, nombre: String, ciudad: String, edad: Int)
```

Salida esperada:

```
defined class Cliente
```

### Celda 2 — Crear el Dataset

```scala
val dsClientes: Dataset[Cliente] = Seq(
  Cliente(1, "Ana", "Madrid", 28),
  Cliente(2, "Luis", "Barcelona", 34),
  Cliente(3, "Marta", "Sevilla", 22)
).toDS()

dsClientes.show(false)
```

Salida esperada:

```
+---+------+---------+----+
|id |nombre|ciudad   |edad|
+---+------+---------+----+
|1  |Ana   |Madrid   |28  |
|2  |Luis  |Barcelona|34  |
|3  |Marta |Sevilla  |22  |
+---+------+---------+----+
```

Esto significa que cada fila del Dataset no es simplemente una fila genérica, sino un objeto Scala de tipo `Cliente`.

```
Dataset[Cliente]

Cliente(1, "Ana", "Madrid", 28)
Cliente(2, "Luis", "Barcelona", 34)
Cliente(3, "Marta", "Sevilla", 22)
```

---

# 3. `DataFrame = Dataset[Row]`

En Scala, un `DataFrame` es simplemente un alias de tipo para:

```scala
Dataset[Row]
```

Es decir:

```
DataFrame = Dataset[Row]
```

La diferencia práctica es esta:

| Estructura | Tipo interno | Acceso a los datos |
| --- | --- | --- |
| `DataFrame` | `Dataset[Row]` | Por nombre de columna o posición |
| `Dataset[Cliente]` | `Dataset[Cliente]` | Por atributos Scala: `cliente.edad`, `cliente.nombre` |

Ejemplo con DataFrame:

```scala
val dfClientes = Seq(
  (1, "Ana", "Madrid", 28),
  (2, "Luis", "Barcelona", 34)
).toDF("id", "nombre", "ciudad", "edad")

val mayores = dfClientes.filter($"edad" >= 30)
```

Ejemplo con Dataset:

### Celda 1 — Definir la `case class`

```scala
case class Cliente(id: Int, nombre: String, ciudad: String, edad: Int)
```

### Celda 2 — Crear el Dataset

```scala
val dsClientes = Seq(
  Cliente(1, "Ana", "Madrid", 28),
  Cliente(2, "Luis", "Barcelona", 34)
).toDS()
```

### Celda 3 — Filtrar con acceso tipado

```scala
val mayores = dsClientes.filter(cliente => cliente.edad >= 30)
mayores.show(false)
```

Salida esperada:

```
+---+------+---------+----+
|id |nombre|ciudad   |edad|
+---+------+---------+----+
|2  |Luis  |Barcelona|34  |
+---+------+---------+----+
```

> La segunda forma permite que Scala revise en compilación si `edad` existe y si se está usando correctamente.
> 

---

# 4. Ventaja principal: tipo seguro en tiempo de compilación

La gran ventaja de `Dataset[T]` en Scala es la **seguridad de tipos**. Esto significa que algunos errores se detectan antes de ejecutar el programa.

## 4.1 Error en DataFrame: se detecta en ejecución

```scala
val dfClientes = Seq(
  (1, "Ana", "Madrid", 28),
  (2, "Luis", "Barcelona", 34)
).toDF("id", "nombre", "ciudad", "edad")

// Error: la columna "edadd" no existe
val resultado = dfClientes.filter($"edadd" >= 30)

resultado.show()
```

El código puede compilar, pero al ejecutar la acción `show()` Spark fallará porque la columna `edadd` no existe.

Ejemplo de error esperado:

```
[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `edadd` cannot be resolved.
Did you mean one of the following? [`edad`, `id`, `ciudad`, `nombre`]?
```

## 4.2 Error en Dataset: se detecta antes

### Celda 1 — Definir la `case class`

```scala
case class Cliente(id: Int, nombre: String, ciudad: String, edad: Int)
```

### Celda 2 — Crear el Dataset

```scala
val dsClientes = Seq(
  Cliente(1, "Ana", "Madrid", 28),
  Cliente(2, "Luis", "Barcelona", 34)
).toDS()
```

### Celda 3 — Provocar el error tipado

```scala
// Error: el atributo edadd no existe en Cliente
val resultado = dsClientes.filter(cliente => cliente.edadd >= 30)
```

Este código no compila porque `Cliente` no tiene un campo llamado `edadd`.

Ejemplo de error esperado:

```
value edadd is not a member of Cliente
did you mean edad?
Compilation Failed
```

---

# 5. ¿Qué es un `Encoder`?

Para que Spark pueda trabajar con objetos Scala dentro de un Dataset, necesita saber cómo convertir esos objetos a una representación interna eficiente. Ese trabajo lo hace un `Encoder`.

```
Objeto Scala  <---- Encoder ---->  Representación interna Spark SQL
```

Por ejemplo:

```scala
case class Pedido(id: Int, cliente: String, importe: Double)
```

Spark necesita saber cómo convertir:

```scala
Pedido(1, "Ana", 250.75)
```

En una estructura interna con columnas:

```
id: Int
cliente: String
importe: Double
```

## 5.1 Encoders derivados de `case class`

En la mayoría de casos, no tenemos que crear el `Encoder` manualmente. Spark puede derivarlo automáticamente si usamos una `case class` y hemos importado:

```scala
import spark.implicits._
```

## Ejemplo:

### Celda 1 — Definir la `case class`

```scala
case class Pedido(id: Int, cliente: String, importe: Double)
```

### Celda 2 — Crear el Dataset

```scala
val dsPedidos = Seq(
  Pedido(1, "Ana", 250.75),
  Pedido(2, "Luis", 99.90),
  Pedido(3, "Marta", 430.00)
).toDS()

dsPedidos.show(false)
```

Salida esperada:

```
+---+-------+-------+
|id |cliente|importe|
+---+-------+-------+
|1  |Ana    |250.75 |
|2  |Luis   |99.9   |
|3  |Marta  |430.0  |
+---+-------+-------+
```

> Aquí `.toDS()` funciona porque `spark.implicits._` proporciona las conversiones y encoders necesarios.
> 

---

# 6. Crear Datasets tipados con `case class`

## 6.1 Datos de entrada

### Celda 1 — Definir la `case class`

```scala
	case class Venta(
  idVenta: Int,
  producto: String,
  categoria: String,
  cantidad: Int,
  precioUnitario: Double,
  ciudad: String
)
```

Salida esperada:

```
defined class Venta
```

### Celda 2 — Crear el Dataset

```scala
val dsVentas = Seq(
  Venta(1, "Portátil", "Informática", 2, 850.50, "Madrid"),
  Venta(2, "Ratón", "Informática", 5, 18.90, "Valencia"),
  Venta(3, "Teclado", "Informática", 3, 45.00, "Sevilla"),
  Venta(4, "Monitor", "Informática", 1, 199.99, "Madrid"),
  Venta(5, "Silla", "Oficina", 4, 120.00, "Barcelona"),
  Venta(6, "Mesa", "Oficina", 2, 250.00, "Zaragoza"),
  Venta(7, "Webcam", "Informática", 6, 39.90, "Madrid"),
  Venta(8, "Auriculares", "Informática", 3, 59.99, "Valencia")
).toDS()
```

Salida esperada:

```
dsVentas: Dataset[Venta] = [idVenta: int, producto: string ... 4 more fields]
```

## 6.2 Ver contenido y schema

```scala
dsVentas.show(false)
dsVentas.printSchema()
```

Salida esperada:

```
+-------+-----------+-----------+--------+--------------+---------+
|idVenta|producto   |categoria  |cantidad|precioUnitario|ciudad   |
+-------+-----------+-----------+--------+--------------+---------+
|1      |Portátil   |Informática|2       |850.5         |Madrid   |
|2      |Ratón      |Informática|5       |18.9          |Valencia |
|3      |Teclado    |Informática|3       |45.0          |Sevilla  |
|4      |Monitor    |Informática|1       |199.99        |Madrid   |
|5      |Silla      |Oficina    |4       |120.0         |Barcelona|
|6      |Mesa       |Oficina    |2       |250.0         |Zaragoza |
|7      |Webcam     |Informática|6       |39.9          |Madrid   |
|8      |Auriculares|Informática|3       |59.99         |Valencia |
+-------+-----------+-----------+--------+--------------+---------+
```

Schema esperado:

```
root
 |-- idVenta: integer (nullable = false)
 |-- producto: string (nullable = true)
 |-- categoria: string (nullable = true)
 |-- cantidad: integer (nullable = false)
 |-- precioUnitario: double (nullable = false)
 |-- ciudad: string (nullable = true)
```

---

# 7. Operaciones tipadas con Dataset

## 7.1 `filter` tipado

```scala
val ventasGrandes = dsVentas.filter(venta => venta.cantidad >= 3)

ventasGrandes.show(false)
```

Salida esperada:

```
+-------+-----------+-----------+--------+--------------+---------+
|idVenta|producto   |categoria  |cantidad|precioUnitario|ciudad   |
+-------+-----------+-----------+--------+--------------+---------+
|2      |Ratón      |Informática|5       |18.9          |Valencia |
|3      |Teclado    |Informática|3       |45.0          |Sevilla  |
|5      |Silla      |Oficina    |4       |120.0         |Barcelona|
|7      |Webcam     |Informática|6       |39.9          |Madrid   |
|8      |Auriculares|Informática|3       |59.99         |Valencia |
+-------+-----------+-----------+--------+--------------+---------+
```

## 7.2 `map` tipado

Creamos una segunda `case class` para representar una venta enriquecida con el importe total.

### Celda 1 — Definir la nueva `case class`

```scala
case class VentaConTotal(
  idVenta: Int,
  producto: String,
  categoria: String,
  cantidad: Int,
  precioUnitario: Double,
  ciudad: String,
  total: Double
)
```

Salida esperada:

```
defined class VentaConTotal
```

### Celda 2 — Aplicar `map` tipado

```scala
val dsVentasConTotal = dsVentas.map { venta =>
  VentaConTotal(
    venta.idVenta,
    venta.producto,
    venta.categoria,
    venta.cantidad,
    venta.precioUnitario,
    venta.ciudad,
    venta.cantidad * venta.precioUnitario
  )
}

dsVentasConTotal.show(false)
```

Salida esperada:

```
+-------+-----------+-----------+--------+--------------+---------+------+
|idVenta|producto   |categoria  |cantidad|precioUnitario|ciudad   |total |
+-------+-----------+-----------+--------+--------------+---------+------+
|1      |Portátil   |Informática|2       |850.5         |Madrid   |1701.0|
|2      |Ratón      |Informática|5       |18.9          |Valencia |94.5  |
|3      |Teclado    |Informática|3       |45.0          |Sevilla  |135.0 |
|4      |Monitor    |Informática|1       |199.99        |Madrid   |199.99|
|5      |Silla      |Oficina    |4       |120.0         |Barcelona|480.0 |
|6      |Mesa       |Oficina    |2       |250.0         |Zaragoza |500.0 |
|7      |Webcam     |Informática|6       |39.9          |Madrid   |239.4 |
|8      |Auriculares|Informática|3       |59.99         |Valencia |179.97|
+-------+-----------+-----------+--------+--------------+---------+------+
```

## 7.3 `groupByKey` tipado

```scala
val ventasPorCategoria = dsVentasConTotal
  .groupByKey(venta => venta.categoria)
  .count()

ventasPorCategoria.show(false)
```

Salida esperada:

```
+-----------+--------+
|key        |count(1)|
+-----------+--------+
|Informática|6       |
|Oficina    |2       |
+-----------+--------+
```

> 💡 En `groupByKey`, la clave se genera usando una función Scala tipada. En este caso, `venta => venta.categoria`.
> 

---

# 8. Convertir entre DataFrame y Dataset

## 8.1 Dataset a DataFrame con `.toDF()`

```scala
val dfVentas = dsVentas.toDF()

dfVentas.show(false)
```

Salida esperada:

```
+-------+-----------+-----------+--------+--------------+---------+
|idVenta|producto   |categoria  |cantidad|precioUnitario|ciudad   |
+-------+-----------+-----------+--------+--------------+---------+
|1      |Portátil   |Informática|2       |850.5         |Madrid   |
|2      |Ratón      |Informática|5       |18.9          |Valencia |
|3      |Teclado    |Informática|3       |45.0          |Sevilla  |
|4      |Monitor    |Informática|1       |199.99        |Madrid   |
|5      |Silla      |Oficina    |4       |120.0         |Barcelona|
|6      |Mesa       |Oficina    |2       |250.0         |Zaragoza |
|7      |Webcam     |Informática|6       |39.9          |Madrid   |
|8      |Auriculares|Informática|3       |59.99         |Valencia |
+-------+-----------+-----------+--------+--------------+---------+
```

Ahora el objeto es un `DataFrame`, es decir, un `Dataset[Row]`.

```scala
println(dfVentas.getClass)
```

Salida esperada:

```
class org.apache.spark.sql.classic.Dataset
```

<aside>
💡

Puedes ver la diferencia mas a detalle usando un `printSchema()` de esta forma:

```scala
println("=== DataFrame ===")
dfVentas.printSchema()

println("=== Dataset[Venta] ===")
dsVentas.printSchema()
```

Salida:

```scala
=== DataFrame ===
root
 |-- idVenta: integer (nullable = false)
 |-- producto: string (nullable = true)
 |-- categoria: string (nullable = true)
 |-- cantidad: integer (nullable = false)
 |-- precioUnitario: double (nullable = false)
 |-- ciudad: string (nullable = true)

=== Dataset[Venta] ===
root
 |-- idVenta: integer (nullable = false)
 |-- producto: string (nullable = true)
 |-- categoria: string (nullable = true)
 |-- cantidad: integer (nullable = false)
 |-- precioUnitario: double (nullable = false)
 |-- ciudad: string (nullable = true)
```

</aside>

## 8.2 DataFrame a Dataset con `.as[T]`

Supongamos que tenemos un DataFrame:

```scala
val dfClientes = Seq(
  (1, "Ana", "Madrid", 28),
  (2, "Luis", "Barcelona", 34),
  (3, "Marta", "Sevilla", 22)
).toDF("id", "nombre", "ciudad", "edad")
```

Definimos una `case class` compatible:

```scala
case class Cliente(id: Int, nombre: String, ciudad: String, edad: Int)
```

Convertimos:

```scala
val dsClientes = dfClientes.as[Cliente]

dsClientes.show(false)
```

Salida esperada:

```
+---+------+---------+----+
|id |nombre|ciudad   |edad|
+---+------+---------+----+
|1  |Ana   |Madrid   |28  |
|2  |Luis  |Barcelona|34  |
|3  |Marta |Sevilla  |22  |
+---+------+---------+----+
```

> ⚠️ Para que `.as[Cliente]` funcione correctamente, los nombres y tipos del DataFrame deben ser compatibles con la `case class`.
> 

---

# 9. Comparativa: Dataset vs DataFrame vs RDD

| Criterio | RDD | DataFrame | Dataset |
| --- | --- | --- | --- |
| Nivel de abstracción | Bajo | Alto | Alto |
| Tiene schema | No | Sí | Sí |
| Optimización Catalyst | Limitada | Sí | Sí |
| Tipado en compilación | Sí, pero sin schema SQL | No para nombres de columnas | Sí |
| Forma de trabajo | Funciones Scala sobre objetos | Columnas y expresiones SQL | Objetos Scala tipados |
| Facilidad para analistas SQL | Baja | Alta | Media |
| Recomendado para principiantes | Solo conceptos base | Sí | Sí, después de DataFrames |
| Uso típico | Control muy fino, APIs antiguas, datos no estructurados | ETL, SQL, BI, agregaciones | Lógica de negocio tipada en Scala |

## Regla práctica:

```
Usa DataFrame cuando trabajes con datos tabulares, SQL, joins, agregaciones y pipelines de BI.

Usa Dataset cuando quieras lógica de negocio tipada con case classes y seguridad de compilación.

Usa RDD solo cuando necesites control de bajo nivel o trabajar con datos muy poco estructurados.
```

Por ejemplo, en un DataFrame puedes hacer:

```scala
dfVentas.filter($"cantidad" >= 3)
dfVentas.withColumn("total", $"cantidad" * $"precioUnitario")
```

Eso es una transformación de datos. Pero imagina que la empresa tiene reglas de negocio como estas:

```scala
- Una venta es grande si su total supera 500 €
- Una venta necesita revisión si el total es alto pero la cantidad es baja
- Un cliente es VIP si ha comprado más de 1000 € y tiene más de 3 pedidos
- Un pedido es sospechoso si viene de cierta ciudad y supera cierto importe
- Un producto tiene descuento si pertenece a una categoría concreta y hay mucho stock
```

Eso ya es **lógica de negocio**.

### Ejemplo:

Supongamos esta `case class`:

```scala
case class Venta(
  idVenta: Int,
  producto: String,
  categoria: String,
  cantidad: Int,
  precioUnitario: Double,
  ciudad: String
)
```

```scala
val dsVentas = Seq(
  Venta(1, "Portátil", "Informática", 2, 850.50, "Madrid"),
  Venta(2, "Ratón", "Informática", 5, 18.90, "Valencia"),
  Venta(3, "Teclado", "Informática", 3, 45.00, "Sevilla"),
  Venta(4, "Monitor", "Informática", 1, 199.99, "Madrid"),
  Venta(5, "Silla", "Oficina", 4, 120.00, "Barcelona"),
  Venta(6, "Mesa", "Oficina", 2, 250.00, "Zaragoza"),
  Venta(7, "Webcam", "Informática", 6, 39.90, "Madrid"),
  Venta(8, "Auriculares", "Informática", 3, 59.99, "Valencia")
).toDS()
```

Podemos crear una función de negocio:

```scala
def esVentaGrande(venta: Venta): Boolean = {
  val total = venta.cantidad * venta.precioUnitario
  total >= 500
}
```

Y otra regla:

```scala
def esVentaGrande(venta: Venta): Boolean =
  calcularTotal(venta) >= 500
```

Luego la aplicamos al Dataset:

```scala
val ventasGrandes = dsVentas.filter(venta => esVentaGrande(venta))

ventasGrandes.show(false)
```

Aquí `esVentaGrande` no es una operación genérica de Spark. Es una regla de la empresa:

> “Para esta empresa, una venta grande es una venta cuyo total supera 500 €”.
> 

Otro ejemplo:

```scala
def clasificarVenta(venta: Venta): String = {
  val total = venta.cantidad * venta.precioUnitario

  if (total >= 1000) "Venta premium"
  else if (total >= 500) "Venta importante"
  else "Venta normal"
}
```

Aplicado al Dataset:

```scala
val clasificaciones = dsVentas.map { venta =>
  (venta.idVenta, venta.producto, clasificarVenta(venta))
}
clasificaciones.show(false)
```

Aquí estamos metiendo conocimiento del negocio dentro del código:

```scala
total >= 1000  → Venta premium
total >= 500   → Venta importante
menos de 500   → Venta normal
```

Eso es **lógica de negocio**.

# 10. Errores comunes con Datasets

## Error 1 — Olvidar `import spark.implicits._`

```scala
val ds = Seq(1, 2, 3).toDS()
```

Error aproximado:

```
value toDS is not a member of Seq[Int]
```

Solución:

```scala
import spark.implicits._
```

---

## Error 2 — La `case class` no coincide con el DataFrame

```scala
case class Cliente(id: Int, nombre: String, edad: Int)

val df = Seq(
  (1, "Ana", "Madrid", 28)
).toDF("id", "nombre", "ciudad", "edad")

val ds = df.as[Cliente]
```

> Aquí el DataFrame tiene una columna extra `ciudad`. Spark puede ignorar columnas extra en algunos casos, pero fallará si falta una columna obligatoria o si el tipo no coincide.
> 

Ejemplo más problemático:

```scala
case class Cliente(id: Int, nombre: String, edad: Int)

val df = Seq(
  (1, "Ana", "28")
).toDF("id", "nombre", "edad")

val ds = df.as[Cliente]

ds.show()
```

Posible error en ejecución:

```
Cannot up cast edad from string to int
```

---

## Error 3 — Usar nombres de columnas incorrectos en DataFrame

```scala
val df = dsVentas.toDF()

val resultado = df.select("producto", "cantidadd")

resultado.show()
```

Error aproximado:

```
AnalysisException: A column or function parameter with name `cantidadd` cannot be resolved
```

Con Dataset tipado, este error se detectaría antes si usamos atributos de la `case class`:

```scala
// No compila
val resultado = dsVentas.map(v => v.cantidadd)
```

---

# 💻  Práctica: Datasets tipados

---

# Ejercicio 1 — Crear un Dataset tipado de empleados

## Enunciado

Crea un Dataset tipado llamado `dsEmpleados` usando una `case class` llamada `Empleado`.

## Datos de entrada

### Celda 1 — Definir la `case class`

```scala
case class Empleado(
  id: Int,
  nombre: String,
  departamento: String,
  salario: Double,
  activo: Boolean
)
```

Salida esperada:

```
defined class Empleado
```

### Celda 2 — Crear el Dataset

```scala
val dsEmpleados = Seq(
  Empleado(1, "Ana García", "Ingeniería", 42000.0, true),
  Empleado(2, "Luis Martínez", "Marketing", 38000.0, true),
  Empleado(3, "Marta López", "Ingeniería", 35000.0, true),
  Empleado(4, "Pedro Ruiz", "Dirección", 75000.0, true),
  Empleado(5, "Carmen Díaz", "Marketing", 36500.0, true),
  Empleado(6, "Jorge Santos", "Ingeniería", 48000.0, false)
).toDS()
```

## Tareas

1. Mostrar el contenido con `show(false)`.
2. Imprimir el schema.
3. Filtrar solo empleados activos.
4. Filtrar empleados con salario mayor o igual a 40.000.

## Solución

```scala
println("=== Empleados ===")
dsEmpleados.show(false)

println("=== Schema ===")
dsEmpleados.printSchema()

val empleadosActivos = dsEmpleados.filter(e => e.activo)

println("=== Empleados activos ===")
empleadosActivos.show(false)

val empleadosSalarioAlto = dsEmpleados.filter(e => e.salario >= 40000.0)

println("=== Empleados con salario >= 40000 ===")
empleadosSalarioAlto.show(false)
```

## Salida esperada

```
=== Empleados ===
+---+-------------+------------+-------+------+
|id |nombre       |departamento|salario|activo|
+---+-------------+------------+-------+------+
|1  |Ana García   |Ingeniería  |42000.0|true  |
|2  |Luis Martínez|Marketing   |38000.0|true  |
|3  |Marta López  |Ingeniería  |35000.0|true  |
|4  |Pedro Ruiz   |Dirección   |75000.0|true  |
|5  |Carmen Díaz  |Marketing   |36500.0|true  |
|6  |Jorge Santos |Ingeniería  |48000.0|false |
+---+-------------+------------+-------+------+

=== Schema ===
root
 |-- id: integer (nullable = false)
 |-- nombre: string (nullable = true)
 |-- departamento: string (nullable = true)
 |-- salario: double (nullable = false)
 |-- activo: boolean (nullable = false)

=== Empleados activos ===
+---+-------------+------------+-------+------+
|id |nombre       |departamento|salario|activo|
+---+-------------+------------+-------+------+
|1  |Ana García   |Ingeniería  |42000.0|true  |
|2  |Luis Martínez|Marketing   |38000.0|true  |
|3  |Marta López  |Ingeniería  |35000.0|true  |
|4  |Pedro Ruiz   |Dirección   |75000.0|true  |
|5  |Carmen Díaz  |Marketing   |36500.0|true  |
+---+-------------+------------+-------+------+

=== Empleados con salario >= 40000 ===
+---+------------+------------+-------+------+
|id |nombre      |departamento|salario|activo|
+---+------------+------------+-------+------+
|1  |Ana García  |Ingeniería  |42000.0|true  |
|4  |Pedro Ruiz  |Dirección   |75000.0|true  |
|6  |Jorge Santos|Ingeniería  |48000.0|false |
+---+------------+------------+-------+------+
```

---

# Ejercicio 2 — Crear una nueva estructura con `map`

## Enunciado

A partir de `dsEmpleados`, crea un nuevo Dataset llamado `dsEmpleadosClasificados`, donde cada empleado tenga una categoría salarial.

Reglas:

| Salario | Categoría |
| --- | --- |
| `>= 60000` | `ALTO` |
| `>= 40000` y `< 60000` | `MEDIO` |
| `< 40000` | `BAJO` |

## Solución

### Celda 1 — Definir la nueva `case class`

```scala
case class EmpleadoClasificado(
  id: Int,
  nombre: String,
  departamento: String,
  salario: Double,
  activo: Boolean,
  categoriaSalarial: String
)
```

Salida esperada:

```
defined class EmpleadoClasificado
```

### Celda 2 — Crear el Dataset clasificado

```scala
val dsEmpleadosClasificados = dsEmpleados.map { e =>
  val categoria =
    if (e.salario >= 60000.0) "ALTO"
    else if (e.salario >= 40000.0) "MEDIO"
    else "BAJO"

  EmpleadoClasificado(
    e.id,
    e.nombre,
    e.departamento,
    e.salario,
    e.activo,
    categoria
  )
}

dsEmpleadosClasificados.show(false)
```

## Salida esperada

```
+---+-------------+------------+-------+------+-----------------+
|id |nombre       |departamento|salario|activo|categoriaSalarial|
+---+-------------+------------+-------+------+-----------------+
|1  |Ana García   |Ingeniería  |42000.0|true  |MEDIO            |
|2  |Luis Martínez|Marketing   |38000.0|true  |BAJO             |
|3  |Marta López  |Ingeniería  |35000.0|true  |BAJO             |
|4  |Pedro Ruiz   |Dirección   |75000.0|true  |ALTO             |
|5  |Carmen Díaz  |Marketing   |36500.0|true  |BAJO             |
|6  |Jorge Santos |Ingeniería  |48000.0|false |MEDIO            |
+---+-------------+------------+-------+------+-----------------+
```

---

# Ejercicio 3 — Convertir Dataset a DataFrame

## Enunciado

Convierte `dsEmpleadosClasificados` a DataFrame y realiza una agregación por departamento.

## Solución

```scala
val dfEmpleadosClasificados = dsEmpleadosClasificados.toDF()

val resumenPorDepartamento = dfEmpleadosClasificados
  .groupBy("departamento")
  .agg(
    count("id").alias("num_empleados"),
    avg("salario").alias("salario_medio"),
    max("salario").alias("salario_maximo")
  )

resumenPorDepartamento.show(false)
```

## Salida esperada aproximada

```
+------------+-------------+------------------+--------------+
|departamento|num_empleados|salario_medio     |salario_maximo|
+------------+-------------+------------------+--------------+
|Ingeniería  |3            |41666.666666666664|48000.0       |
|Marketing   |2            |37250.0           |38000.0       |
|Dirección   |1            |75000.0           |75000.0       |
+------------+-------------+------------------+--------------+
```

---

# Ejercicio 4 — Convertir DataFrame a Dataset con `.as[T]`

## Enunciado

Crea un DataFrame de productos y conviértelo a Dataset usando una `case class`.

## Datos de entrada

```scala
val dfProductos = Seq(
  (101, "Laptop Pro", "Informática", 1299.99, 45),
  (102, "Teclado Inalámbrico", "Periféricos", 59.90, 120),
  (103, "Monitor 27", "Monitores", 349.00, 30),
  (104, "Ratón Óptico", "Periféricos", 24.95, 200)
).toDF("id", "nombre", "categoria", "precio", "stock")
```

## Solución

### Celda 1 — Definir la `case class`

```scala
case class Producto(
  id: Int,
  nombre: String,
  categoria: String,
  precio: Double,
  stock: Int
)
```

Salida esperada:

```
defined class Producto
```

### Celda 2 — Convertir a Dataset

```scala
val dsProductos = dfProductos.as[Producto]

dsProductos.show(false)

dsProductos.printSchema()
```

## Filtro tipado

```scala
val productosStockAlto = dsProductos.filter(p => p.stock >= 100)

productosStockAlto.show(false)
```

## Salida esperada

Primero se muestra el Dataset completo:

```
+---+-------------------+-----------+-------+-----+
|id |nombre             |categoria  |precio |stock|
+---+-------------------+-----------+-------+-----+
|101|Laptop Pro         |Informática|1299.99|45   |
|102|Teclado Inalámbrico|Periféricos|59.9   |120  |
|103|Monitor 27         |Monitores  |349.0  |30   |
|104|Ratón Óptico       |Periféricos|24.95  |200  |
+---+-------------------+-----------+-------+-----+

root
 |-- id: integer (nullable = false)
 |-- nombre: string (nullable = true)
 |-- categoria: string (nullable = true)
 |-- precio: double (nullable = false)
 |-- stock: integer (nullable = false)
```

Después se muestra el filtro de productos con stock alto:

```
+---+-------------------+-----------+-----+-----+
|id |nombre             |categoria  |precio|stock|
+---+-------------------+-----------+-----+-----+
|102|Teclado Inalámbrico|Periféricos|59.9 |120  |
|104|Ratón Óptico       |Periféricos|24.95|200  |
+---+-------------------+-----------+-----+-----+
```

---

# Ejercicio 5 — Comparar error de DataFrame vs Dataset

## Parte A — Error con DataFrame

```scala
val dfError = dsEmpleados.toDF()

// La columna "departamentoo" no existe
val resultadoErrorDF = dfError.select("nombre", "departamentoo")

resultadoErrorDF.show(false)
```

## Resultado esperado

El código puede compilar, pero fallará al ejecutar la acción:

```
AnalysisException: A column or function parameter with name `departamentoo` cannot be resolved
```

## Parte B — Error con Dataset

```scala
// Este código NO compila porque departamentoo no existe en la case class Empleado
val resultadoErrorDS = dsEmpleados.map(e => e.departamentoo)
```

## Resultado esperado

```
value departamentoo is not a member of Empleado
```

## Pregunta para reflexionar

¿Qué error preferirías encontrar en un proyecto real: uno al compilar o uno después de lanzar el job Spark?

---

# 🧪 Pipeline completo con Dataset tipado

## Enunciado

Vas a construir un pipeline completo de ventas usando Datasets tipados.

<aside>

El objetivo es:

1. Crear un Dataset de ventas.
2. Filtrar ventas válidas.
3. Calcular el total de cada venta.
4. Clasificar cada venta según su importe.
5. Convertir el resultado a DataFrame.
6. Agregar ventas por categoría.
</aside>

---

## Paso 1 — Definir las clases del dominio

```scala
case class VentaRaw(
  idVenta: Int,
  producto: String,
  categoria: String,
  cantidad: Int,
  precioUnitario: Double,
  ciudad: String
)

case class VentaProcesada(
  idVenta: Int,
  producto: String,
  categoria: String,
  cantidad: Int,
  precioUnitario: Double,
  ciudad: String,
  total: Double,
  tipoVenta: String
)
```

---

## Paso 2 — Crear el Dataset inicial

```scala
val dsVentasRaw = Seq(
  VentaRaw(1, "Portátil", "Informática", 2, 850.50, "Madrid"),
  VentaRaw(2, "Ratón", "Informática", 5, 18.90, "Valencia"),
  VentaRaw(3, "Teclado", "Informática", 3, 45.00, "Sevilla"),
  VentaRaw(4, "Monitor", "Informática", 1, 199.99, "Madrid"),
  VentaRaw(5, "Silla", "Oficina", 4, 120.00, "Barcelona"),
  VentaRaw(6, "Mesa", "Oficina", 2, 250.00, "Zaragoza"),
  VentaRaw(7, "Webcam", "Informática", 6, 39.90, "Madrid"),
  VentaRaw(8, "Auriculares", "Informática", 3, 59.99, "Valencia"),
  VentaRaw(9, "Producto defectuoso", "Prueba", 0, 100.00, "Madrid")
).toDS()

println("=== Ventas raw ===")
dsVentasRaw.show(false)
```

Salida esperada:

```
=== Ventas raw ===
+-------+-------------------+-----------+--------+--------------+---------+
|idVenta|producto           |categoria  |cantidad|precioUnitario|ciudad   |
+-------+-------------------+-----------+--------+--------------+---------+
|1      |Portátil           |Informática|2       |850.5         |Madrid   |
|2      |Ratón              |Informática|5       |18.9          |Valencia |
|3      |Teclado            |Informática|3       |45.0          |Sevilla  |
|4      |Monitor            |Informática|1       |199.99        |Madrid   |
|5      |Silla              |Oficina    |4       |120.0         |Barcelona|
|6      |Mesa               |Oficina    |2       |250.0         |Zaragoza |
|7      |Webcam             |Informática|6       |39.9          |Madrid   |
|8      |Auriculares        |Informática|3       |59.99         |Valencia |
|9      |Producto defectuoso|Prueba     |0       |100.0         |Madrid   |
+-------+-------------------+-----------+--------+--------------+---------+
```

---

## Paso 3 — Filtrar ventas válidas

Una venta válida debe cumplir:

- `cantidad > 0`
- `precioUnitario > 0`

```scala
val dsVentasValidas = dsVentasRaw.filter { venta =>
  venta.cantidad > 0 && venta.precioUnitario > 0
}

println("=== Ventas válidas ===")
dsVentasValidas.show(false)
```

Salida esperada:

```
=== Ventas válidas ===
+-------+-----------+-----------+--------+--------------+---------+
|idVenta|producto   |categoria  |cantidad|precioUnitario|ciudad   |
+-------+-----------+-----------+--------+--------------+---------+
|1      |Portátil   |Informática|2       |850.5         |Madrid   |
|2      |Ratón      |Informática|5       |18.9          |Valencia |
|3      |Teclado    |Informática|3       |45.0          |Sevilla  |
|4      |Monitor    |Informática|1       |199.99        |Madrid   |
|5      |Silla      |Oficina    |4       |120.0         |Barcelona|
|6      |Mesa       |Oficina    |2       |250.0         |Zaragoza |
|7      |Webcam     |Informática|6       |39.9          |Madrid   |
|8      |Auriculares|Informática|3       |59.99         |Valencia |
+-------+-----------+-----------+--------+--------------+---------+
```

---

## Paso 4 — Calcular total y clasificar ventas

Reglas de clasificación:

| Total | Tipo de venta |
| --- | --- |
| `>= 1000` | `VENTA_ALTA` |
| `>= 300` y `< 1000` | `VENTA_MEDIA` |
| `< 300` | `VENTA_BAJA` |

```scala
val dsVentasProcesadas = dsVentasValidas.map { venta =>
  val total = venta.cantidad * venta.precioUnitario

  val tipoVenta =
    if (total >= 1000.0) "VENTA_ALTA"
    else if (total >= 300.0) "VENTA_MEDIA"
    else "VENTA_BAJA"

  VentaProcesada(
    venta.idVenta,
    venta.producto,
    venta.categoria,
    venta.cantidad,
    venta.precioUnitario,
    venta.ciudad,
    total,
    tipoVenta
  )
}

println("=== Ventas procesadas ===")
dsVentasProcesadas.show(false)
```

Salida esperada:

```
=== Ventas procesadas ===
+-------+-----------+-----------+--------+--------------+---------+------------------+-----------+
|idVenta|producto   |categoria  |cantidad|precioUnitario|ciudad   |total             |tipoVenta  |
+-------+-----------+-----------+--------+--------------+---------+------------------+-----------+
|1      |Portátil   |Informática|2       |850.5         |Madrid   |1701.0            |VENTA_ALTA |
|2      |Ratón      |Informática|5       |18.9          |Valencia |94.5              |VENTA_BAJA |
|3      |Teclado    |Informática|3       |45.0          |Sevilla  |135.0             |VENTA_BAJA |
|4      |Monitor    |Informática|1       |199.99        |Madrid   |199.99            |VENTA_BAJA |
|5      |Silla      |Oficina    |4       |120.0         |Barcelona|480.0             |VENTA_MEDIA|
|6      |Mesa       |Oficina    |2       |250.0         |Zaragoza |500.0             |VENTA_MEDIA|
|7      |Webcam     |Informática|6       |39.9          |Madrid   |239.39999999999998|VENTA_BAJA |
|8      |Auriculares|Informática|3       |59.99         |Valencia |179.97            |VENTA_BAJA |
+-------+-----------+-----------+--------+--------------+---------+------------------+-----------+
```

---

## Paso 5 — Guardar el resultado del pipeline en una variable

```scala
val pipelineVentasTipado = dsVentasProcesadas

pipelineVentasTipado.show(false)
```

Salida esperada:

```
+-------+-----------+-----------+--------+--------------+---------+------------------+-----------+
|idVenta|producto   |categoria  |cantidad|precioUnitario|ciudad   |total             |tipoVenta  |
+-------+-----------+-----------+--------+--------------+---------+------------------+-----------+
|1      |Portátil   |Informática|2       |850.5         |Madrid   |1701.0            |VENTA_ALTA |
|2      |Ratón      |Informática|5       |18.9          |Valencia |94.5              |VENTA_BAJA |
|3      |Teclado    |Informática|3       |45.0          |Sevilla  |135.0             |VENTA_BAJA |
|4      |Monitor    |Informática|1       |199.99        |Madrid   |199.99            |VENTA_BAJA |
|5      |Silla      |Oficina    |4       |120.0         |Barcelona|480.0             |VENTA_MEDIA|
|6      |Mesa       |Oficina    |2       |250.0         |Zaragoza |500.0             |VENTA_MEDIA|
|7      |Webcam     |Informática|6       |39.9          |Madrid   |239.39999999999998|VENTA_BAJA |
|8      |Auriculares|Informática|3       |59.99         |Valencia |179.97            |VENTA_BAJA |
+-------+-----------+-----------+--------+--------------+---------+------------------+-----------+
```

---

## Paso 6 — Convertir a DataFrame para agregaciones

```scala
val dfVentasProcesadas = pipelineVentasTipado.toDF()

val resumenCategoria = dfVentasProcesadas
  .groupBy("categoria")
  .agg(
    count("idVenta").alias("num_ventas"),
    sum("total").alias("importe_total"),
    avg("total").alias("importe_medio")
  )
  .orderBy(desc("importe_total"))

resumenCategoria.show(false)
```

## Salida esperada aproximada

```
+-----------+----------+-------------+------------------+
|categoria  |num_ventas|importe_total|importe_medio     |
+-----------+----------+-------------+------------------+
|Informática|6         |2549.86      |424.9766666666667 |
|Oficina    |2         |980.0        |490.0             |
+-----------+----------+-------------+------------------+
```

---

# 🏢 Caso de Estudio Propuesto: LogiCommerce Analytics

---

## Contexto empresarial

<aside>

**LogiCommerce Analytics** es una empresa que presta servicios de análisis de ventas para tiendas online medianas. La empresa recibe diariamente registros de ventas desde distintas ciudades españolas y necesita preparar un pipeline inicial para clasificar las ventas antes de enviarlas al equipo de BI.

El equipo de ingeniería ha decidido usar **Datasets tipados** porque quiere que la lógica de negocio quede representada con `case class` y que los errores de atributos se detecten lo antes posible.

En esta sesión solo puedes usar los conocimientos vistos hoy:

- `case class`
- `Dataset[T]`
- `.toDS()`
- `.filter(...)`
- `.map(...)`
- `.toDF()`
- Agregaciones simples tras convertir a DataFrame
- Comparación de errores DataFrame vs Dataset
</aside>

---

## Objetivo del caso

Construir un pipeline tipado que procese ventas online y genere un resumen por canal de venta.

---

## Datos de entrada

### Celda 1 — Definir la `case class`

```scala
case class VentaOnlineRaw(
  idPedido: Int,
  cliente: String,
  canal: String,
  producto: String,
  unidades: Int,
  precioUnitario: Double,
  ciudad: String
)
```

Salida esperada:

```
defined class VentaOnlineRaw
```

### Celda 2 — Crear el Dataset inicial

```scala
val dsVentasOnlineRaw = Seq(
  VentaOnlineRaw(1001, "Ana", "Web", "Portátil", 1, 899.99, "Madrid"),
  VentaOnlineRaw(1002, "Luis", "App", "Ratón", 2, 24.95, "Valencia"),
  VentaOnlineRaw(1003, "Marta", "Web", "Monitor", 1, 249.90, "Sevilla"),
  VentaOnlineRaw(1004, "Pedro", "Marketplace", "Teclado", 3, 45.00, "Madrid"),
  VentaOnlineRaw(1005, "Carmen", "App", "Auriculares", 2, 79.90, "Barcelona"),
  VentaOnlineRaw(1006, "Jorge", "Web", "Silla", 1, 129.99, "Zaragoza"),
  VentaOnlineRaw(1007, "Elena", "Marketplace", "Webcam", 4, 39.90, "Madrid"),
  VentaOnlineRaw(1008, "Tomás", "App", "Hub USB-C", 2, 34.90, "Valencia"),
  VentaOnlineRaw(1009, "Laura", "Web", "Pedido erróneo", 0, 199.99, "Madrid"),
  VentaOnlineRaw(1010, "Andrés", "App", "Producto sin precio", 1, 0.0, "Sevilla")
).toDS()
```

---

# Tareas del caso

## Tarea 1 — Explorar el Dataset inicial

Muestra el contenido y el schema.

```scala
dsVentasOnlineRaw.show(false)
dsVentasOnlineRaw.printSchema()
```

Salida esperada:

```
+--------+-------+-----------+-------------------+--------+--------------+---------+
|idPedido|cliente|canal      |producto           |unidades|precioUnitario|ciudad   |
+--------+-------+-----------+-------------------+--------+--------------+---------+
|1001    |Ana    |Web        |Portátil           |1       |899.99        |Madrid   |
|1002    |Luis   |App        |Ratón              |2       |24.95         |Valencia |
|1003    |Marta  |Web        |Monitor            |1       |249.9         |Sevilla  |
|1004    |Pedro  |Marketplace|Teclado            |3       |45.0          |Madrid   |
|1005    |Carmen |App        |Auriculares        |2       |79.9          |Barcelona|
|1006    |Jorge  |Web        |Silla              |1       |129.99        |Zaragoza |
|1007    |Elena  |Marketplace|Webcam             |4       |39.9          |Madrid   |
|1008    |Tomás  |App        |Hub USB-C          |2       |34.9          |Valencia |
|1009    |Laura  |Web        |Pedido erróneo     |0       |199.99        |Madrid   |
|1010    |Andrés |App        |Producto sin precio|1       |0.0           |Sevilla  |
+--------+-------+-----------+-------------------+--------+--------------+---------+

root
 |-- idPedido: integer (nullable = false)
 |-- cliente: string (nullable = true)
 |-- canal: string (nullable = true)
 |-- producto: string (nullable = true)
 |-- unidades: integer (nullable = false)
 |-- precioUnitario: double (nullable = false)
 |-- ciudad: string (nullable = true)
```

---

## Tarea 2 — Filtrar pedidos válidos

Un pedido válido debe cumplir:

- `unidades > 0`
- `precioUnitario > 0`

Guarda el resultado en una variable llamada:

```scala
val dsPedidosValidos = ???
```

### Solución

```scala
val dsPedidosValidos = dsVentasOnlineRaw.filter { pedido =>
  pedido.unidades > 0 && pedido.precioUnitario > 0
}

dsPedidosValidos.show(false)
```

Salida esperada:

```
+--------+-------+-----------+-----------+--------+--------------+---------+
|idPedido|cliente|canal      |producto   |unidades|precioUnitario|ciudad   |
+--------+-------+-----------+-----------+--------+--------------+---------+
|1001    |Ana    |Web        |Portátil   |1       |899.99        |Madrid   |
|1002    |Luis   |App        |Ratón      |2       |24.95         |Valencia |
|1003    |Marta  |Web        |Monitor    |1       |249.9         |Sevilla  |
|1004    |Pedro  |Marketplace|Teclado    |3       |45.0          |Madrid   |
|1005    |Carmen |App        |Auriculares|2       |79.9          |Barcelona|
|1006    |Jorge  |Web        |Silla      |1       |129.99        |Zaragoza |
|1007    |Elena  |Marketplace|Webcam     |4       |39.9          |Madrid   |
|1008    |Tomás  |App        |Hub USB-C  |2       |34.9          |Valencia |
+--------+-------+-----------+-----------+--------+--------------+---------+
```

---

## Tarea 3 — Crear una `case class` para pedidos procesados

La nueva estructura debe contener:

- `idPedido`
- `cliente`
- `canal`
- `producto`
- `unidades`
- `precioUnitario`
- `ciudad`
- `importeTotal`
- `segmentoPedido`

Reglas para `segmentoPedido`:

| Importe total | Segmento |
| --- | --- |
| `>= 500` | `ALTO_VALOR` |
| `>= 150` y `< 500` | `VALOR_MEDIO` |
| `< 150` | `BAJO_VALOR` |

### Solución

```scala
case class VentaOnlineProcesada(
  idPedido: Int,
  cliente: String,
  canal: String,
  producto: String,
  unidades: Int,
  precioUnitario: Double,
  ciudad: String,
  importeTotal: Double,
  segmentoPedido: String
)
```

---

## Tarea 4 — Construir el pipeline tipado

```scala
val dsPedidosProcesados = dsPedidosValidos.map { pedido =>
  val importeTotal = pedido.unidades * pedido.precioUnitario

  val segmento =
    if (importeTotal >= 500.0) "ALTO_VALOR"
    else if (importeTotal >= 150.0) "VALOR_MEDIO"
    else "BAJO_VALOR"

  VentaOnlineProcesada(
    pedido.idPedido,
    pedido.cliente,
    pedido.canal,
    pedido.producto,
    pedido.unidades,
    pedido.precioUnitario,
    pedido.ciudad,
    importeTotal,
    segmento
  )
}

dsPedidosProcesados.show(false)
```

Salida esperada:

```
+--------+-------+-----------+-----------+--------+--------------+---------+------------+--------------+
|idPedido|cliente|canal      |producto   |unidades|precioUnitario|ciudad   |importeTotal|segmentoPedido|
+--------+-------+-----------+-----------+--------+--------------+---------+------------+--------------+
|1001    |Ana    |Web        |Portátil   |1       |899.99        |Madrid   |899.99      |ALTO_VALOR    |
|1002    |Luis   |App        |Ratón      |2       |24.95         |Valencia |49.9        |BAJO_VALOR    |
|1003    |Marta  |Web        |Monitor    |1       |249.9         |Sevilla  |249.9       |VALOR_MEDIO   |
|1004    |Pedro  |Marketplace|Teclado    |3       |45.0          |Madrid   |135.0       |BAJO_VALOR    |
|1005    |Carmen |App        |Auriculares|2       |79.9          |Barcelona|159.8       |VALOR_MEDIO   |
|1006    |Jorge  |Web        |Silla      |1       |129.99        |Zaragoza |129.99      |BAJO_VALOR    |
|1007    |Elena  |Marketplace|Webcam     |4       |39.9          |Madrid   |159.6       |VALOR_MEDIO   |
|1008    |Tomás  |App        |Hub USB-C  |2       |34.9          |Valencia |69.8        |BAJO_VALOR    |
+--------+-------+-----------+-----------+--------+--------------+---------+------------+--------------+
```

---

## Tarea 5 — Guardar el resultado del pipeline en otra variable

```scala
val pipelineFinalPedidos = dsPedidosProcesados

pipelineFinalPedidos.show(false)
```

Salida esperada:

```
+--------+-------+-----------+-----------+--------+--------------+---------+------------+--------------+
|idPedido|cliente|canal      |producto   |unidades|precioUnitario|ciudad   |importeTotal|segmentoPedido|
+--------+-------+-----------+-----------+--------+--------------+---------+------------+--------------+
|1001    |Ana    |Web        |Portátil   |1       |899.99        |Madrid   |899.99      |ALTO_VALOR    |
|1002    |Luis   |App        |Ratón      |2       |24.95         |Valencia |49.9        |BAJO_VALOR    |
|1003    |Marta  |Web        |Monitor    |1       |249.9         |Sevilla  |249.9       |VALOR_MEDIO   |
|1004    |Pedro  |Marketplace|Teclado    |3       |45.0          |Madrid   |135.0       |BAJO_VALOR    |
|1005    |Carmen |App        |Auriculares|2       |79.9          |Barcelona|159.8       |VALOR_MEDIO   |
|1006    |Jorge  |Web        |Silla      |1       |129.99        |Zaragoza |129.99      |BAJO_VALOR    |
|1007    |Elena  |Marketplace|Webcam     |4       |39.9          |Madrid   |159.6       |VALOR_MEDIO   |
|1008    |Tomás  |App        |Hub USB-C  |2       |34.9          |Valencia |69.8        |BAJO_VALOR    |
+--------+-------+-----------+-----------+--------+--------------+---------+------------+--------------+
```

---

## Tarea 6 — Convertir a DataFrame para generar resumen por canal

```scala
val dfPedidosProcesados = pipelineFinalPedidos.toDF()

val resumenPorCanal = dfPedidosProcesados
  .groupBy("canal")
  .agg(
    count("idPedido").alias("num_pedidos"),
    sum("importeTotal").alias("importe_total"),
    avg("importeTotal").alias("importe_medio")
  )
  .orderBy(desc("importe_total"))

resumenPorCanal.show(false)
```

### Salida esperada aproximada

```
+-----------+-----------+-------------+------------------+
|canal      |num_pedidos|importe_total|importe_medio     |
+-----------+-----------+-------------+------------------+
|Web        |3          |1279.88      |426.62666666666667|
|Marketplace|2          |294.6        |147.3             |
|App        |3          |279.4        |93.13333333333334 |
+-----------+-----------+-------------+------------------+
```

---

## Tarea 7 — Comparar error en Dataset vs DataFrame

### Parte A — Error con Dataset

Intenta ejecutar este código:

```scala
// Error intencionado: el campo "canall" no existe
val errorDataset = pipelineFinalPedidos.map(p => p.canall)
```

Pregunta:

```
¿El error aparece antes o después de ejecutar una acción como show()?
```

Respuesta esperada:

```
Aparece antes, porque Scala detecta que canall no existe en la case class VentaOnlineProcesada.
```

---

### Parte B — Error con DataFrame

Intenta ejecutar este código:

```scala
val errorDataFrame = dfPedidosProcesados.select("cliente", "canall")

errorDataFrame.show(false)
```

Pregunta:

```
¿El error aparece al escribir la transformación o al ejecutar la acción show()?
```

Respuesta esperada:

```
Aparece al ejecutar show(), porque Spark intenta resolver la columna canall en tiempo de análisis del plan.
```

---

# Preguntas

1. ¿Qué significa que un `DataFrame` sea un `Dataset[Row]`?
2. ¿Qué ventaja tiene acceder a `cliente.edad` frente a escribir `$"edad"`?
3. ¿Por qué necesitamos `import spark.implicits._`?
4. ¿Qué hace un `Encoder`?
5. ¿Cuándo usarías Dataset en lugar de DataFrame?
6. ¿Por qué un error en Dataset puede aparecer antes que uno en DataFrame?
7. ¿Qué debe coincidir para poder usar `.as[T]` sobre un DataFrame?
8. ¿Por qué en muchos pipelines reales se mezclan Dataset y DataFrame?

---

# Mini-reto

A partir de `pipelineFinalPedidos`, crea un nuevo Dataset que solo contenga pedidos `ALTO_VALOR`. Después conviértelo a DataFrame y calcula el importe total por ciudad.

## Solución orientativa

```scala
val pedidosAltoValor = pipelineFinalPedidos.filter { pedido =>
  pedido.segmentoPedido == "ALTO_VALOR"
}

val resumenAltoValorPorCiudad = pedidosAltoValor
  .toDF()
  .groupBy("ciudad")
  .agg(
    count("idPedido").alias("num_pedidos_alto_valor"),
    sum("importeTotal").alias("importe_total_alto_valor")
  )
  .orderBy(desc("importe_total_alto_valor"))

resumenAltoValorPorCiudad.show(false)
```

Salida esperada:

```
+------+-----------------------+------------------------+
|ciudad|num_pedidos_alto_valor|importe_total_alto_valor|
+------+-----------------------+------------------------+
|Madrid|1                      |899.99                  |
+------+-----------------------+------------------------+
```