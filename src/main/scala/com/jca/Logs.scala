package com.jca


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Logs extends App with spark {
  // Formato datetime deseado
  val DATETIME_FORMAT = "dd/MMM/yyyy:HH:mm:ss"
  // Nombre de las columnas
  val HOST = "host"
  val DATE = "date"
  val METHOD = "method"
  val RESOURCE = "resource"
  val PROTOCOL = "protocol"
  val STATUS = "status"
  val SIZE = "size"
  val NUM_APARICIONES = "num_apariciones"

  // Leer todos los ficheros que se encuentren en la carpeta server_logs
  val logs = spark.read.csv("src/main/resources/server_logs")
  logs.show()

  // Parsear el DF anterior utilizando regexp para extraer cada uno de sus componentes como una columna
  val parsedLogs = serverLogParser(logs)

  private def serverLogParser(df: DataFrame): DataFrame = {
    df.select(regexp_extract(col("_c0"), """^([^(\s|,)]+)""", 1).alias(HOST),
      to_timestamp(regexp_extract(col("_c0"), """^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""", 1), DATETIME_FORMAT).alias(DATE),
      regexp_extract(col("_c0"), """^.*\"(GET|HEAD|POST|PUT|DELETE|CONNECT|OPTIONS|TRACE)""", 1).alias(METHOD),
      regexp_extract(col("_c0"), """^.*\w+\s+([^\s]+)\s+HTTP.*""", 1).alias(RESOURCE),
      regexp_extract(col("_c0"), """^.*\s+(HTTP\S+)\"""", 1).alias(PROTOCOL),
      regexp_extract(col("_c0"), """^.*\s+([^\s]+)\s+""", 1).cast("int").alias(STATUS),
      regexp_extract(col("_c0"), """^.*\s(\S+)""", 1).cast("int").alias(SIZE),
    )
  }

  parsedLogs.printSchema()
  parsedLogs.show(20, truncate = false)

  parsedLogs.select(METHOD).distinct().show()

  // Almacenamos el DF ya tratado en formato parquet
  parsedLogs.write.mode("overwrite").parquet("src/main/resources/server_logs/parquet")

  // Leemos nuestro server_logs/parquet
  val parquetLogs = spark.read.parquet("src/main/resources/server_logs/parquet")
  parquetLogs.printSchema()
  parquetLogs.show(10, truncate = false)

  // Cuales son los distintos protocolos web utilizados?
  val protocolos = parquetLogs.select(PROTOCOL).distinct()
  protocolos.show()

  // Cuales son los codigos de estado, agrupalos y ordenalos para ver cuales son los más comunes
  val estados = parquetLogs.groupBy(STATUS).agg(count(STATUS).alias(NUM_APARICIONES)).sort(desc(NUM_APARICIONES))
  estados.show()

  // Cuales son los metodos de peticion mas utilizados
  val metodos = parquetLogs.groupBy(METHOD).agg(count(METHOD).alias(NUM_APARICIONES)).sort(desc(NUM_APARICIONES))
  metodos.show()

  // Que recurso tuvo la mayor transferencia de Bytes de la pagina
  val recursoConMayorTamano = parquetLogs.groupBy(RESOURCE).agg(max(SIZE).alias("maxSize")).sort(desc("maxSize")).limit(1)
  recursoConMayorTamano.show(truncate = false)

  // que recurso es el que tiene mas registros dentro de nuestro log?
  val recursoConMasTrafico = parquetLogs.groupBy(RESOURCE).count().sort(desc("count")).limit(1)
  recursoConMasTrafico.show()

  // Que dias la web recibio mas trafico
  val diasConMasTrafico = parquetLogs.groupBy(to_date(col(DATE)).alias("day")).count().sort(desc("count"))
  diasConMasTrafico.show()

  // Cuales son los hosts mas frecuentes
  val hostsMasFrecuentes = parquetLogs.groupBy(HOST).count().sort(desc("count"))
  hostsMasFrecuentes.show(truncate = false)

  // A que horas hay más trafico
  val horasConMasTrafico = parquetLogs.groupBy(hour(col(DATE)).alias("Hora")).count().sort(desc("count"))
  horasConMasTrafico.show()

  // Cual es el numero de errores 404 que ha habido cada día
  val erroresNotFoundByDay = parquetLogs.select(STATUS, DATE).where(s"${STATUS} = 404").groupBy(to_date(col(DATE)).alias("Dia")).count().sort(desc("count"))
  erroresNotFoundByDay.show()

}