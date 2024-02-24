import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.net.{URL, URLDecoder}
import java.time.LocalDateTime
import scala.util.Try

object website_relevance {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("lab02")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("Allocated", LocalDateTime.now())

    // Пути к файлам
    val autousersPath: String = "/Users/m.bulankin/spark_de_course/lab02/laba02/autousers.json"
    val logsPath: String = "/Users/m.bulankin/spark_de_course/lab02/laba02/logs"

    // Читаем файл с автолюбителями
    val usersSchema =
      StructType(
        List(
          StructField("autousers", ArrayType(StringType, containsNull = false))
        )
      )
    val autousers = spark.read.schema(usersSchema).json(autousersPath)

    // Читаем файл с логами посещений сайтов
    val logsSchema =
      StructType(
        List(
          StructField("uid", StringType),
          StructField("ts", StringType),
          StructField("url", StringType)
        )
      )
    val domains = spark.read.schema(logsSchema).option("delimiter", "\t").csv(logsPath)

    // Помещаем id автолюбителей в список
    val autousersDF = autousers.select(explode(col("autousers")).as("uid"))
    val autousersList = autousersDF
      .select("uid")
      .rdd
      .map(r => r(0).asInstanceOf[String])
      .collect
      .toList

    // Декодируем урл
    def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
      Try {
        val parsedUrl = new URL(URLDecoder.decode(url, "UTF-8"))
        val protocol = parsedUrl.getProtocol
        val host = parsedUrl.getHost
        s"$protocol://$host"
      }.getOrElse("")
    })

    // Берем только урл с протоколами
    val transformedDomainsDF = domains
      .select(col("uid"), decodeUrlAndGetDomain(col("url")).alias("domain"))
      .filter(
        col("domain") =!= "" && (col("domain")
          .startsWith("http://") || col("domain").startsWith("https://"))
      )

    // Убираем протоколы у урл
    val trimmedDomainsDF = transformedDomainsDF.withColumn(
      "domain",
      when(
        col("domain").startsWith("http://"),
        regexp_replace(col("domain"), "^http://", "")
      )
        .when(
          col("domain").startsWith("https://"),
          regexp_replace(col("domain"), "^https://", "")
        )
        .otherwise(col("domain"))
    )

    // Убираем префикс "www."
    val finalTrimmedDomainsDF = trimmedDomainsDF.withColumn(
      "domain",
      when(
        col("domain").startsWith("www."),
        regexp_replace(col("domain"), "^www.", "")
      )
        .otherwise(col("domain"))
    )

    // Добавляем колонку с помещениями автолюбителями
    val urlHitsDF = finalTrimmedDomainsDF.withColumn(
      "hit",
      when(col("uid").isin(autousersList: _*), 1).otherwise(0)
    )

    // Группируем
    val groupedDF = urlHitsDF
      .groupBy(col("domain"))
      .agg(sum(col("hit")).as("sum"), count(col("domain")).as("count"))

    // Считаем общее количество посещений сайтов
    val totalDomainCount =
      groupedDF.agg(sum("count")).collect()(0)(0).asInstanceOf[Long].toDouble

    // Считаем количество посещений сайтов автолюбителями
    val totalHitSum =
      groupedDF.agg(sum("sum")).collect()(0)(0).asInstanceOf[Long].toDouble

    // Считаем искомую метрику
    val resultDF = groupedDF
      .withColumn(
        "relevance",
        format_number(
          ((col("sum") / totalDomainCount) * (col(
            "sum"
          ) / totalDomainCount)) / ((col(
            "count"
          ) / totalDomainCount) * (totalHitSum / totalDomainCount)),
          15
        )
      )

    // Сортируем датафрейм
    val orderedResultDF = resultDF
      .select(col("domain"), col("relevance"))
      .orderBy(desc("relevance"), asc("domain"))
      .limit(200)

    // Записываем результат
    orderedResultDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("delimiter", "\t")
      .csv("lab02_result")

    println("DIRECTED BY ROBERT B.WEIDE", LocalDateTime.now())
  }
}
