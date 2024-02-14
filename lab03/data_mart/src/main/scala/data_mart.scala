import org.apache.spark.sql.{DataFrame, SparkSession}
import org.postgresql.Driver
import java.sql.{Connection, DriverManager, Statement}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import java.net.{URL, URLDecoder}
import scala.util.Try

import java.time.LocalDateTime

object data_mart {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("bulankin_lab03")
      .config("spark.cassandra.connection.host", "10.0.0.31")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("Allocated", LocalDateTime.now())

    import spark.implicits._

    // чтение из Cassandra
    val clients: DataFrame = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "clients", "keyspace" -> "labdata"))
      .load()

    val clientsAgeCategories: DataFrame = clients
      .withColumn(
        "age_cat",
        when($"age".between(18, 24), "18-24")
          .when($"age".between(25, 34), "25-34")
          .when($"age".between(35, 44), "35-44")
          .when($"age".between(45, 54), "45-54")
          .otherwise(">=55")
      )
      .select($"uid", $"age_cat", $"gender")

    // чтение из Elasticsearch
    val visits: DataFrame = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(
        Map(
          "es.read.metadata" -> "true",
          "es.nodes.wan.only" -> "true",
          "es.port" -> "9200",
          "es.nodes" -> "10.0.0.31",
          "es.net.ssl" -> "false"
        )
      )
      .load("visits")

    val visitsReplaced: DataFrame = visits
      .withColumn("category", regexp_replace(lower($"category"), "[-\\s]", "_"))
      .withColumn("category", concat(lit("shop_"), $"category"))

    val visitsPivoted: DataFrame =
      visitsReplaced.groupBy("uid").pivot("category").count().na.fill(0)

    // чтение из JSON
    val logs: DataFrame = spark.read.json("hdfs:///labs/laba03/weblogs.json")

    val logsExploded: DataFrame = logs.select($"uid", explode($"visits").as("visit"))

    def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
      Try {
        new URL(URLDecoder.decode(url, "UTF-8")).getHost
      }.getOrElse("")
    })

    val logsDecoded: DataFrame = logsExploded.select(
      $"uid",
      decodeUrlAndGetDomain($"visit.url").alias("domain")
    )

    val logsTrimmedDomains: DataFrame = logsDecoded
      .withColumn(
        "domain",
        when(
          col("domain").startsWith("www."),
          regexp_replace(col("domain"), "^www.", "")
        )
          .otherwise(col("domain"))
      )
      .filter(col("domain") =!= "" && col("uid").isNotNull)

    // чтение из Postgres
    val categories: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "mihail_bulankin")
      .option("password", "fBjEoAzs")
      .option("driver", "org.postgresql.Driver")
      .load()

    val categoriesReplaced: DataFrame = categories
      .withColumn("category", regexp_replace(lower($"category"), "[-\\s]", "_"))
      .withColumn("category", concat(lit("web_"), $"category"))

    val logsCategories = logsTrimmedDomains.join(categoriesReplaced, Seq("domain"), "inner")

    val logsCategoriesPivoted: DataFrame =
      logsCategories.groupBy("uid").pivot("category").count().na.fill(0)

    val result: DataFrame = clientsAgeCategories
      .join(visitsPivoted, Seq("uid"), "left_outer")
      .join(logsCategoriesPivoted, Seq("uid"), "left_outer")

    // запись в Postgres
    result.write
      .format("jdbc")
      .option(
        "url",
        "jdbc:postgresql://10.0.0.31:5432/mihail_bulankin"
      )
      .option("dbtable", "clients")
      .option("user", "mihail_bulankin")
      .option("password", "fBjEoAzs")
      .option("driver", "org.postgresql.Driver")
      .option("truncate", value = true) // позволит не терять гранты на таблицу
      .mode("overwrite")
      .save()

    // вызвать после сохранения в Postgres: grantTable()
    def grantTable(): Unit = {
      val driverClass: Class[Driver] = classOf[org.postgresql.Driver]
      val driver: Any = Class.forName("org.postgresql.Driver").newInstance()
      val url = "jdbc:postgresql://10.0.0.31:5432/mihail_bulankin?user=mihail_bulankin&password=fBjEoAzs"
      val connection: Connection = DriverManager.getConnection(url)
      val statement: Statement = connection.createStatement()
      val bool: Boolean =
        statement.execute("GRANT SELECT ON clients TO labchecker2")
      connection.close()
    }

    grantTable()

    println("DIRECTED BY ROBERT B.WEIDE", LocalDateTime.now())
  }
}
