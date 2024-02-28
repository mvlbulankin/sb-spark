import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import java.time.LocalDateTime

object features {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("bulankin_lab06")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    println("Allocated", LocalDateTime.now())

    val webLogs: DataFrame = spark.read.json("hdfs:///labs/laba03/weblogs.json")
    val webLogsExploded: DataFrame = webLogs.select($"uid", explode($"visits").as("visit"))
    val webLogsDecoded: DataFrame = webLogsExploded
      .withColumn(
        "host",
        lower(callUDF("parse_url", $"visit.url", lit("HOST")))
      )
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .select(
        $"uid",
        $"visit.timestamp".alias("timestamp"),
        $"domain"
      )
    val webLogsCollected: DataFrame = webLogsDecoded
      .groupBy($"uid")
      .agg(collect_list($"domain").alias("domain_collected"))

    val mostPopularDomains: DataFrame = webLogsDecoded
      .filter($"domain".isNotNull)
      .select($"domain")
      .groupBy($"domain")
      .agg(count($"domain").alias("domain_counter"))
      .orderBy($"domain_counter".desc)
      .limit(1000)
      .orderBy($"domain")
      .select($"domain")
    val mostPopularDomainsList =
      mostPopularDomains.select("domain").rdd.map(r => r(0).toString).collect()

    val cvModel: CountVectorizerModel =
      new CountVectorizerModel(mostPopularDomainsList)
        .setInputCol("domain_collected")
        .setOutputCol("domain_features")
    val domainFeatures =
      cvModel.transform(webLogsCollected).select($"uid", $"domain_features")

    val webLogsVisitsCount: DataFrame = webLogsDecoded
      .withColumn("date_column", to_date(from_unixtime($"timestamp" / 1000)))
      .withColumn(
        "web_day_mon",
        when(date_format($"date_column", "u").equalTo("1"), 1).otherwise(0)
      )
      .withColumn(
        "web_day_tue",
        when(date_format($"date_column", "u").equalTo("2"), 1).otherwise(0)
      )
      .withColumn(
        "web_day_wed",
        when(date_format($"date_column", "u").equalTo("3"), 1).otherwise(0)
      )
      .withColumn(
        "web_day_thu",
        when(date_format($"date_column", "u").equalTo("4"), 1).otherwise(0)
      )
      .withColumn(
        "web_day_fri",
        when(date_format($"date_column", "u").equalTo("5"), 1).otherwise(0)
      )
      .withColumn(
        "web_day_sat",
        when(date_format($"date_column", "u").equalTo("6"), 1).otherwise(0)
      )
      .withColumn(
        "web_day_sun",
        when(date_format($"date_column", "u").equalTo("7"), 1).otherwise(0)
      )
      .withColumn("timestamp_col", from_unixtime($"timestamp" / 1000))
      .withColumn(
        "web_hour_0",
        when(hour($"timestamp_col").equalTo("0"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_1",
        when(hour($"timestamp_col").equalTo("1"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_2",
        when(hour($"timestamp_col").equalTo("2"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_3",
        when(hour($"timestamp_col").equalTo("3"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_4",
        when(hour($"timestamp_col").equalTo("4"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_5",
        when(hour($"timestamp_col").equalTo("5"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_6",
        when(hour($"timestamp_col").equalTo("6"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_7",
        when(hour($"timestamp_col").equalTo("7"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_8",
        when(hour($"timestamp_col").equalTo("8"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_9",
        when(hour($"timestamp_col").equalTo("9"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_10",
        when(hour($"timestamp_col").equalTo("10"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_11",
        when(hour($"timestamp_col").equalTo("11"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_12",
        when(hour($"timestamp_col").equalTo("12"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_13",
        when(hour($"timestamp_col").equalTo("13"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_14",
        when(hour($"timestamp_col").equalTo("14"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_15",
        when(hour($"timestamp_col").equalTo("15"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_16",
        when(hour($"timestamp_col").equalTo("16"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_17",
        when(hour($"timestamp_col").equalTo("17"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_18",
        when(hour($"timestamp_col").equalTo("18"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_19",
        when(hour($"timestamp_col").equalTo("19"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_20",
        when(hour($"timestamp_col").equalTo("20"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_21",
        when(hour($"timestamp_col").equalTo("21"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_22",
        when(hour($"timestamp_col").equalTo("22"), 1).otherwise(0)
      )
      .withColumn(
        "web_hour_23",
        when(hour($"timestamp_col").equalTo("23"), 1).otherwise(0)
      )
      .withColumn(
        "web_work_hours",
        $"web_hour_9" + $"web_hour_10" + $"web_hour_11" + $"web_hour_12" + $"web_hour_13" + $"web_hour_14" + $"web_hour_15" + $"web_hour_16" + $"web_hour_17" + $"web_hour_18"
      )
      .withColumn(
        "web_evening_hours",
        $"web_hour_18" + $"web_hour_19" + $"web_hour_20" + $"web_hour_21" + $"web_hour_22" + $"web_hour_23" + $"web_hour_0"
      )

    val webLogsGrouped: DataFrame = webLogsVisitsCount
      .groupBy($"uid")
      .agg(
        sum($"web_day_mon").alias("web_day_mon"),
        sum($"web_day_tue").alias("web_day_tue"),
        sum($"web_day_wed").alias("web_day_wed"),
        sum($"web_day_thu").alias("web_day_thu"),
        sum($"web_day_fri").alias("web_day_fri"),
        sum($"web_day_sat").alias("web_day_sat"),
        sum($"web_day_sun").alias("web_day_sun"),
        sum($"web_hour_0").alias("web_hour_0"),
        sum($"web_hour_1").alias("web_hour_1"),
        sum($"web_hour_2").alias("web_hour_2"),
        sum($"web_hour_3").alias("web_hour_3"),
        sum($"web_hour_4").alias("web_hour_4"),
        sum($"web_hour_5").alias("web_hour_5"),
        sum($"web_hour_6").alias("web_hour_6"),
        sum($"web_hour_7").alias("web_hour_7"),
        sum($"web_hour_8").alias("web_hour_8"),
        sum($"web_hour_9").alias("web_hour_9"),
        sum($"web_hour_10").alias("web_hour_10"),
        sum($"web_hour_11").alias("web_hour_11"),
        sum($"web_hour_12").alias("web_hour_12"),
        sum($"web_hour_13").alias("web_hour_13"),
        sum($"web_hour_14").alias("web_hour_14"),
        sum($"web_hour_15").alias("web_hour_15"),
        sum($"web_hour_16").alias("web_hour_16"),
        sum($"web_hour_17").alias("web_hour_17"),
        sum($"web_hour_18").alias("web_hour_18"),
        sum($"web_hour_19").alias("web_hour_19"),
        sum($"web_hour_20").alias("web_hour_20"),
        sum($"web_hour_21").alias("web_hour_21"),
        sum($"web_hour_22").alias("web_hour_22"),
        sum($"web_hour_23").alias("web_hour_23"),
        sum($"web_work_hours").alias("web_work_hours"),
        sum($"web_evening_hours").alias("web_evening_hours")
      )
      .withColumn(
        "web_total",
        $"web_day_mon" + $"web_day_tue" + $"web_day_wed" + $"web_day_thu" + $"web_day_fri" + $"web_day_sat" + $"web_day_sun"
      )
      .withColumn("web_fraction_work_hours", $"web_work_hours" / $"web_total")
      .withColumn(
        "web_fraction_evening_hours",
        $"web_evening_hours" / $"web_total"
      )
      .select(
        $"uid",
        $"web_day_mon",
        $"web_day_tue",
        $"web_day_wed",
        $"web_day_thu",
        $"web_day_fri",
        $"web_day_sat",
        $"web_day_sun",
        $"web_hour_0",
        $"web_hour_1",
        $"web_hour_2",
        $"web_hour_3",
        $"web_hour_4",
        $"web_hour_5",
        $"web_hour_6",
        $"web_hour_7",
        $"web_hour_8",
        $"web_hour_9",
        $"web_hour_10",
        $"web_hour_11",
        $"web_hour_12",
        $"web_hour_13",
        $"web_hour_14",
        $"web_hour_15",
        $"web_hour_16",
        $"web_hour_17",
        $"web_hour_18",
        $"web_hour_19",
        $"web_hour_20",
        $"web_hour_21",
        $"web_hour_22",
        $"web_hour_23",
        $"web_fraction_work_hours",
        $"web_fraction_evening_hours"
      )

    val oldDf: DataFrame = spark.read
      .format("parquet")
      .load("/user/mihail.bulankin/users-items/20200429")

    val result: DataFrame = oldDf
      .alias("left")
      .join(webLogsGrouped.alias("mid"), Seq("uid"), "inner")
      .join(domainFeatures.alias("right"), Seq("uid"), "inner")

    result.write.mode("overwrite").parquet("/user/mihail.bulankin/features")

    println("DIRECTED BY ROBERT B.WEIDE", LocalDateTime.now())
  }
}
