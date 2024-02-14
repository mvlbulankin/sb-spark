import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDateTime

object filter {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("bulankin_lab04a")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    println("Allocated", LocalDateTime.now())

    val master: String = spark.sparkContext.getConf.get("spark.master")
    val topicName: String =
      spark.sparkContext.getConf.get("spark.filter.topic_name")
    val offset: String = spark.sparkContext.getConf.get("spark.filter.offset")
    val outputDirPrefix: String =
      spark.sparkContext.getConf.get("spark.filter.output_dir_prefix")

    import spark.implicits._

    val logs = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", "lab04_input_data")
      .option(
        "startingOffsets",
        if (offset.contains("earliest"))
          offset
        else {
          "{\"" + topicName + "\":{\"0\":" + offset + "}}"
        }
      )
      .load()
      .selectExpr("CAST(value AS STRING)")

    val schema: StructType = StructType(
      Seq(
        StructField("category", StringType, true),
        StructField("event_type", StringType, true),
        StructField("item_id", StringType, true),
        StructField("item_price", StringType, true),
        StructField("timestamp", StringType, true),
        StructField("uid", StringType, true)
      )
    )

    val dfWithJson: DataFrame =
      logs.select(from_json($"value", schema).as("data"))
    val unpackedJson: DataFrame = dfWithJson.select("data.*")

    val df: DataFrame = unpackedJson
      .withColumn("date", from_unixtime($"timestamp" / 1000, "yyyyMMdd"))
      .withColumn("p_date", $"date")
      .select(
        $"event_type",
        $"category",
        $"item_id",
        $"item_price",
        $"timestamp",
        $"uid",
        $"date",
        $"p_date"
      )

    val dfBuy: DataFrame = df.filter($"event_type" === "buy")

    dfBuy.write
      .partitionBy("p_date")
      .mode("overwrite")
      .json(
        if (offset.contains("local[1]"))
          s"hdfs://$outputDirPrefix/buy"
        else {
          s"$outputDirPrefix/buy"
        }
      )

    val dfView: DataFrame = df.filter($"event_type" === "view")

    dfView.write
      .partitionBy("p_date")
      .mode("overwrite")
      .json(
        if (offset.contains("local[1]"))
          s"hdfs://$outputDirPrefix/view"
        else {
          s"$outputDirPrefix/buy"
        }
      )

    println("DIRECTED BY ROBERT B.WEIDE", LocalDateTime.now())
  }
}
