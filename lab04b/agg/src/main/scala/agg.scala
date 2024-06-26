import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

import java.time.LocalDateTime

object agg {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("bulankin_lab06")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    println("Allocated", LocalDateTime.now())

    val schema: StructType = StructType(
      Seq(
        StructField("category", StringType, true),
        StructField("event_type", StringType, true),
        StructField("item_id", StringType, true),
        StructField("item_price", LongType, true),
        StructField("timestamp", StringType, true),
        StructField("uid", StringType, true)
      )
    )

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1.newprolab.com:6667",
      "subscribe" -> "mihail_bulankin",
      "startingOffsets" -> """earliest""",
      "maxOffsetsPerTrigger" -> "1000"
    )

    val sdf = spark.readStream.format("kafka").options(kafkaParams).load
    val parsedSdf = sdf
      .select($"value".cast("string"))
      .select(from_json($"value", schema).as("data"))
      .select(
        "data.event_type",
        "data.item_price",
        "data.timestamp",
        "data.uid"
      )
      .withColumn(
        "timestamp",
        from_unixtime($"timestamp" / 1000, "yyyy-MM-dd HH:mm:ss")
      )
      .select(
        $"timestamp",
        $"uid",
        when($"event_type" === "buy", $"item_price").as("item_price"),
        when($"event_type" === "buy", $"event_type").as("buy")
      )
      .groupBy(window($"timestamp", "1 hours").as("time"))
      .agg(
        sum("item_price").as("revenue"),
        count("uid").as("visitors"),
        count("buy").as("purchases"),
        (sum("item_price") / count("buy")).as("aov")
      )
      .withColumn("start_ts", unix_timestamp($"time.start"))
      .withColumn("end_ts", unix_timestamp($"time.end"))
      .withColumn(
        "value",
        to_json(
          struct(
            $"start_ts",
            $"end_ts",
            $"revenue",
            $"visitors",
            $"purchases",
            $"aov"
          )
        )
      )
      .select($"value")

    def createKafkaSinkWithCheckpoint(chkName: String, df: DataFrame) = {
      df
      .writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", s"/tmp/$chkName")
      .option("truncate", "false")
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1.newprolab.com:6667")
      .option("topic", "mihail_bulankin_lab04b_out")
    }

    val sink: DataStreamWriter[Row] = createKafkaSinkWithCheckpoint("test_mvl_0", parsedSdf)
    val sq: Unit = sink.start.awaitTermination()

    println("DIRECTED BY ROBERT B.WEIDE", LocalDateTime.now())
  }
}
