import org.apache.spark.sql._
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

import scala.concurrent.duration.DurationInt
import scala.util.Random

object test {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("bulankin_lab07_test")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    import spark.implicits._

    val hdfsModelPath: String = spark.conf.get("spark.mlproject.model_dir",
      "/user/mihail.bulankin/labs/lab07/model")
    val kafkaTestHosts = spark.conf.get("spark.mlproject.test.kafka.hosts",
      "spark-master-1:6667")
    val kafkaTestStartingOffsets =
      spark.conf.get("spark.mlproject.test.kafka.starting_offsets",
        "earliest")
    val kafkaTestMaxOffsetsPerTrigger =
      spark.conf.get("spark.mlproject.test.kafka.max_offsets",
        "1000")
    val kafkaTestInputTopic: String =
      spark.conf.get("spark.mlproject.test.kafka.input_topic",
        "mihail_bulankin")
    val kafkaTestOutputTopic: String =
      spark.conf.get("spark.mlproject.test.kafka.output_topic",
        "mihail_bulankin_lab07_out")

    val formatter: DateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy_MM_dd_hh_mm_ss")
    val dateTimeNow: String = LocalDateTime.now.format(formatter)
    val kafkaCheckPointLocation = spark.conf.get(
      "spark.agg.kafka.checkpoint.location",
      s"/tmp/mihail.bulankin/chk/lab07/state_${dateTimeNow}_${Random.nextInt(1000)}"
    )

    val kafkaOptions: Map[String, String] =
      Map(
        "kafka.bootstrap.servers" -> kafkaTestHosts,
        "startingOffsets" -> kafkaTestStartingOffsets,
        "maxOffsetsPerTrigger" -> kafkaTestMaxOffsetsPerTrigger,
        "subscribe" -> kafkaTestInputTopic
      )

    val testSchema: StructType =
      StructType(
        StructField("uid", StringType) ::
          StructField(
            "visits",
            ArrayType(
              StructType(
                StructField("url", StringType) ::
                  StructField("timestamp", LongType) :: Nil
              )
            )
          ) :: Nil
      )

    val testDS: DataFrame = spark.readStream
      .format("kafka")
      .options(kafkaOptions)
      .load
      .select(
        from_json($"value".cast(StringType), testSchema).as("json")
      )
      .select(
        testSchema.fields.map { field =>
          col(s"json.${field.name}").as(field.name)
        }: _*
      )

    val clearedDS: DataFrame = testDS
      .withColumn("visits", explode_outer($"visits"))
      .withColumn(
        "pre_url",
        regexp_replace(
          regexp_replace(
            regexp_replace(
              $"visits.url",
              "(http(s)?:\\/\\/https(:)?\\/\\/)",
              "https:\\/\\/"
            ),
            "(http(s)?:\\/\\/http(:)?\\/\\/)",
            "http:\\/\\/"
          ),
          "www\\.",
          ""
        )
      )
      .withColumn(
        "domain",
        lower(trim(callUDF("parse_url", $"pre_url", lit("HOST"))))
      )
      .withColumn("url", $"visits.url")
      .drop("visits")

    val featuresDS: DataFrame =
      clearedDS
        .groupBy($"uid")
        .agg(
          collect_list($"domain").as("domains"),
          clearedDS.columns
            .filterNot(List("uid", "domain").contains(_))
            .map(nm => max(col(nm)).as(nm)): _*
        )
        .select(
          $"uid" +:
            clearedDS.columns
              .filterNot(List("uid", "domain").contains(_))
              .map(col) :+
            $"domains": _*
        )
        .drop("timestamp")

    val model: PipelineModel = PipelineModel.load(hdfsModelPath)

    val predict: DataFrame = model.transform(featuresDS)

    val result: DataFrame = predict
      .select(
        to_json(
          struct(
            $"uid",
            $"prediction_gender_age".as("gender_age")
          )
        ).as("value")
      )

    result.writeStream
      .format("kafka")
      .outputMode(OutputMode.Update)
      .trigger(Trigger.ProcessingTime(5.seconds))
      .option("kafka.bootstrap.servers", kafkaTestHosts)
      .option("topic", kafkaTestOutputTopic)
      .option("checkpointLocation", kafkaCheckPointLocation)
      .start
      .awaitTermination(3.minutes.toMillis)
  }
}
