import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{
  CountVectorizer,
  IndexToString,
  StringIndexer
}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  ArrayType,
  LongType,
  StringType,
  StructField,
  StructType
}

import java.time.LocalDateTime

object train {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("bulankin_lab07_train")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    println("Allocated", LocalDateTime.now())

    import spark.implicits._

    val hdfsDataPath: String = spark.conf.get("spark.mlproject.data_dir",
      "/labs/laba07/laba07.json")
    val hdfsModelPath: String = spark.conf.get("spark.mlproject.model_dir",
      "/user/mihail.bulankin/labs/lab07/model")

    case class Visit(timestamp: Long, url: String)

    case class TrainData(uid: String, gender_age: String, visits: Array[Visit])

    val trainSchema: StructType =
      StructType(
        StructField("uid", StringType) ::
          StructField("gender_age", StringType) ::
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

    val trainDS: Dataset[TrainData] = spark.read
      .format("json")
      .schema(trainSchema)
      .option("inferSchema", "false")
      .load(hdfsDataPath)
      .as[TrainData]

    case class ClearedTrainData(
        uid: String,
        gender_age: String,
        domain: String,
        url: String
    )

    val clearedDS: Dataset[ClearedTrainData] = trainDS
      .withColumn("visits", explode_outer(col("visits")))
      .withColumn(
        "pre_url",
        regexp_replace(
          regexp_replace(
            regexp_replace(
              col("visits.url"),
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
        lower(trim(callUDF("parse_url", col("pre_url"), lit("HOST"))))
      )
      .withColumn("url", col("visits.url"))
      .drop("visits")
      .as[ClearedTrainData]

    case class TrainFeatures(
        uid: String,
        gender_age: String,
        domains: Array[String]
    )

    val featuresDS: Dataset[TrainFeatures] = clearedDS
      .groupBy(col("uid"))
      .agg(
        collect_list(col("domain")).as("domains"),
        clearedDS.columns
          .filterNot(List("uid", "domain").contains(_))
          .map(nm => max(col(nm)).as(nm)): _*
      )
      .select(
        col("uid") +:
          clearedDS.columns
            .filterNot(List("uid", "domain").contains(_))
            .map(col) :+
          col("domains"): _*
      )
      .drop("timestamp")
      .as[TrainFeatures]

    val countVectorizer: CountVectorizer = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val stringIndexer: StringIndexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")

    val labels: Array[String] = stringIndexer.fit(featuresDS).labels

    val logisticRegression: LogisticRegression = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val indexToString: IndexToString = new IndexToString()
      .setLabels(labels)
      .setInputCol("prediction")
      .setOutputCol("prediction_gender_age")

    val pipeline: Pipeline = new Pipeline()
      .setStages(
        Array(countVectorizer, stringIndexer, logisticRegression, indexToString)
      )

    val model: PipelineModel = pipeline.fit(featuresDS)

    model.write.overwrite
      .save(hdfsModelPath)

    println("DIRECTED BY ROBERT B.WEIDE", LocalDateTime.now())
  }
}
