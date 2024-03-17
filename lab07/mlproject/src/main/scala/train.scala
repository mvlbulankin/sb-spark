import org.apache.spark.sql.{DataFrame, SparkSession}
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

    val trainDS: DataFrame = spark.read
      .format("json")
      .schema(trainSchema)
      .option("inferSchema", "false")
      .load(hdfsDataPath)

    val clearedDS: DataFrame = trainDS
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

    val featuresDS: DataFrame = clearedDS
      .groupBy($"uid")
      .agg(
        collect_list(col("domain")).as("domains"),
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

    model.write.overwrite.save(hdfsModelPath)

    println("DIRECTED BY ROBERT B.WEIDE", LocalDateTime.now())
  }
}
