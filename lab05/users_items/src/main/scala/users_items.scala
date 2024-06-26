import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

object users_items {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("bulankin_lab05")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    println("Allocated", LocalDateTime.now())

    import spark.implicits._

    val workMode: String = spark.sparkContext.getConf.get("spark.users_items.update")
    val inputDirPrefix: String = spark.sparkContext.getConf.get("spark.users_items.input_dir")
    val outputDirPrefix: String = spark.sparkContext.getConf.get("spark.users_items.output_dir")

    val jsonPathBuy: String = s"$inputDirPrefix/buy/*"
    println(jsonPathBuy, LocalDateTime.now())
    val jsonPathView: String = s"$inputDirPrefix/view/*"
    println(jsonPathView, LocalDateTime.now())

    val buys: DataFrame = spark.read
      .json(jsonPathBuy)
      .withColumn("item_id", regexp_replace(lower($"item_id"), "[-\\s]", "_"))
      .withColumn("item_id", concat(lit("buy_"), $"item_id"))
    val views: DataFrame = spark.read
      .json(jsonPathView)
      .withColumn("item_id", regexp_replace(lower($"item_id"), "[-\\s]", "_"))
      .withColumn("item_id", concat(lit("view_"), $"item_id"))

    val unionDf: DataFrame = buys.union(views)
    val maxDate: String = unionDf.select(max($"date")).first().getString(0)
    println("maxDate", maxDate)

    val newDf: DataFrame =
      unionDf.groupBy("uid").pivot("item_id").count().na.fill(0)

    val outputDir: String = s"$outputDirPrefix/$maxDate"

    if (workMode.contains("0")) {
      newDf.write.mode("overwrite").parquet(outputDir)
      println("workMode = 0")

    } else if (workMode.contains("1")) {
      println("workMode = 1")

      // Read the data from the latest subdirectory
      val oldDf: DataFrame = spark.read.format("parquet").load(s"$outputDirPrefix/20200429")

      def expr(myCols: Set[String], allCols: Set[String]) = {
        allCols.toList.map(x =>
          x match {
            case x if myCols.contains(x) => col(x)
            case _                       => lit(0).as(x)
          }
        )
      }

      val newDataCols: Set[String] = newDf.columns.toSet
      val oldDataCols: Set[String] = oldDf.columns.toSet
      val total_cols: Set[String] = newDataCols ++ oldDataCols
      val oldDataModifiedDf: DataFrame = oldDf.select(expr(oldDataCols, total_cols): _*)
      val newDataModifiedDf: DataFrame = newDf.select(expr(newDataCols, total_cols): _*)
      val finalDf: DataFrame = oldDataModifiedDf.union(newDataModifiedDf).distinct()
//      // Собираем список колонок для суммирования (все кроме "uid")
//      val columnsToSum = df.columns.filterNot(_ == "uid")
//
//      // Создаем список выражений для суммирования значений в остальных колонках
//      val sumExpressions = columnsToSum.map(colName => sum(col(colName)).alias(colName))
//
//      // Выполняем группировку по полю "uid" и суммирование значений в остальных колонках
//      val result = df.groupBy("uid").agg(sumExpressions.head, sumExpressions.tail: _*)

      finalDf.write.mode("overwrite").parquet(outputDir)
    }
    println("DIRECTED BY ROBERT B.WEIDE", LocalDateTime.now())
  }
}
