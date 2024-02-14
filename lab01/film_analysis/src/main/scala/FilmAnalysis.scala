import java.io._


object FilmAnalysis extends App {
  private val source: BufferedSource = fromFile("/Users/m.bulankin/spark_de_course/lab01/laba01/ml-100k/u.data")
  private val lines: Seq[Array[String]] = source.getLines.toList.map(string => string.split("\t"))
  source.close()

  private val hist_film = {
    lines.filter(_(1) == "173").map(_(2).toInt).groupBy(identity).mapValues(_.size).toList.sortBy(_._1).map(_._2)
  }
  private val hist_all = lines.map(_(2).toInt).groupBy(identity).mapValues(_.size).toList.sortBy(_._1).map(_._2)
  private val json = compact(render(("hist_film" -> hist_film) ~ ("hist_all" -> hist_all)))

  val file: File = new File("lab01.json")
  private val writer: BufferedWriter = new BufferedWriter(new FileWriter(file))
  writer.write(json)
  writer.close()
}

