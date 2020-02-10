import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, desc, sum, udf, size => arraySize}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

object ItemRecommender {

  val logger = LoggerFactory.getLogger(this.getClass)

  class Conf(args: Seq[String]) extends ScallopConf(args) {

    val itemsPath = opt[String](name = "itemsPath", default = Some("test-data-for-spark.json"))
    val sku = opt[String](name = "sku", required = true)

    val numberOfRecommendations =
      opt[Int](name = "numberOfRecommendations", required = true, default = Some(10), validate = 0 <)

    verify()
  }

  def main(args: Array[String]): Unit = {

    val conf =
      new SparkConf()
        .setAppName("ItemRecommender")
        .setMaster("local[*]")

    implicit val sparkSession = SparkSession.builder.config(conf).getOrCreate()

    val recommendations = run(args)
    recommendations.show(numRows = recommendations.count().toInt, truncate = false)
    sparkSession.stop()
  }

  def run(args: Array[String])(implicit sparkSession: SparkSession): DataFrame = {

    val conf = new Conf(args)
    logger.info(conf.summary)
    val sku = conf.sku()
    val itemsPath = conf.itemsPath()
    val numberOfRecommendations = conf.numberOfRecommendations()

    val items: DataFrame = ItemRecommender.loadItems(itemsPath)
    val attributesOfGivenItem: Map[String, String] =
      ItemRecommender.getAttributes(sku, items)
        .getOrElse {
          sparkSession.stop()
          throw new IllegalArgumentException(s"SKU $sku not found. Please try another one.")
        }

    val matchAttributesUDF: UserDefinedFunction =
      ItemRecommender.matchAttributesHelperUDF(attributesOfGivenItem)

    val candidatesForRecommendation: DataFrame = items
      .filter(col(ItemColumns.sku) =!= sku)
      .withColumn(ItemColumns.matchingAttributes, matchAttributesUDF(col(ItemColumns.attributes)))
      .withColumn(ItemColumns.matchingCount, arraySize(col(ItemColumns.matchingAttributes)))

    ItemRecommender.getRecommendations(candidatesForRecommendation, numberOfRecommendations)
  }

  val matchAttributesHelperUDF: (Map[String, String]) => UserDefinedFunction = example =>
    udf[Seq[String], Row] { row =>
      val parsedRow: Map[String, String] = row.getValuesMap[String](row.schema.names)

      val matchingAttributes =
        parsedRow.filter { case (key, value) => example.get(key).contains(value) }

      matchingAttributes.keys.toSeq.sorted
    }

  def loadItems(path: String)(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(path)

  def getAttributes(sku: String, items: DataFrame): Option[Map[String, String]] = {

    val extractedRow = items
      .filter(col(ItemColumns.sku) === sku)
      .select(ItemColumns.attributes)
      .collect()
      .headOption
      .map(_.getStruct(0))

    extractedRow.map(row => row.getValuesMap[String](row.schema.names))
  }

  def calcMatchingAttributeCounts(candidates: DataFrame)
                                 (implicit sparkSession: SparkSession): Array[MatchingCount] = {
    val window = Window
      .orderBy(desc(ItemColumns.matchingCount))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    import sparkSession.implicits._

    val candidateStatistics = candidates
      .groupBy(ItemColumns.matchingCount)
      .count()
      .withColumn("cumsum", sum("count").over(window))
      .select(col(ItemColumns.matchingCount).as("count"), col("cumsum"))

    candidateStatistics.show()

    candidateStatistics
      .as[MatchingCount]
      .collect()
  }

  def getRecommendations(candidates: DataFrame, numberOfRecommendations: Int)
                        (implicit sparkSession: SparkSession): DataFrame = {

    val matchingCounts: Array[MatchingCount] = calcMatchingAttributeCounts(candidates)

    val selectAllItemsUntilCount: Option[MatchingCount] =
      matchingCounts
        .sortBy(_.count)
        .find(_.cumsum <= numberOfRecommendations)

    val numberOfAdditionalItemsNeeded: Long =
      Math.min(numberOfRecommendations, candidates.count()) -
        selectAllItemsUntilCount.map(_.cumsum).getOrElse(0L)

    val countWhereSelectionIsNeeded: Long =
      matchingCounts
        .sortBy(-_.count)
        .find(_.cumsum > numberOfRecommendations)
        .getOrElse(matchingCounts.maxBy(_.count))
        .count

    logger.info(s"Select $numberOfAdditionalItemsNeeded additional items " +
      s"with $countWhereSelectionIsNeeded matching attributes based on given sorting criterion.")

    val itemsWithoutSelection =
      selectAllItemsUntilCount
        .map { matchingCount =>
          logger.info(s"Select all (=${matchingCount.cumsum}) items with" +
            s" at least ${matchingCount.count} matching attributes.")

          candidates.filter(col(ItemColumns.matchingCount) >= matchingCount.count)
        }
        .getOrElse(
          sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], candidates.schema)
        )

    val unsortedRecommendations = if (numberOfAdditionalItemsNeeded > 0) {
      val additionalItems = candidates
        .filter(col(ItemColumns.matchingCount) === countWhereSelectionIsNeeded)
        .orderBy(ItemColumns.matchingAttributes)
        .limit(numberOfAdditionalItemsNeeded.toInt)

      itemsWithoutSelection.union(additionalItems)
    } else
      itemsWithoutSelection

    unsortedRecommendations
      .orderBy(desc(ItemColumns.matchingCount), col(ItemColumns.matchingAttributes))
  }
}
