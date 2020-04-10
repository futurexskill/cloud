package common

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object SparkTraformer {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def replaceNullValues(dataFrame : DataFrame) : DataFrame = {
    logger.warn("replaceNullValues method started")

    val transformedDF = dataFrame.na.fill("Unknown",Seq("author_name"))
      .na.fill(value = "0",Seq("no_of_reviews"))
    logger.warn("replaceNullValues method ended")
    transformedDF
  }
}
