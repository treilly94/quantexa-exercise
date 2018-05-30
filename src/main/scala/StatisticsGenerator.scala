import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object StatisticsGenerator {
  def main(args: Array[String]): Unit = {
    // Preparation
    val spark: SparkSession = getSparkSession("Statistics Generator")
    spark.sparkContext.setLogLevel("ERROR")
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("./src/main/resources/transactions.txt")
    // Question1
    groupAndSum(df, List("transactionDay"), "transactionAmount", "totalValue").show()
    // Question2
    groupAndMean(df, List("accountId", "category"), "transactionAmount", "totalValue").show()
    // Question3
    lastFiveStats(df).show()
  }

  def getSparkSession(name: String): SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName(name)
      .getOrCreate()
  }

  def groupAndSum(df: DataFrame, partCols: List[String], sumCol: String, name: String): DataFrame = {
    df.groupBy(partCols.head, partCols.tail: _*).agg(sum(sumCol).alias(name))
      .withColumn(name, round(col(name), 2))
  }

  def groupAndMean(df: DataFrame, partCols: List[String], meanCol: String, name: String): DataFrame = {
    df.groupBy(partCols.head, partCols.tail: _*).agg(mean(meanCol).alias(name))
  }

  def lastFiveStats(df: DataFrame): DataFrame = {
    // Lag
    val w: WindowSpec = Window.partitionBy("accountId").orderBy("transactionDay")
    val dfLagged: DataFrame =
      df.withColumn("lagged", array(lag("transactionAmount", 1).over(w),
        lag("transactionAmount", 2).over(w),
        lag("transactionAmount", 3).over(w),
        lag("transactionAmount", 4).over(w),
        lag("transactionAmount", 5).over(w)))

    // Max
    dfLagged
    // Mean

    // Total for “AA”, “CC” and “FF”
  }
}
