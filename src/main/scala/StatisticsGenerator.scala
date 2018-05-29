import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lag, mean, sum}
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
  }

  def groupAndMean(df: DataFrame, partCols: List[String], meanCol: String, name: String): DataFrame = {
    df.groupBy(partCols.head, partCols.tail: _*).agg(mean(meanCol).alias(name))
  }

  def lastFiveStats(df: DataFrame): DataFrame = {
    // Lag
    val w = Window.partitionBy("accountId").orderBy("transactionDay")
    df.withColumn("lag1", lag("transactionAmount", 1).over(w))
      .withColumn("lag2", lag("transactionAmount", 2).over(w))
      .withColumn("lag3", lag("transactionAmount", 3).over(w))
      .withColumn("lag4", lag("transactionAmount", 4).over(w))
      .withColumn("lag5", lag("transactionAmount", 5).over(w))

    // Max

    // Mean

    // Total for “AA”, “CC” and “FF”
  }
}
