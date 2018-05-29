import org.apache.spark.sql.functions.{mean, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StatisticsGenerator {
  def main(args: Array[String]): Unit = {
    // Preparation
    val spark: SparkSession = getSparkSession("Statistics Generator")
    spark.sparkContext.setLogLevel("ERROR")
    val df: DataFrame = spark.read.option("header", "true").csv("./src/main/resources/transactions.txt")
    // Question1
    groupAndSum(df, List("transactionDay"), "transactionAmount", "totalValue").show()
    // Question2
    groupAndMean(df, List("accountId", "category"), "transactionAmount", "totalValue").show()
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
}
