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
      .withColumn(name, round(col(name), 2)) // Rounding added because of floating point like error
  }

  def groupAndMean(df: DataFrame, partCols: List[String], meanCol: String, name: String): DataFrame = {
    df.groupBy(partCols.head, partCols.tail: _*).agg(mean(meanCol).alias(name))
      .withColumn(name, round(col(name), 2)) // Rounding added because of floating point like error
  }

  def lastFiveStats(df: DataFrame): DataFrame = {

    // Temp columns
    val lAmount: String = "lAmount"
    val lCat: String = "lCat"

    // Lag
    val w: WindowSpec = Window.partitionBy("accountId").orderBy("transactionDay")
    val dfLagged: DataFrame =
      df.withColumn(lAmount, array(lag("transactionAmount", 1, null).over(w),
        lag("transactionAmount", 2, null).over(w),
        lag("transactionAmount", 3, null).over(w),
        lag("transactionAmount", 4, null).over(w),
        lag("transactionAmount", 5, null).over(w)))
        .withColumn(lCat, array(lag("category", 1, null).over(w),
          lag("category", 2, null).over(w),
          lag("category", 3, null).over(w),
          lag("category", 4, null).over(w),
          lag("category", 5, null).over(w)))

    // Max
    val maxUDF = udf { s: Seq[Double] => s.max }
    // Mean
    val meanUDF = udf { s: Seq[Double] => s.sum / s.count(_ > 0) }
    // Total for “AA”, “CC” and “FF”
    val sumCatUDF = udf {
      (amounts: Seq[Double], cat: Seq[String], target: String) => amounts.zip(cat).filter(_._2 == target).map { case (x, y) => x }.sum
    }

    dfLagged.withColumn("Maximum", maxUDF(col(lAmount)))
      .withColumn("Average", meanUDF(col(lAmount)))
      .withColumn("AA Total Value", sumCatUDF(col(lAmount), col(lCat), lit("AA")))


  }
}
