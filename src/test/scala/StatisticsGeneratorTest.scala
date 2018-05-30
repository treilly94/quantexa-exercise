import StatisticsGenerator._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class StatisticsGeneratorTest extends FlatSpec with Matchers {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Test Session")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def orderCols(df: DataFrame): DataFrame = {
    val cols: Array[String] = df.columns.sorted
    df.select(cols.head, cols.tail: _*)
  }

  def readJSON(path: String): DataFrame = orderCols(spark.read.json(path))


  "GroupAndSum" should "return the expected values" in {
    println("Input Data")
    val inDf: DataFrame = readJSON("./src/test/resources/inputs/question1.json")
    inDf.show()
    println("Expected Data")
    val expDf: DataFrame = readJSON("./src/test/resources/expected/question1.json")
    expDf.show()
    println("Output Data")
    val outDf: DataFrame = orderCols(
      groupAndSum(inDf, List("transactionDay"), "transactionAmount", "totalValue")
    )
      .orderBy("transactionDay")
    outDf.show()

    outDf.collectAsList() should be(expDf.collectAsList())
  }

  "GroupAndMean" should "return the expected values" in {
    println("Input Data")
    val inDf: DataFrame = readJSON("./src/test/resources/inputs/question2.json")
    inDf.show()
    println("Expected Data")
    val expDf: DataFrame = readJSON("./src/test/resources/expected/question2.json")
    expDf.show()
    println("Output Data")
    val outDf: DataFrame = orderCols(
      groupAndMean(inDf, List("transactionDay"), "transactionAmount", "totalValue")
    )
      .orderBy("transactionDay")
    outDf.show()

    outDf.collectAsList() should be(expDf.collectAsList())
  }

  "LastFiveStats" should "return the expected values" in {
    println("Input Data")
    val inDf: DataFrame = readJSON("./src/test/resources/inputs/question3.json")
    inDf.show()
    println("Expected Data")
    val expDf: DataFrame = readJSON("./src/test/resources/expected/question3.json")
    expDf.show()
    println("Output Data")
    val outDf: DataFrame = orderCols(
      lastFiveStats(inDf)
    )
      .orderBy("accountId", "transactionDay")
    outDf.show()

    outDf.collectAsList() should be(expDf.collectAsList())
  }

}
