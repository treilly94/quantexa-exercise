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

  // TODO change test data to json to avoid type issues
  def readCSV(path: String): DataFrame = spark.read.option("header", "true").csv(path)


  "GroupAndSum" should "return the expected values" in {
    println("Input Data")
    val inDf: DataFrame = readCSV("./src/test/resources/inputs/question1.csv")
    inDf.show()
    println("Expected Data")
    val expDf: DataFrame = readCSV("./src/test/resources/expected/question1.csv")
    expDf.show()
    println("Output Data")
    val outDf: DataFrame = groupAndSum(inDf, List("transactionDay"), "transactionAmount", "totalValue")
        .orderBy("transactionDay")
    outDf.show()

    outDf.collect() should be(expDf.collect())
  }

}
