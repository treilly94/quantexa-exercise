import scala.io.Source

object StatisticsGenerator {
  def main(args: Array[String]): Unit = {
    // Read data
    val data: List[Transaction] = csvParser("./src/main/resources/transactions.txt")
    println("======================Question1==============================")
    groupAndSum(data).foreach(println)
    println("======================Question2==============================")
    groupAndMean(data).foreach(println)
    println("======================Question3==============================")
    lastFiveStats(data).foreach(println)
  }

  def csvParser(fileName: String): List[Transaction] = {
    val transactionslines = Source.fromFile(fileName).getLines().drop(1)

    //Here we split each line up by commas and construct Transactions
    transactionslines.map { line =>
      val split = line.split(',')
      Transaction(split(0), split(1), split(2).toInt, split(3), split(4).toDouble)
    }.toList
  }

  def groupAndSum(data: List[Transaction]): Map[Int, Double] = {
    data.groupBy(_.transactionDay) // Generate a collection containing the transaction day and a list of all transactions under that day
      .mapValues(_.map(_.transactionAmount).sum) // Map the transactions within each list, Take only the amount and sum them
  }

  def groupAndMean(data: List[Transaction]): Map[String, Map[String, Double]] = {
    data.groupBy(_.accountId) // Group by account ID
      .mapValues(_.groupBy(_.category) // Within each group Group by category
      .mapValues(_.map(_.transactionAmount)) // Take only the transaction amounts within each group
      .mapValues(v => v.sum / v.size) // Divide the sum of the values by the number of values
    )
  }

  def lastFiveStats(data: List[Transaction]) = {
    // Window the data
    val windowedData: Map[String, List[List[Transaction]]] = data.groupBy(_.accountId) // Group by account ID
      .mapValues(_.sortWith(_.transactionDay > _.transactionDay)) // Sort by day from high to low
      .mapValues(_.sliding(5, 1).toList) // break into windows of 5 days

    // For each group get the ID and the windows
    windowedData.map { group =>
      val accountId: String = group._1 // Get account id
    val windows: List[List[Transaction]] = group._2 // Get list of windows

      // For each window get the statistics and create a new stats object
      windows.map { window =>
        val day: Int = window.head.transactionDay
        val max: Double = window.map(_.transactionAmount).max
        val mean: Double = window.map(_.transactionAmount).sum / window.map(_.transactionAmount).size
        val aa: Double = window.filter(_.category == "AA").map(_.transactionAmount).sum
        val cc: Double = window.filter(_.category == "CC").map(_.transactionAmount).sum
        val ff: Double = window.filter(_.category == "FF").map(_.transactionAmount).sum

        Stats(day, accountId, max, mean, aa, cc, ff)
      }
    }.flatten
  }

  case class Transaction(transactionId: String,
                         accountId: String,
                         transactionDay: Int,
                         category: String,
                         transactionAmount: Double)

  case class Stats(Day: Int,
                   AccountID: String,
                   Maximum: Double,
                   Average: Double,
                   AATotalValue: Double,
                   CCTotalValue: Double,
                   FFTotalValue: Double
                  )

}
