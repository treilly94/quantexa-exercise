import scala.io.Source

object StatisticsGenerator {
  def main(args: Array[String]): Unit = {
    // Read data
    val data: List[Transaction] = csvParser("./src/main/resources/transactions.txt")
    println("======================Question1==============================")
    data.foreach(println)
    println("======================Question2==============================")
    groupAndSum(data).foreach(println)
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

  case class Transaction(transactionId: String,
                         accountId: String,
                         transactionDay: Int,
                         category: String,
                         transactionAmount: Double)

}
