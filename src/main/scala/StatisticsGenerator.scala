import scala.io.Source

object StatisticsGenerator {
  def main(args: Array[String]): Unit = {
    // Read data
    val data: List[Transaction] = csvParser("./src/main/resources/transactions.txt")
    data.foreach(println)
  }

  def csvParser(fileName: String): List[Transaction] = {
    val transactionslines = Source.fromFile(fileName).getLines().drop(1)

    //Here we split each line up by commas and construct Transactions
    transactionslines.map { line =>
      val split = line.split(',')
      Transaction(split(0), split(1), split(2).toInt, split(3), split(4).toDouble)
    }.toList
  }

  case class Transaction(transactionId: String,
                         accountId: String,
                         transactionDay: Int,
                         category: String,
                         transactionAmount: Double)

}
