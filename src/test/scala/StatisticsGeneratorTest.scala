import StatisticsGenerator._
import org.scalatest.{FlatSpec, Matchers}

class StatisticsGeneratorTest extends FlatSpec with Matchers {

  "groupAndSum" should "return summed values for each day" in {
    val input: List[Transaction] = List(
      Transaction("1", "1", 1, "a", 10.5),
      Transaction("2", "1", 1, "a", 10.0),
      Transaction("3", "1", 2, "a", 77.7),
      Transaction("4", "1", 3, "a", 1.0)
    )

    val expected: Map[Int, Double] = Map(
      1 -> 20.5,
      2 -> 77.7,
      3 -> 1.0
    )
    groupAndSum(input) should be(expected)
  }

  "groupAndMean" should "return the mean values for each account and category" in {
    val input: List[Transaction] = List(
      Transaction("1", "1", 1, "a", 10.5),
      Transaction("2", "1", 1, "a", 5.5),
      Transaction("3", "1", 2, "b", 9.0),
      Transaction("4", "2", 3, "a", 1.0),
      Transaction("5", "2", 2, "a", 100.0),
      Transaction("6", "3", 3, "c", 1.0)
    )

    val expected: Map[String, Map[String, Double]] = Map(
      "1" -> Map("a" -> 8.0, "b" -> 9.0),
      "2" -> Map("a" -> 50.5),
      "3" -> Map("c" -> 1.0)
    )
    groupAndMean(input) should be(expected)
  }

  "lastFiveStats" should "return a valid stats object for each account and day" in {
    val input: List[Transaction] = List(
      Transaction("00", "10", 0, "AA", 1.00),
      Transaction("01", "10", 1, "AA", 1.00),
      Transaction("02", "10", 2, "AA", 1.00),
      Transaction("03", "10", 3, "AA", 1.00),
      Transaction("04", "10", 4, "AA", 1.00),
      Transaction("05", "10", 5, "AA", 1.00),
      Transaction("96", "20", 4, "BB", 1.11),
      Transaction("06", "20", 5, "BB", 1.22),
      Transaction("07", "20", 6, "FF", 1.33),
      Transaction("08", "20", 7, "AA", 1.44),
      Transaction("09", "20", 8, "CC", 1.55),
      Transaction("10", "20", 9, "CC", 1.66),
      Transaction("11", "30", 4, "CC", 5.00),
      Transaction("12", "30", 5, "AA", 4.00),
      Transaction("13", "30", 6, "FF", 3.00),
      Transaction("14", "30", 7, "SS", 2.00),
      Transaction("15", "30", 8, "KK", 1.00),
      Transaction("11", "30", 9, "CC", 0.00)
    )

    val expected: List[Stats] = List(
      Stats(9, "30", 5.00, 3.000, 4.00, 5.00, 3.00),
      Stats(9, "20", 1.55, 1.330, 1.44, 1.55, 1.33),
      Stats(5, "10", 1.00, 1.000, 5.00, 0.00, 0.00)
    )
    lastFiveStats(input) should be(expected)
  }

}
