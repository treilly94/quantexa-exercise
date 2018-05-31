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

  "groupAndMean" should "return the mean values for each account and catagory" in {
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

}
