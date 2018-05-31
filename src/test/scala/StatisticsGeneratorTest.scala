import StatisticsGenerator._
import org.scalatest.{FlatSpec, Matchers}

class StatisticsGeneratorTest extends FlatSpec with Matchers {

  "groupAndSum" should "return summed values for each group" in {
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

}
