import App.import_data

import collection.mutable.Stack
import org.junit.Assert.assertEquals
import org.scalatest.FailureMessages.should
import org.scalatest.Resources.should
import org.scalatest._
import org.scalatest.matchers.should.Matchers


class TestSpec extends FlatSpec with Matchers {

  "import" should "import file in dataframe" in {

    case class data(deo: Int, ptopt: Int)
    val results = import_data(spark, ";", "esgi_tp/Communes.csv").select("DEPCOM", "PTOT")

    results.as[data].collect().take(1) should contain theSameElementsAs Array(
      data(01001, 794),
    )

  }

}
