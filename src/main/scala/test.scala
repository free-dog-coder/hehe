class test {

}


object sparkTest {

  import org.apache.log4j._
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    //    println("Hello World!")


    import scala.math.random

    import org.apache.spark._

    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local").setAppName("PI")
    val sc = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = sc.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    sc.stop()

  }
}
