/**
  * Created by chenweigen on 2016/5/20.
  */
import scala.math.random
import org.apache.spark._

object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setSparkHome("E:/spark-1.6.1-bin-hadoop2.6")
      .setMaster("spark://192.168.241.113:7077").setJars(List("C:\\Users\\chenweigen\\IdeaProjects\\sparktest1\\out\\artifacts\\sparktest_jar\\sparktest.jar"))
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}