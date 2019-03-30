package SparkRdd
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkRdd {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    
    var conf = new SparkConf().setAppName("SparkRdd").setMaster("local[*]");
    var sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    val data = sc.textFile("file:///E:/Projects/Big Data/Big Datasets/ml-20m/movies.csv");
    data.foreach(println);
    
    
  }
}