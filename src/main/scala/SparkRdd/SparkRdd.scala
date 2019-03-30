package SparkRdd
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql


object SparkRdd {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    
    var conf = new SparkConf().setAppName("SparkRdd").setMaster("local[*]");
    var sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    // we have use 4 core processor. 
    var movies = sc.textFile("file:///E:/Projects/Big Data/Big Datasets/ml-20m/movies.csv",4);
    var ratings = sc.textFile("file:///E:/Projects/Big Data/Big Datasets/ml-20m/ratings.csv");
    var uniondata  = movies.union(ratings);
    
    ratings.foreach(println);
    
    
  }
}