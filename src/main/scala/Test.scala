import com.gtest.WordSimilarityWithG2Test
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by snehal.mistry on 4/9/17.
  */
object Test {
  def main(args: Array[String]) {
    val logFile = ""
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Snehal-Spark")
      .set("spark.executor.memory", "2g")

    var sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://localhost:9000")
    val wordSimilarity = new WordSimilarityWithG2Test(sc, "hdfs:///user/snehal.mistry/blogs")
    println(wordSimilarity.findSimilarity("original", "new"))
  }
}
