import java. io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf,SparkContext }
object WordCount {
  val inputFile= "E:\\dataset\\word2.txt"
  val outputFile= "E:\\dataset\\output.txt"
  def main (args: Array [String]): Unit ={
    val conf = new SparkConf ( ) .setMaster("local") . setAppName ("My App")
    val sc =new SparkContext (conf)
    val inputRDD: RDD [String]= sc. textFile(inputFile)
    println (s"Total Lines:${inputRDD.count()}")
    val contentArr : Array [ String] =inputRDD.collect ( )
    println ( "content : " )
    contentArr.foreach (println)
    val words: RDD [ String] =inputRDD. flatMap (line=> line.split ( " "))
    val count1PerWords: RDD[ (String, Int) ] = words.map (word => (word,1))
    val counts: RDD[ (String, Int) ] = count1PerWords.reduceByKey { case
      (counter , nextVal) => counter + nextVal }
    FileUtils . deleteQuietly(new File (outputFile) )
    counts.saveAsTextFile (outputFile)
    println ( "Program executed successfully")
  }
}