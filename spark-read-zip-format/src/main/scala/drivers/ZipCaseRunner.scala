package drivers

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD

/**
  * Example of how to read into a Spark RDD, content from .zip files.
  *
  * Where, each files lines is an element within the RDD, which is RDD[String]
  *
  * This is just a sample of how to read the FIle content into an RDD[String]
  * Therefore, the code reads into the RDD, and calls foreach, printing each line
  *
  */
object ZipCaseRunner extends App {


  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ZipCaseRunner"))


  //reading from
  val rdd = sc.readFile(path = "spark-read-zip-format/src/main/resources/*.zip")

  var nr = 0
  rdd.foreach { l =>
    nr += 1
    println(s"lines nr: $nr , line: $l")
  }

  sc.stop()

  implicit class ZipSparkContext(val sc: SparkContext) extends AnyVal {

    def readFile(path: String,
                 minPartitions: Int = sc.defaultMinPartitions): RDD[String] = {

      if (path.endsWith(".zip")) {
        sc.binaryFiles(path, minPartitions)
          .flatMap { case (name: String, content: PortableDataStream) =>
            val zis = new ZipInputStream(content.open)
            Stream.continually(zis.getNextEntry)
              .takeWhile(_ != null)
              .flatMap { _ =>
                val br = new BufferedReader(new InputStreamReader(zis))
                Stream.continually(br.readLine()).takeWhile(_ != null)
              }
          }
      } else {
        sc.textFile(path, minPartitions)
      }
    }
  }

}



