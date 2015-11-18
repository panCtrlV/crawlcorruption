/**
 * This script crawl and save the FBI documents on HDFS.
 *
 * Scaping articles are performed in parallel through Spark.
 * Then the articles are serialized as JSON and save on HDFS.
 *
 * One can rename import modules, see:
 *  http://blog.bruchez.name/2012/06/scala-tip-import-renames.html
 */

import java.util.UUID
import scala.sys.process._
import scala.collection.JavaConversions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.{conf => HadoopConf}
import org.apache.hadoop.{fs => HadoopFileSystem}
import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import org.jsoup.{Jsoup, Connection}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import com.impanchao.crawlcorruption._


object CrawlSaveAndReadArticlesOnHdfsWithSparkExample extends App {
  /*
  Create Spark Context
  */
  val conf = new SparkConf().setAppName("Crawl FBI Articles").setMaster("local[4]")
  val sc = new SparkContext(conf)

  /*
  HDFS URI and target folder
  */
  ///////////////////////////////////////////////////////
  // Alternatively, we can use Scala and Hadoop API to //
  // talk to native HDFS                               //
  ///////////////////////////////////////////////////////
  var cmd = "hdfs getconf -nnRpcAddresses"
  val namenode = "hdfs://" + cmd.!! .toString.trim
  var user = System.getProperty("user.name")
  var dir = "/user/" + user + "/stat598bigdata/fbi-public-corruption-articles"

  // Create Hadoop FileSystem object for the current HDFS
  val hdfsConf = new HadoopConf.Configuration()
  hdfsConf.set("fs.defaultFS", namenode)
  val hdfs = HadoopFileSystem.FileSystem.get(hdfsConf)

  // Create directory for storing articles on HDFS
  var fbiArticleDirPath = new HadoopFileSystem.Path(namenode + dir)
  if (hdfs.exists(fbiArticleDirPath)) { hdfs.delete(fbiArticleDirPath, true) }
  hdfs.mkdirs(fbiArticleDirPath)

  /*
  Crawl articles and save on HDFS
  */
  ///////////////////////////
  // Get all article links //
  ///////////////////////////
  val userAgentString: String = "Mozilla/5.0 (X11; Linux x86_64) " + "AppleWebKit/535.21 (KHTML, like Gecko) " + "Chrome/45.0.2454.101 Safari/535.21"

  // Use Spark accumulator as a counter
  val counter = sc.accumulator(0, "Crawled Article Counter")

  // Prepare links to articles
  var pageIndexArray: Array[Int] = (0 to 1980 by 20).toArray  // 1980 as of Nov. 11 2015
  val baseUrl: String = "https://www.fbi.gov/collections/public-corruption?b_start:int="

  // Get article links from one page
  def getLinksFromOnePage(index: Int) = {
    val url: String = baseUrl + index
    val connToLinkPage: Connection = Jsoup.connect(url).userAgent(userAgentString)
    val docOfLinkPage: Document = connToLinkPage.get()
    // extract all article links on the page
    val articleLinks: Elements = docOfLinkPage.getElementsByTag("dl").select("a")
    articleLinks.toList.map {link => link.attr("href")}
  }

  // Get all article links
  val pageIndexRDD =  sc.parallelize(pageIndexArray)
  val articleLinksRDD = pageIndexRDD.flatMap(ind => getLinksFromOnePage(ind)).persist()

  //////////////////////////////////////////
  // Crawll all articles and save on HDFS //
  //////////////////////////////////////////
  // Crawl each link as a processor
  val processorsRDD =
    articleLinksRDD.
      map(link =>{
        try {
          Some(CrawledDocumentProcessor(link))
        } catch {
          case e: Exception => println("Error in creating CrawledDocumentProcessor " + e)
            None
        }
      }).
      map(_.get)

  // Call processor.saveOnHDFS to save each article on native HDFS
  processorsRDD.
    foreach(processor => {
      counter += 1
      val filename = "article-" + UUID.randomUUID() + ".json"
      println(counter + ": " + filename + ": " + processor.url)
      try {
        val fbiArticlePath = new HadoopFileSystem.Path(fbiArticleDirPath.toString + "/" + filename)
        processor.saveOnHDFS(hdfs, fbiArticlePath)
      } catch {
        case e: Exception => println("Error writing to HDFS " + e); None
      }
    })

  /*
  Now, we have articles saved on HDFS as .json and we want to
  read them back as Spark RDD of CrawledCorruptionArticle objects.
  */
  // Create mapper
  //  Reference: http://stackoverflow.com/questions/18027233/jackson-scala-json-deserialization-to-case-classes
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  // Read files from HDFS using Spark API
  val allFbiArticlesTextFilesRDD = sc.wholeTextFiles(fbiArticleDirPath.toString)

  // Deserialize JSON string to CrawledCorruptionArticle objects
  // Reference: http://stackoverflow.com/questions/4089537/scala-catching-an-exception-within-a-map
  val allFbiArticlesObjectsRDD = allFbiArticlesTextFilesRDD.
    flatMap(t => {
      try {
        Some(mapper.readValue(t._2, classOf[CrawledCorruptionArticle]))
      } catch {
        case e: Exception => println("Error in deserialization " + e); None
      }
    }).persist

  println(allFbiArticlesObjectsRDD.collect().length)
}
