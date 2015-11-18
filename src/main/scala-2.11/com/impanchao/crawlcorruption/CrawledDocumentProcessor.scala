package com.impanchao.crawlcorruption

/**
 * Given an article link, we should be able to
 *  1) extract the content to our interest;
 *  2) serialize the object to JSON format;
 *  3) save the object on HDFS as a .json file
 */

import java.io.BufferedOutputStream
import java.util.Calendar
import scala.util.{Failure, Success, Try}
import org.apache.hadoop.{conf => HadoopConf}
import org.apache.hadoop.{fs => HadoopFileSystem}
import org.jsoup.{Jsoup, Connection}
import org.jsoup.nodes.Document
import com.fasterxml.jackson.databind.ObjectMapper


case class CrawledDocumentProcessor(url: String) extends Serializable {
  private val USERAGENT: String = "Mozilla/5.0 (X11; Linux x86_64) " + "AppleWebKit/535.21 (KHTML, like Gecko) " + "Chrome/45.0.2454.101 Safari/535.21"

  private val _fetchTime = Calendar.getInstance().getTime().toString
  def fetchTime = _fetchTime

  def parse: Document = {
    val conn: Connection = Jsoup.connect(url).userAgent(USERAGENT)
    conn.get()
  }

  // Extraction methods
  def getContent: String = {
    parse.getElementById("parent-fieldname-text").text()
  }

  def getTitle: String = {
    parse.select("h1[class=documentFirstHeading]").text()
  }

  def getTime: String = {
    parse.select("span[id=parent-fieldname-releaseDate]").text()
  }

  def extract: CrawledCorruptionArticle = {
    new CrawledCorruptionArticle(this.url, this.fetchTime, this.getTime, this.getTitle, this.getContent)
  }

  def formatAsJson: String = {
    val mapper = new ObjectMapper()
    mapper.writeValueAsString(this.extract)
  }

  @throws(classOf[Exception])
  def saveOnHDFS(fs: HadoopFileSystem.FileSystem, articlePath: HadoopFileSystem.Path): Unit = {
    val output = fs.create(articlePath)
    val os = new BufferedOutputStream(output)
    os.write(this.formatAsJson.getBytes("UTF-8"))
    os.close()
  }

  // Main function is used to illustrate the API
  // type sbt "run-main com.impanchao.crawlcorruption.CrawledDocumentProcessor" under project root to run
  def main(args: Array[String]): Unit = {
    val exampleUrl = "https://www.fbi.gov/neworleans/press-releases/2013/jeanne-gavin-pleads-guilty"

    // Create Hadoop FileSystem object for the current HDFS
    val namenode = "hdfs://127.0.0.1:9000"
    val hdfsConf = new HadoopConf.Configuration()
    hdfsConf.set("fs.defaultFS", namenode)
    val hdfs = HadoopFileSystem.FileSystem.get(hdfsConf)

    // Create path on HDFS to store a file
    val dir = "/"
    val fileName = "sampleArticle.json"
    val path = namenode + dir + fileName
    val articlePath = new HadoopFileSystem.Path(path)

    if (hdfs.exists(articlePath)) { hdfs.delete(articlePath, true) }
    hdfs.create(articlePath)

    // Crawl and save article
    Try {
      val processor = CrawledDocumentProcessor(exampleUrl)
      // print url
      println(processor.url)
      // print formatted article (as JSON string)
      println(processor.formatAsJson)
      // save on my local hdfs
      processor.saveOnHDFS(hdfs, articlePath)
    } match {
      // TODO: Logging
      case Success(x) => Some(x)
      case Failure(e) => println("Exception while parsing: " + e); None
    }
  }
}
