package com.impanchao.crawlcorruption

/**
 * After parsed HTML and got Jsoup Document object,
 * we need to extract the content to our interest.
 */

import java.io.BufferedOutputStream
import java.util.Calendar
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.jsoup.{Jsoup, Connection}
import org.jsoup.nodes.Document
import com.fasterxml.jackson.databind.ObjectMapper

import scala.util.{Failure, Success, Try}


//class SerializableDocument(baseUri: String) extends Document(baseUri) with Serializable

case class CrawledDocumentProcessor(doc: Document) extends Serializable {
  val fetchTime = Calendar.getInstance().getTime().toString
  var url: String = ""

  def setUrl(url: String) = {this.url = url}

  // Extraction methods
  def getContent: String = {
    doc.getElementById("parent-fieldname-text").text()
  }

  def getTitle: String = {
    doc.select("h1[class=documentFirstHeading]").text()
  }

  def getTime: String = {
    doc.select("span[id=parent-fieldname-releaseDate]").text()
  }

  def extract: CrawledCorruptionArticle = {
    new CrawledCorruptionArticle(this.fetchTime, this.getTime, this.getTitle, this.getContent)
  }

  def formatAsJson: String = {
    val mapper = new ObjectMapper()
    mapper.writeValueAsString(this.extract)
  }

//  @throws(classOf[Exception])
  def saveOnHDFS(hdfsUri: String, dir: String, fileName: String): Unit = {
    val path = new Path(Array(hdfsUri, dir, fileName).mkString("/"))  // file dir and name
    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfsUri)
    val fs = FileSystem.get(conf)
    val output = fs.create(path)
    val os = new BufferedOutputStream(output)
//    os.write("hello, world".getBytes("UTF-8"))
    os.write(this.formatAsJson.getBytes("UTF-8"))
    os.close()
  }
}

/*
 By using a companion object, one can define multiple (auxiliary) constructors
 without being required to call the primary constructor as the first statement.
 While calling the constructor, no `new` is required.

 Details can be found at:
  http://daily-scala.blogspot.com/2009/11/multiple-constructors.html
  https://www.safaribooksonline.com/library/view/scala-cookbook/9781449340292/ch04s04.html
 */
object CrawledDocumentProcessor {
  private val USERAGENT: String = "Mozilla/5.0 (X11; Linux x86_64) " + "AppleWebKit/535.21 (KHTML, like Gecko) " + "Chrome/45.0.2454.101 Safari/535.21"

  // A second constructor
  @throws(classOf[Exception])
  def apply(urlToArticle: String) = {
    val conn: Connection = Jsoup.connect(urlToArticle).userAgent(USERAGENT)
    val doc: Document = conn.get()
    val processor = new CrawledDocumentProcessor(doc)  // primary constructor
    processor.setUrl(urlToArticle)
    processor
  }

  // Main function is used to illustrate the API
  def main(args: Array[String]): Unit = {
    val hdfsUri = "hdfs://127.0.0.1:9000"
    val dir = "/"
    val fileName = "article1.json"

    val exampleUrl = "https://www.fbi.gov/neworleans/press-releases/2013/jeanne-gavin-pleads-guilty"

    Try{
      val processor = CrawledDocumentProcessor(exampleUrl)
      // print url
      println(processor.url)
      // print formatted article (as JSON string)
      println(processor.formatAsJson)
      // save on my local hdfs
      processor.saveOnHDFS(hdfsUri, dir, fileName)
    } match {
      // TODO: Logging
      case Success(x) => Some(x)
      case Failure(e) => println("Exception while parsing: " + e); None
    }

    // type sbt "run-main com.impanchao.crawlcorruption.CrawledDocumentProcessor" under project root to run
  }
}
