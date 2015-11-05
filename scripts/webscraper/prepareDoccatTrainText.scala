import java.io._
import java.util.{Date, Calendar}

import scala.collection.JavaConversions._

import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import org.jsoup.{Connection, Jsoup}

/**
 * Recursively crawl a website to collect articles
 * for manual annotation, either corrupt or non-corrupt.
 */

val newsListUrl: String = "https://www.fbi.gov/collections/public-corruption"
/*
It seems that the fbi's server won't let you connect without a
properly specified User Agent in the header.
 */
val userAgentString: String = {"Mozilla/5.0 (X11; Linux x86_64) " +
                              "AppleWebKit/535.21 (KHTML, like Gecko) " +
                              "Chrome/19.0.1042.0 Safari/535.21"}
val conn: Connection = Jsoup.connect(newsListUrl).userAgent(userAgentString)
var doc: Document = conn.get()

/*
1. Extract all article link elements <a href="url-link-to-article" ...> some-description </a>
2. Extract all url strings
3. For each url, fetch the corresponding article
4. After fetched the article string, save it as a single txt file.
5. Get some other information from the article page
 */
var articleLinks: Elements = doc.getElementsByTag("dl").select("a")
var links: List[String] = articleLinks.toList.map {link => link.attr("href")}

def getArticleContent(doc: Document): String = {
  doc.getElementById("parent-fieldname-text").text()
}

def getArticleTitle(doc: Document): String = {
  doc.select("h1[class=documentFirstHeading]").text()
}

def getArticleTime(doc: Document): String = {
  doc.select("span[id=parent-fieldname-releaseDate]").text()
}

//var url: String = links(0)
//var conn: Connection = Jsoup.connect(url).userAgent(userAgentString)
//var doc: Document = conn.get()
//getArticleTime(doc)


var counter: Int = 0
for (link <- links){
  counter += 1
  print(counter + "\n")

  var fetchTime: Date = Calendar.getInstance().getTime()
  println("Fetch time: " + fetchTime + "\n")

  var conn_to_link: Connection = Jsoup.connect(link).userAgent(userAgentString)
  var link_doc: Document = conn_to_link.get()

  // get article release time
  var releaseTime: String = getArticleTime(link_doc)
  println("Release time: " + releaseTime + "\n")

  // get article title
  var articleTitle: String = getArticleTitle(link_doc)
  println(articleTitle + "\n")

  // get article content
  var articleContent: String = getArticleContent(link_doc)
  println(articleContent + "\n")
}

/*
Crawl all articles on https://www.fbi.gov/collections/public-corruption,
where each page has only 20 article links. We want to traverse all pages.
 */
var pageIndex: Array[Int] = (0 to 1940 by 20).toArray
val baseUrl: String = "https://www.fbi.gov/collections/public-corruption?b_start:int="
//baseUrl + pageIndex(0)
pageIndex.map(index => baseUrl + index).foreach(???)  // apply above for each page


/**
 * Create a class for the above codes.
 */
//class FbiGovPublicCorruptionNewsCrawler {
//  /*
//  It seems that the fbi's server won't let you connect without a
//  properly specified User Agent in the header.
//   */
//  val userAgentString: String = "Mozilla/5.0 (X11; Linux x86_64) " +
//    "AppleWebKit/535.21 (KHTML, like Gecko) " +
//    "Chrome/19.0.1042.0 Safari/535.21"
//
//  var pageIndex: Array[Int] = (0 to 1940 by 20).toArray
//  val baseUrl: String = "https://www.fbi.gov/collections/public-corruption?b_start:int="
//
//  var counter: Int = 0  // track crawled articles
//
//  def getArticleContent(doc: Document): String = {
//    doc.getElementById("parent-fieldname-text").text()
//  }
//
//  def getArticleTitle(doc: Document): String = {
//    doc.select("h1[class=documentFirstHeading]").text()
//  }
//
//  def getArticleTime(doc: Document): String = {
//    doc.select("span[id=parent-fieldname-releaseDate]").text()
//  }
//
//  def printAllFromOneLinkPage(index: Int): Unit = {
//    val url: String = baseUrl + index
//    val conn: Connection = Jsoup.connect(url).userAgent(userAgentString)
//    val doc: Document = conn.get()
//
//    // extract article links
//    val articleLinks: Elements = doc.getElementsByTag("dl").select("a")
//    val links: List[String] = articleLinks.toList.map {link => link.attr("href")}
//
//    // print results for each article
////    var counter: Int = 0
//    for (link <- links){
//      counter += 1
//      print(counter + "\n")
//
//      var fetchTime: Date = Calendar.getInstance().getTime()
//      println("Fetch time: " + fetchTime + "\n")
//
//      var conn_to_link: Connection = Jsoup.connect(link).userAgent(userAgentString)
//      var link_doc: Document = conn_to_link.get()
//
//      // get article release time
//      var releaseTime: String = getArticleTime(link_doc)
//      println("Release time: " + releaseTime + "\n")
//
//      // get article title
//      var articleTitle: String = getArticleTitle(link_doc)
//      println(articleTitle + "\n")
//
//      // get article content
//      var articleContent: String = getArticleContent(link_doc)
//      println(articleContent + "\n")
//    }
//  }
//
//  def printAll(): Unit = {
//    pageIndex.foreach(printAllFromOneLinkPage)
//  }
//
//}

//val fbiGovPublicCorruptionNewsCrawler: FbiGovPublicCorruptionNewsCrawler = new FbiGovPublicCorruptionNewsCrawler()
//fbiGovPublicCorruptionNewsCrawler.printAll()

/**
 * Prepare the crawled article in OpenNLP Document Categorizer training format.
 * This is one document per line, containing category and text separated by a whitespace.
 *
 * All labelled articles are saved in one txt file.
 */

/*
Format article with labels for writing to file.

The articles crawled from FBI's website are all labeled "corruption".
 They are the positive examples in our training set.
 Later on, we will augment this set with other "noncorruption" articles.
 */
def formatTrainArticleWithPositiveLabel(articleContent: String): String = {
  "corruption" + " " + articleContent
}

def formatTrainArticleWithNegativeLabel(articleContent: String): String = {
  "noncorruption" + " " + articleContent
}


/*
Write text to file.
 */
val fw: FileWriter = new FileWriter("temp/doccat_train.txt", true)
links.foreach{ link => fw.write(link + "\n")}
fw.close()

/**
 * Create a class for the above codes.
 */
class FbiGovPublicCorruptionNewsCrawler {
  /*
  It seems that the fbi's server won't let you connect without a
  properly specified User Agent in the header.
   */
  val userAgentString: String = "Mozilla/5.0 (X11; Linux x86_64) " +
    "AppleWebKit/535.21 (KHTML, like Gecko) " +
    "Chrome/45.0.2454.101 Safari/535.21"

  var pageIndex: Array[Int] = (0 to 1940 by 20).toArray
  val baseUrl: String = "https://www.fbi.gov/collections/public-corruption?b_start:int="

  val fileToWrite: String = "temp/doccat_train.txt"

  var counter: Int = 0  // track crawled articles

  def getArticleContent(doc: Document): String = {
    doc.getElementById("parent-fieldname-text").text()
  }

  def getArticleTitle(doc: Document): String = {
    doc.select("h1[class=documentFirstHeading]").text()
  }

  def getArticleTime(doc: Document): String = {
    doc.select("span[id=parent-fieldname-releaseDate]").text()
  }

  def printAllFromOneLinkPage(index: Int): Unit = {
    val url: String = baseUrl + index
    val conn: Connection = Jsoup.connect(url).userAgent(userAgentString)
    val doc: Document = conn.get()

    // extract article links
    val articleLinks: Elements = doc.getElementsByTag("dl").select("a")
    val links: List[String] = articleLinks.toList.map {link => link.attr("href")}

    // print results for each article
    //    var counter: Int = 0
    for (link <- links){
      counter += 1
      print(counter + "\n")

      var fetchTime: Date = Calendar.getInstance().getTime()
      println("Fetch time: " + fetchTime + "\n")

      var conn_to_link: Connection = Jsoup.connect(link).userAgent(userAgentString)
      var link_doc: Document = conn_to_link.get()

      // get article release time
      var releaseTime: String = getArticleTime(link_doc)
      println("Release time: " + releaseTime + "\n")

      // get article title
      var articleTitle: String = getArticleTitle(link_doc)
      println(articleTitle + "\n")

      // get article content
      var articleContent: String = getArticleContent(link_doc)
      println(articleContent + "\n")
    }
  }

  def printAll(): Unit = {
    pageIndex.foreach(printAllFromOneLinkPage)
  }

  def formatTrainArticleWithPositiveLabel(articleContent: String): String = {
    "corruption" + " " + articleContent
  }

  def formatTrainArticleWithNegativeLabel(articleContent: String): String = {
    "noncorruption" + " " + articleContent
  }

  def openFile(): OutputStreamWriter = {
//    new FileWriter(writeToFile)
    /*
    Using OutputStreamWriter instead, we can specify encoding.
    Reference: http://stackoverflow.com/questions/1001540/how-to-write-a-utf-8-file-with-java
     */
    var fileOutputStream = new FileOutputStream(fileToWrite, true)
    new OutputStreamWriter(fileOutputStream, "UTF-8")
  }

  def closeFile(writer: OutputStreamWriter): Unit = {
    writer.close()
  }

  def saveAllFromOneLinkPage(index: Int, writer: OutputStreamWriter): Unit = {
    val url: String = baseUrl + index
    val conn: Connection = Jsoup.connect(url).userAgent(userAgentString)
    val doc: Document = conn.get()

    // extract article links
    val articleLinks: Elements = doc.getElementsByTag("dl").select("a")
    val links: List[String] = articleLinks.toList.map {link => link.attr("href")}

    for (link <- links){
      counter += 1
      print(counter + "\n")

      var conn_to_link: Connection = Jsoup.connect(link).userAgent(userAgentString)
      var link_doc: Document = conn_to_link.get()
      var articleContent: String = getArticleContent(link_doc)
      // each article is a newline
      var labeledArticle: String = formatTrainArticleWithPositiveLabel(articleContent + "\n")
      writer.write(labeledArticle)
    }
  }

  def saveAll(writer: OutputStreamWriter): Unit = {
    pageIndex.foreach(x => saveAllFromOneLinkPage(x, writer))
  }
}

// test
val fbiGovPublicCorruptionNewsCrawler: FbiGovPublicCorruptionNewsCrawler = new FbiGovPublicCorruptionNewsCrawler()
val writer: OutputStreamWriter = fbiGovPublicCorruptionNewsCrawler.openFile()
try {
  fbiGovPublicCorruptionNewsCrawler.saveAll(writer)
} finally {
  fbiGovPublicCorruptionNewsCrawler.closeFile(writer)
}


//var filename = "temp/myfile.txt"
//var fileOutputStream = new FileOutputStream(filename, true)
//var writer = new OutputStreamWriter(fileOutputStream, "UTF-8")
//writer.write("haha")
//writer.close()

//for (x <- 1 to 10) {
//  println(x)
//  Thread.sleep(5000)
//}