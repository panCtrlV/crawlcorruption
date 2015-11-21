/*
TODO:
1) save corruption dictionary words to Hardrvie as Json string     (done)
2) write a function which input is HDFS path of articles, output is RDD[my CorruptionArticle]  (done)
3) redefine my CorruptionArticle class which include a method named tokenize  (done)
*/

import com.tongx.crawlcorruption.CrawledCorruptionArticle
import net.liftweb.json._


val hdfsPath: String = "/user/tongx/BigDataProj/articles/politics"

def parseString(article: String) = {
  implicit val formats = DefaultFormats
  val articleJson = parse(article)
  articleJson.extract[CrawledCorruptionArticle]
}


sc.textFile(hdfsPath).map(x => parseString(x)).map(_.tokenize(true)).take(10)

