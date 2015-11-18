package com.impanchao.crawlcorruption

/**
 * Now all the articles crawled from a website are assumed to
 * be saved in a folder / directory on HDFS. We should be able
 * to:
 *  1) read them into memory as a Spark RDD[CrawledCorruptionArticle]
 *  2) apply some NLP tasks ...
 *
 *  All JSON files in the directory should have the same structure.
 */

import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.hadoop.{fs => HadoopFileSystem}
import org.apache.spark.SparkContext


case class CrawledArticleFilesProcessors(sc: SparkContext,
                                         dirPath: String,
                                         mapper: ObjectMapper) extends java.io.Serializable {
  val readFilesFromHDFS = {
    // Use Spark API to read files
    val allFbiArticlesTextFilesRDD = sc.wholeTextFiles(dirPath)

    // Deserialize to RDD[CrrawledCorruptionArticle]
    allFbiArticlesTextFilesRDD.
      flatMap(t => {
        try {
          Some(this.mapper.readValue(t._2, classOf[CrawledCorruptionArticle]))
        } catch {
          case e: Exception => println("Error in deserialization " + e); None
        }
      })
  }
}


/*
Use companion object to implement multiple constructors. Details can be found at:
  http://daily-scala.blogspot.com/2009/11/multiple-constructors.html
 */
object CrawledArticleFilesProcessors {
  def apply(sc: SparkContext, dirPath: HadoopFileSystem.Path, mapper: ObjectMapper) = {
    new CrawledArticleFilesProcessors(sc, dirPath.toString, mapper)
  }
}
