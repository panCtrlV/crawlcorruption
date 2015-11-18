import org.apache.hadoop.{fs => HadoopFileSystem, conf => HadoopConf}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.sys.process._

import com.impanchao.crawlcorruption._


val sparkConf = new SparkConf().setAppName("Read Fbi Articles").setMaster("local[4]")
val sparkContext = new SparkContext(sparkConf)

// Get the path to the data folder
var cmd = "hdfs getconf -nnRpcAddresses"
val namenode = "hdfs://" + cmd.!! .toString.trim
var user = System.getProperty("user.name")
var dir = "/user/" + user + "/stat598bigdata/fbi-public-corruption-articles"
val fbiArticleDir = namenode + dir

// Read articles as RDD
val articlesRDD = sparkContext.wholeTextFiles(fbiArticleDir).persist()
var article = articlesRDD.take(1)(0)
article._1

/*
  TODO:
    1. Document classification:
      1) Tokenize all articles, and calculate the top 1000 word counts based on labeled articles.
      1.1) Optionally, remove the stop words.
      2) Classify un-labeled articles by word matching against the word dictionary obtained form (1).

        // An example of combine words counts from different articles
        val article1 = Tuple2("title1", Array(("a", 10), ("b", 5), ("c", 20)))
        val article2 = Tuple2("title2", Array(("a", 10), ("c", 20)))
        val article3 = Tuple2("title3", Array(("b", 5), ("c", 20)))

        val articleRDD = sparkContext.parallelize(Array(article1, article2, article3))
        val articleContentRDD = articleRDD.flatMap(_._2)
        articleContentRDD.reduceByKey(_+_).collect().foreach(println)

    2. Named entity extraction
      1) For each classified article, extract the entities using Stanford NER 7-class classifier.
      2) For each article, compile the entities according to the following format:

        article1_title, location, person, organization, article_time, money
        ...

        and save as a single json file.

    3. Semantic analysis? Think about it ...
 */


