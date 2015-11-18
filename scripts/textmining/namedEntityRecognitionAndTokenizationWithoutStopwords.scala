/**
 * The documents are saved in a CouchDB database. We will load the data in Scala, transform as
 * an RDD, apply named entity recognition (NER) and tokenization without stop words.
 *
 * NER is applied to extract Person, Organization, and Location for visualization in the later
 * step. Tokenization is applied to prepare a set of top frequent words in our corruption corpora,
 * which will be used to classify non-labelled articles.
 *
 * Note: We haven't fully incorporate a distributed environment since CouchDB is not deployed
 *       on a cluster. If we want to save all articles in a distributed No-SQL database, we may
 *       consider Couchbase, which can be scaled much better than CouchDB on a cluster. It can
 *       be integrated with HDFS for large storage. Besides, there is a Spark connector which
 *       enable us to query and load document directly as a RDD.
 *
 *       If we want to show how Spark can access CouchDB data in parallel, we might consider
 *       save articles (crawled from different website) in different CouchDB databases where
 *       each database could be accessed by one thread.
 */

import java.io._
import java.util.Properties

//import java.util.Properties

import scala.collection.JavaConversions._

import com.ibm.couchdb._

import edu.stanford.nlp.ling._
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ie.machinereading.structure.MachineReadingAnnotations
import edu.stanford.nlp.ie.crf._

import org.apache.lucene.analysis.{CharArraySet, StopAnalyzer}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import intoxicant.analytics.coreNlp._

import com.impanchao.crawlcorruption._


/////////////////////////////////
// Query articles from CouchDB //
/////////////////////////////////

/*
 Create an db instance to connect to the CouchDB database
 */
val couch = CouchDb("127.0.0.1", 5984)  // CouchDB client instance
val dbName = "fbi-public-corruption-news"  // database name
// CrawledCorruptionArticle is my own class (compiled)
val typeMapping = TypeMapping(classOf[CrawledCorruptionArticle] -> "CrawledCorruptionArticle")
val db = couch.db(dbName, typeMapping) // instance of the DB API

/*
 Query and load data from the database.

 If it is known that all documents in a database are of the same type,
 the following query returns all the documents in the database as a
 sequence where the full document contents are included.

val couchDocs = db.docs.getMany.queryIncludeDocs[CrawledCorruptionArticle].run  // CouchDocs object
couchDocs.getDocs  // Sequence of all articles as CrawledCorruptionArticle objects
val crawledCorruptionArticle = couchDocs.getDocs(0).doc  // the first article as a CrawledCorruptionArticle object
//crawledCorruptionArticle.content

 However, most of the time a database contains different types of documents,
 such as design document, article documents, etc. In order to pull all the
 CrawledCorruptionArticle-type document into Scala, we need to:

 1) create a view to return all CrawledCorruptionArticle-type article,
    which only needs to run once
 2) query the view so that they can be pulled into Scala

////////////////////////////////////////////////////////////The following is executed once
val allCrawledCorruptionArticlesView = CouchView(map =
  """
    |function(doc) {
    |   if (doc.kind == "CrawledCorruptionArticle") {
    |       emit(doc._id, doc.doc)
    |   }
    |}
  """.stripMargin)

val designDoc = CouchDesign(
  name = "tokenization-design",
  views = Map("allCrawledCorruptionArticle-view" -> allCrawledCorruptionArticlesView)
)

db.design.create(designDoc).run  // Create the desgin doc in the database.
////////////////////////////////////////////////////////////////////////////////////////
 */

// Create a query builder by querying the design document from the database
val allCrawledCorruptionArticlesViewQueryBuilder =
  db.query.view[String, CrawledCorruptionArticle](
      "tokenization-design", "allCrawledCorruptionArticle-view"
  ).get

// Query all crawled corruption articles from the database
val allCrawledCorruptionArticles = allCrawledCorruptionArticlesViewQueryBuilder.query.run


////////////////
// Create RDD //
////////////////

/*
Create SparkContext
 */
val sparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
val sc = new SparkContext(sparkConf)

val crawledCorruptionArticlesRDD =
  sc.parallelize(allCrawledCorruptionArticles.rows).map(
    couchKeyVal => couchKeyVal.value
  ) // RDD of articles, where each article is a CrawledCorruptionArticle object


//////////////////////////////
// Named Entity Recognition //
//////////////////////////////
val articleContentsRDD = crawledCorruptionArticlesRDD.map(doc => doc.content).persist()
// Build a classifier
// One can use either a 3, 4 or 7-class classifier. The details can be found at:
//  http://nlp.stanford.edu/software/CRF-NER.shtml
val serializedClassifier = "resources/StanfordNERclassifiers/english.all.3class.distsim.crf.ser.gz"
val classifier = CRFClassifier.getClassifier(serializedClassifier)

// Apply NER on each article
val nerClassifiedArticleContentsRDD = articleContentsRDD.map(a => classifier.classifyToCharacterOffsets(a))





/*
 To my understanding, multiple NLP tasks can be chained as a pipeline if we use
 Stanford NLP packages. So we can add tokenization and NER in the same pipeline.

 A serialized model is required for constructing a pipeline. So make sure the follwoing
 dependency is added in the "build.sbt":

 libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" classifier "models"
 */

//////////////////////////////////
// Create Stanford NLP pipeline //
//////////////////////////////////
val props = new Properties()
props.setProperty("annotators", "tokenize,ssplit,pos,lemma,parse,ner")  // not sure if they are all needed
val pipeline = new StanfordCoreNLP(props)
//val relationExtractionAnnotator = new RelationExtractorAnnotator(props)

/*
`pipline.annotate` and `relationExtractionAnnotator.annotate` annotate the input in-place.
In other words, the two methods are called for their side effects. Since RDD is immutable,
we need the following function to force the methods to return something so that we keep the
functional style.

TODO: Is there a better way?
 */
def annotateContent(content: Annotation, pipeline: StanfordCoreNLP) = {
  val contentCopy = content.copy()
  pipeline.annotate(contentCopy)
  contentCopy
}

val annotatedContentsRDD = crawledCorruptionArticlesRDD.
                              map(doc => doc.content).
                              map(s => new Annotation(s)).
                              map(a => annotateContent(a, pipeline)).persist()

annotatedContentsRDD.take(1)

