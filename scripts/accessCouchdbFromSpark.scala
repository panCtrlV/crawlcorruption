import java.io._
import java.util.Properties

import akka.serialization.JavaSerializer

import scala.collection.JavaConversions._

import com.ibm.couchdb._

import edu.stanford.nlp.ling._
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ie.machinereading.structure.MachineReadingAnnotations._
import edu.stanford.nlp.process.{CoreLabelTokenFactory, PTBTokenizer}

import opennlp.tools.tokenize._

//import org.apache.lucene.analysis.{CharArraySet, StopAnalyzer}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

//import intoxicant.analytics.coreNlp._

import com.impanchao.crawlcorruption._


/**
 * Created by panc25 on 11/5/15.
 *
 * Experimenting how to access CouchDB from Spark.
 *
 * In the current experiment, the CouchDB database is loaded into Scala as a whole.
 * Then the Spark RDD is created for those articles through sc.parallelize().
 */


val couch = CouchDb("127.0.0.1", 5984)  // CouchDB client instance
// TODO: how to start CouchDB server from within Scala?
val dbName = "fbi-public-corruption-news"  // database name
// CrawledCorruptionArticle is my self-defined class
val typeMapping = TypeMapping(classOf[CrawledCorruptionArticle] -> "CrawledCorruptionArticle")
val db = couch.db(dbName, typeMapping) // instance of the DB API

/**
 * Get data from CouchDB database
 */
// If it is known that all documents in a database are of the same type,
// the following query returns all the documents in the database as a
// sequence where the full document contents are included.
val couchDocs = db.docs.getMany.queryIncludeDocs[CrawledCorruptionArticle].run  // CouchDocs object
couchDocs.getDocs  // Sequence of all articles as CrawledCorruptionArticle objects
val crawledCorruptionArticle = couchDocs.getDocs(0).doc  // the first article as a CrawledCorruptionArticle object
//crawledCorruptionArticle.content



////////////////////////////////////////////////////////////The following is ran once
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

// Create a query builder by querying the design document from the database
val allCrawledCorruptionArticlesViewQueryBuilder =
  db.query.view[String, CrawledCorruptionArticle](
      "tokenization-design", "allCrawledCorruptionArticle-view"
  ).get

val allCrawledCorruptionArticles = allCrawledCorruptionArticlesViewQueryBuilder.query.run
//allCrawledCorruptionArticles.rows(0).value.content  // content of the first article
//allCrawledCorruptionArticles.rows  // returns a sequence of all articles as CouchKeyVal objects

// It is also possible to execute a query without including the document content,
// which only returns meta-data. In this case, we don't need to specify the type
// parameter as no mapping is required since the document content is not retrieved.
val couchDocsWithoutContents = db.docs.getMany.query.run
//couchDocsWithoutContents.rows(0)  // meta-data for the first document

// TODO: Currently the CouchDB databases are saved in local file system.
// todo: If we save them in HDFS, will it help to access data from within Spark?

/**
 * For each document count the total number of words:
 *  1) tokenize the article
 *  2) count the number of tokens
 */
// Create SparkContext
val sparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
val sc = new SparkContext(sparkConf)

//val crawledCorruptionArticlesRDD = sc.parallelize(couchDocs.getDocs).map(couchDoc => couchDoc.doc)  // RDD of articles
val crawledCorruptionArticlesRDD =
  sc.parallelize(allCrawledCorruptionArticles.rows).map(
    couchKeyVal => couchKeyVal.value
  )  // RDD of articles, where each article is a CrawledCorruptionArticle object

//crawledCorruptionArticlesRDD.take(1)(0)

////////////////////////////////
// Tokenization using OpenNLP //
////////////////////////////////

// Construct OpenNLP tokenizer
def constructTokenizer(modelFileLocation: String): Option[Tokenizer] = {
  val modelIn: InputStream = new FileInputStream(modelFileLocation)

  try{
    val model: TokenizerModel = new TokenizerModel(modelIn)
    val tokenizer = new TokenizerME(model)
    return Some(tokenizer)
  } catch {
    case e: Exception => {
      e.printStackTrace()
      return None
    }
  } finally {
    if (modelIn != null){
      try {
        modelIn.close()
      } catch {
        case e: IOException => println("IO exception")
      }
    }
  }
}

val modelFileLocation = "scripts/textmining/en-token.bin"
val tokenizer = constructTokenizer(modelFileLocation).get
//tokenizer.tokenize("I am!! Pan, Chao")

def tokenizeArticle(article: CrawledCorruptionArticle, tokenizer: Tokenizer) = tokenizer.tokenize(article.content)

// Tokenize all articles
val tokenizedArticlesRDD = crawledCorruptionArticlesRDD.map(article => tokenizeArticle(article, tokenizer))
//tokenizedArticlesRDD.take(1)(0)


/////////////////////////////////////
// Tokenization using Stanford NLP //
// with stop word annotation       //
/////////////////////////////////////

/**
 * Tokenization and removing stop words.
 * OpenNLP doens't have built-in functionality for removing stop words.
 * Neither does Stanford NLP. However, there is a extension library, coreNlp,
 * which could identify stop words. More details can be found at:
 *
 *  https://github.com/jconwell/coreNlp
 *
 * So we try to use Stanford NLP and coreNlp for removing stop words in an
 * article.
 */

//// Sample article
//val articleString = crawledCorruptionArticlesRDD.take(1)(0).content

// Create StanfordCoreNLP pipeline
val props: Properties = new Properties()
props.put("annotators", "tokenize, ssplit, stopword")
props.setProperty("customAnnotatorClass.stopword", "intoxicant.analytics.coreNlp.StopwordAnnotator")
val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

//// Tokenize
//val annotationDoc: Annotation = new Annotation(articleString)
//pipeline.annotate(annotationDoc)
//val tokens = annotationDoc.get(classOf[CoreAnnotations.TokensAnnotation])  // List[CoreLabel]

// Lucene built-in stop words
val stopWords = StopAnalyzer.ENGLISH_STOP_WORDS_SET
//stopWords.getClass

//// Alternatively, one can use custom stop words
//val customStopWordList = "start,starts,period,periods,a,an,and,are,as,at,be,but,by,for,if,in,into,is,it,no,not,of,on,or,such,that,the,their,then,there,these,they,this,to,was,will,with"
//props.setProperty(StopwordAnnotator.STOPWORDS_LIST, customStopWordList)

/*
 scala.collection.JavaConversions._ implicitly converts Java list to Scala list,
 so we can call call the filter method.
 The following function filters out all non-stopwords from an article as a
 list of CoreLabel objects.
 */
def removeStopwords(tokens: java.util.List[CoreLabel]) =
  tokens.filter(token => !stopWords.contains(token.word().toLowerCase()))

/*
 Question: If I use the parameter type `List[CoreLabel]`, the function cannot be applied
           to `tokens` which is of Java.util.List type. This is so even if I added the
           package `import scala.collection.JavaConversions._`.
           To my understanding, `List[CoreLabel]` is Scala list by default. When we give
           the function a Java list, it should be implicitly converted to Scala list. But
           why it is not the case here?
*/

//removeStopwords(tokens)

// So now, I want a function which returns an article as a list of tokens without stop words.
def tokenizeAndRemoveStopwords(crawledCorruptionArticle: CrawledCorruptionArticle,
                               pipeline: StanfordCoreNLP) = {
  val articleString = crawledCorruptionArticle.content
  // First, tokenize
  val annotationArticle: Annotation = new Annotation(articleString)
  pipeline.annotate(annotationArticle)
  val articleTokens = annotationArticle.get(classOf[CoreAnnotations.TokensAnnotation])
  // Second, remove stopwords
  removeStopwords(articleTokens)
}

//tokenizeAndRemoveStopwords(crawledCorruptionArticlesRDD.take(2)(1), pipeline)

val crawledCorruptionArticlesAfterCleanTokenizationRDD =
  crawledCorruptionArticlesRDD.map(x => tokenizeAndRemoveStopwords(x, pipeline))

// Calculate word counts
val wordCountsRDD =
  crawledCorruptionArticlesAfterCleanTokenizationRDD.flatMap(x => x).map(x => (x.word(), 1)).reduceByKey(_+_)

// Sort word counts in descending order
val sortedWordCountsRDD = wordCountsRDD.sortBy(x => x._2, false)

// Take the top 2000 most frequent words
val topWords = sortedWordCountsRDD.take(2000)
topWords(1999)
topWords.map(x => x._1)

/*
TODO: There are a few possible modifications to perform better extraction of top word counts:
todo:   1) remove punctuations from the word counts;
todo:   2) covert all words to lower cases;
todo:   3) extract entities before performing word counts
 */

/////////////////////////////
// Named Entity Recognizer //
/////////////////////////////

/*
  We need to extract people's names, organizations' names and locations from
  a crawled article. The Stanford NER is used for this task. More specifically,
  the 3-class classifier is used now. More details about Stanford NER can be
  found at:

    http://nlp.stanford.edu/software/CRF-NER.shtml

  For a quick start, one can read the NERDemo.java in the downloaded package.
 */

import edu.stanford.nlp.ie.crf._

val serializedClassifier = "resources/StanfordNERclassifiers/english.all.3class.distsim.crf.ser.gz"
val classifier = CRFClassifier.getClassifier(serializedClassifier)

val simpleArticlesRDD = sc.parallelize(Array("I am a good man in China.", "You are also good, but in America."))
val classifiedSimpleArticlesRDD = simpleArticlesRDD.map(a => classifier.classifyToCharacterOffsets(a))
classifiedSimpleArticlesRDD.take(1)(0)

// prepare a sample article string
val articleString = crawledCorruptionArticlesRDD.take(1)(0).content
// classify each token in the article
val out = classifier.classify(articleString)  // tag (classify the items of) documents
// transform the classification output as a List[(token, annotation)] format
val annotatedWords = out.flatMap(
    sentence => sentence.map(
      word => (word.word(), word.get(classOf[CoreAnnotations.AnswerAnnotation]))
    )
  )
// get all LOCATION tokens
annotatedWords.filter(word => word._2 == "LOCATION").map(filteredWord => filteredWord._1)
// get all PERSON tokens
annotatedWords.filter(word => word._2 == "PERSON").map(filteredWord => filteredWord._1)
// get all ORGANIZATION tokens
annotatedWords.filter(word => word._2 == "ORGANIZATION").map(filteredWord => filteredWord._1)

/*
  There is a problem with the above results since each token has its own annotation.
  That is, the city, San Antonio, will be tokenized into two tokens and each will be
  annotated as LOCATION. I would rather like to get San Antonia as one annotated token.

  In order to extract return a complete Location, Person, or Organization entity. We can
  use `classifyToCharacterOffsets` method, which returns a list of triples for a given
  article as a string. Each triple contains the type of the entity, starting and ending
  index of the entity in the input article string.
 */

// annotating the article
val out_tripleList = classifier.classifyToCharacterOffsets(articleString)
// transformed the annotated results to a list of (entityString, typeOfEntity), e.g. (San Antonio,LOCATION)
val annotatedEntities = out_tripleList.map(
  triple => (articleString.substring(triple.second(), triple.third()), triple.first())
)
// get all locations
annotatedEntities.filter(x => x._2 == "LOCATION").map(x => x._1)

/*
  TODO: How to differentiate location types, i.e. State, City, County, etc.?
  todo: The above result contains a list of locations which treats different
  todo: type of locations equally.

  One solution to identify a state is to prepare a list of American states,
  and then check if the above result contains a state string.
 */

// get all persons
annotatedEntities.filter(x => x._2 == "PERSON").map(x => x._1).foreach(println)

/*
  TODO: In the above list of names, Jose A. Ytuarte and Ytuarte refer to the
  todo: same person. How to reduce the list of contain unique people?

  TODO: Not all people in the above list of names are crime-comitter. Some may
  todo: be law enforcement officials, some may be attorneys and lawyers, ...
  todo: How to classify those occupation types? Can we infer that from the article
  todo: context?

  Maybe we can google each person's name and get their occupation.

  If we can annotate the entity names for Person and Occupation, we may be able to
  use Stanford Relation Extractor to associate a person with his/her occupation via
  the linguistic path in the article.
 */

// get all organization
annotatedEntities.filter(x => x._2 == "ORGANIZATION").map(x => x._1).foreach(println)


/*
 The following codes perform NER via pipeline instead of
 constructing a CRFClassifier.

 A code example can be found at:

  http://www.informit.com/articles/article.aspx?p=2265404
 */
val props3 = new Properties()
props3.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner")
val pipeline3 = new StanfordCoreNLP(props3)

val annotation = new Annotation(articleString)
pipeline3.annotate(annotation)

val annotatedTokens = annotation.get(classOf[CoreAnnotations.TokensAnnotation])
val nerTags = annotatedTokens.map(t => t.get(classOf[CoreAnnotations.NamedEntityTagAnnotation]))
// TODO: A multi-term entity is separated. Fix it.



/////////////////////////////
// Try Relation Extraction //
/////////////////////////////

/*
  I found limited resources on how to use Stanford CoreNLP's
  relation extraction functionality. The introduction page gives
  some explanation for the methodology, which can be found at:

    http://nlp.stanford.edu/software/relationExtractor.shtml

  The only code example I could found is the main method in the
  source file "RelationExtractorAnnotator.java", which could be
  found at:

    https://github.com/stanfordnlp/CoreNLP/blob/master/src/edu/stanford/nlp/pipeline/RelationExtractorAnnotator.java
 */

// We can add NER and Relation Extraction into the same pipeline
val props2 = new Properties()
props2.setProperty("annotators", "tokenize,ssplit,pos,lemma,parse,ner")
val pipeline2 = new StanfordCoreNLP(props2)

//val sentenceString = "Barack Obama lives in America. Obama works for the Federal Goverment."
//val doc = new Annotation(sentenceString)
//pipeline2.annotate(doc)
//val r = new RelationExtractorAnnotator(props2)
//r.annotate(doc)
//
//val rls = doc.get(classOf[CoreAnnotations.SentencesAnnotation]). // for each sentence
//            flatMap(s => s.get(classOf[RelationMentionsAnnotation]))  // extrat relations
//
//rls.foreach(println)
//rls(0).toString()
//
//rls(0).getEntityMentionArgs()(0)

//val articleString = crawledCorruptionArticlesRDD.take(1).head.content
val articleAnnoataion = new Annotation(articleString)
pipeline2.annotate(articleAnnoataion)

val relationExtractionAnnotator = new RelationExtractorAnnotator(props2)
relationExtractionAnnotator.annotate(articleAnnoataion)  // annotate relation

val rls = articleAnnoataion.
            get(classOf[CoreAnnotations.SentencesAnnotation]).
              flatMap(s => s.get(classOf[RelationMentionsAnnotation]))

rls.length  // total number of relations found in an article
//rls(10).getEntityMentionArgs()(0)

// Remove "_NR" relation types
val filteredRls = rls.filter(rl => rl.getType != "_NR")
//filteredRls.length

val articleTokens = articleAnnoataion.get(classOf[CoreAnnotations.TokensAnnotation])

//filteredRls(0)

// Format the relation stirng as "entity 1 ( relation type ) entity 2", e.g. Hondo ( Located_In ) TX
val relationStrings = filteredRls.map(
    rl => (rl.getEntityMentionArgs.map(entity => entity.getExtentString), rl.getType)
  ).map(x => Array(x._1(0), "(", x._2, ")", x._1(1)).mkString(" "))

relationStrings.foreach(println)

/*
  TODO: It seems that Relation Extractor does not provide satisfactory results.
  todo: For exaple, we have the relation:
  todo:
  todo:   FBI ( Live_In ) Christopher Combs
 */


articleAnnoataion.get()