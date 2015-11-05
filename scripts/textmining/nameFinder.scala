import java.io.{IOException, FileInputStream, InputStream, File}

import opennlp.tools.namefind.{NameFinderME, TokenNameFinderModel}
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import opennlp.tools.util.Span
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

/**
 * Prepare article from a local html file
 */
val fileLocation: String = "temp/fbi_news.html"
val input: File = new File(fileLocation)
val doc: Document = Jsoup.parse(input, "UTF-8")
val article: String = doc.getElementById("parent-fieldname-text").text()
//article

/**
 * Since a name finder requires the input to be tokenized or
 * sentence separated. We first perform sentence detection.
 */

/*
First we need to build the model
 */
val modelIn_sentenceDetector: InputStream = new FileInputStream("temp/en-sent.bin")
try {
  val sentenceModel: SentenceModel = new SentenceModel(modelIn_sentenceDetector)
} catch {
  case e: Exception => e.printStackTrace()
} finally {
  if (modelIn_sentenceDetector != null){
    try {
      modelIn_sentenceDetector.close()
    } catch {
      case e: IOException => println("IO exception")
    }
  }
}

val sentenceDetector: SentenceDetectorME = new SentenceDetectorME(sentenceModel)

/*
Then, we perform sentence detection on our article.
 */
val sentences: Array[String] = sentenceDetector.sentDetect(article)
//sentences(1)
//sentences.length

// Span of the sentences in the input article
val sentenceSpans: Array[Span] = sentenceDetector.sentPosDetect(article)


/**
 * Now having the article separated into sentences.
 * We can perform name entity detection.
 *
 * As usual, we build a name finder first, then feed it with
 * the sentences.
 */

/*
Build the model
 */
val modelIn_findPeople: InputStream = new FileInputStream("temp/en-ner-person.bin")

try {
  val model_findPeople: TokenNameFinderModel = new TokenNameFinderModel(modelIn_findPeople)
} catch {
  case e: Exception => e.printStackTrace()
} finally {
  if (modelIn_findPeople != null) {
    try {
      modelIn_findPeople.close()
    } catch {
      case e: IOException => println("IO exception")
    }
  }
}

val nameFinder: NameFinderME = new NameFinderME(model_findPeople)

/* TODO
Then, we perform name entity recognition,
sentence by sentence.
 */


