import java.io._

import opennlp.tools.namefind.{NameFinderME, TokenNameFinderModel}
import opennlp.tools.tokenize.{TokenizerME, Tokenizer, TokenizerModel}
import opennlp.tools.util.Span
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

/* Current working directory */
new File(".").getCanonicalPath()

/**
 * Prepare article text
 */

/*
From URL source
 */
//val url: String = "https://www.law.cornell.edu/wex/white-collar_crime"
//val doc: Document = Jsoup.connect(url).get()
//val article: String = doc.select("div[class^=field-item] > p").text()
//article.reverseIterator.take(10).foreach(print)

/*
From local html file
 */
val fileLocation: String = "temp/fbi_news.html"
var input: File = new File(fileLocation)
val doc: Document = Jsoup.parse(input, "UTF-8")
val article: String = doc.getElementById("parent-fieldname-text").text()

// save article to disk
val pw: PrintWriter = new PrintWriter(new File("temp/fbi_news.txt"))
pw.write(article)
pw.close()


/**
 * Use openNLP for tokenization.
 *
 * OpenNLP components have similar APIs. Normally, to execute a
 * task, one should provide a model and an input.
 *
 * A model is usually loaded by providing a FileInputStream with
 * a pre-trained model to a constructor of the model class.
 * OpenNLP provides different model classes for different NLP
 * tasks such as sentence detection, tokenization, entity finding, etc.
 *
 * After the model is loaded the tool itself can be instantiated.
 *
 * After the tool is instantiated, the processing task can be executed.
 * The input and the output formats are specific to the tool, but often
 * the output is an array of String, and the input is a String or an
 * array of String.
 *
 * Reference:
 *  https://opennlp.apache.org/documentation/1.6.0/manual/opennlp.html#opennlp
 *  https://opennlp.apache.org/documentation/1.6.0/manual/opennlp.html#tools.tokenizer
 */
val modelIn: InputStream = new FileInputStream("temp/en-token.bin")

try{
  val model: TokenizerModel = new TokenizerModel(modelIn)
} catch {
  case e: Exception => e.printStackTrace()
} finally {
  if (modelIn != null){
    try {
      modelIn.close()
    } catch {
      case e: IOException => println("IO exception")
    }
  }
}

/*
A Tokenizer for converting raw text into separated tokens.
It uses Maximum Entropy to make its decisions.

Reference:
  https://opennlp.apache.org/documentation/1.5.2-incubating/apidocs/opennlp-tools/opennlp/tools/tokenize/TokenizerME.html
 */
val tokenizer: Tokenizer = new TokenizerME(model)

val tokens: Array[String] = tokenizer.tokenize(article)
//tokens.length
//tokens(0)

val tokenSpans: Array[Span] = tokenizer.tokenizePos(article)
//tokenSpans.length
//tokenSpans(0)  // each Span contain the begin and end character offsets
//tokenSpans(1).getCoveredText(article)  // get the text for one span

/*
TokenizerME is able to output the probabilities for the detected tokens.
The getTokenProbabilities method must be called directly after one of the
tokenize methods was called.

To clarify, the tokenizer probabilities calculated the likelihood whether
a string of characters in this article is a token or not according to the
tokenizer model. The probabilities do not relate to how often a particular
token content has been seen.

Reference:
  http://stackoverflow.com/questions/28073147/finding-token-probabilies-in-a-text-in-nlp
  https://opennlp.apache.org/documentation/1.5.2-incubating/apidocs/opennlp-tools/opennlp/tools/tokenize/TokenizerME.html#getTokenProbabilities()
 */
val tokenizerME: TokenizerME = new TokenizerME(model)
val tokensME: Array[String] = tokenizerME.tokenize(article)
val tokenProbs: Array[Double] = tokenizerME.getTokenProbabilities()
//tokenProbs.length  // same length as tokes
//tokenProbs

/**
 * To find names in raw text the text must be segmented into tokens and sentences.
 *
 * After every document clearAdaptiveData must be called to clear the adaptive data
 * in the feature generators. Not calling clearAdaptiveData can lead to a sharp drop
 * in the detection rate after a few documents.
 */
val modelIn_findPeople: InputStream = new FileInputStream("temp/en-ner-person.bin")

try {
  val model_findPeople: TokenNameFinderModel = new TokenNameFinderModel(modelIn_findPeople)
  val nameFinder: NameFinderME = new NameFinderME(model_findPeople)
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

// The initialization is now finished and the Name Finder can be used.
val nameSpans: Array[Span] = nameFinder.find(tokens)
val nameSpanProbs: Array[Double] = nameFinder.probs(nameSpans)
nameFinder.clearAdaptiveData()

// Print the names
def printName(i: Int) = {
  println("Span: " + nameSpans(i).toString())
//  println("Covered text is: " + tokens(nameSpans(i).getStart()) + " " + tokens(nameSpans(i).getStart() + 1))
//  println("Covered text is: " + tokens(nameSpans(i).getStart()))
  println("Covered text is: " + tokens.slice(nameSpans(i).getStart(), nameSpans(i).getEnd()).mkString(" "))
  println("Probability is: " + nameSpanProbs(i) + "\n")
}

(0 to nameSpans.length - 1).foreach(printName)
