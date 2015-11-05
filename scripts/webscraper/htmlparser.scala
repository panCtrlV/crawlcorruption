/**
 * Created by panc25 on 10/9/15.
 */

import java.io.File

import org.apache.commons.codec.binary.Base64
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, Document}
import org.jsoup.select.Elements


/*
Load a Document from a URL
 */
var url: String = "http://www.stat.purdue.edu/~panc"
var doc: Document = Jsoup.connect(url).get()
//println(doc)
doc.title()
val newsHeadlines: Elements = doc.select("#mp-itn b a")
println(newsHeadlines)


/*
Sometimes, a webpage requires login.
Load a Document from a URL with basic access authentication

Reference:
  http://stackoverflow.com/questions/7679916/jsoup-connection-with-basic-access-authentication
 */
var url = "http://www.stat.purdue.edu/~chuanhai/teaching/BigData2015/spp/index.html"
val username: String = "student"
val password: String = "computing"
val login: String = username + ":" + password
val base64login: String = new String(Base64.encodeBase64(login.getBytes()))
//var doc = Jsoup.connect(url).get()  // fail
var doc = Jsoup.connect(url).header("Authorization", "Basic " + base64login).get()


/*
Parse document from a string
 */
val html: String = "<html><head><title>First parse</title></head>" + "<body><p>Parsed HTML into a doc.</p></body></html>"
var doc = Jsoup.parse(html)  // parse a string as a DOM


/*
Parse a HTML document into DOM
 */
var input: File = new File("htmlFiles/fbi_news.html")
var doc = Jsoup.parse(input, "UTF-8")
//println(doc)
var content: Element = doc.getElementById("parent-fieldname-text")
content.attributes()  // get all attributes
content.text().length  // get text content, ** What We Need **
content.outerHtml()
content.html()
content.tag()

/*
Use selector to find elements in a document
 */
var content_p: Elements = content.select("p")
//println(content_p)
//content_p.getClass
//Set(content_p).foreach(println(_))  // convert to Scala set which can be iterated
content_p.text().reverseIterator.take(5).foreach(println(_))  // Get the combined text of all the matched elements.
content_p.toString().take(10)  // the result string retains tags

var links: Elements = doc.select("a[href]")  // tag 'a' (hyperlinks) with 'href' attribute
links.size()  // number of elements
//println(links)
links.first().attr("href")
links.select("a[class$=internal-link]")
links.select("[accesskey=2]")

links.attr("href")

var pngs: Elements = doc.select("img[src$=.png]")
pngs.size()





