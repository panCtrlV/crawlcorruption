package com.impanchao.crawlcorruption


case class CrawledCorruptionArticle(fetchTime: String,
                                    time: String,
                                    title: String,
                                    content: String,
                                    label: String = "corruption") {
  /*
  By default Jackson will serialize via get methods.
  The following get methods are added so that the object
  can be serialized by Jackson.

  Details can be found at:
    http://stackoverflow.com/questions/8038718/serializing-generic-java-object-to-json-using-jackson
   */
  def getFetchTime = fetchTime
  def getTime = time
  def getTitle = title
  def getContent = content
  def getLabel = label
}