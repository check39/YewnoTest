package com.gtest

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.util.StatCounter

import scala.util.matching.Regex

class WordSimilarityWithG2Test(sc:SparkContext, path: String) {
  private var termsToDocs:RDD[(String, WordPostCountsByAuthor)] = _
  buildFromBlogs()
  private var totalPosts = 0

  def buildFromBlogs(): Unit ={
    termsToDocs = parseBlogs()
    termsToDocs.cache()
    totalPosts = getAllPostsWithWord("##ALL##").size
  }


  /* TODO: Add method to add new blogs which would append termsToDocs document
  // For that all we need to do is parse new blogs and merge new results with termsToDocs
  //Since WordPostCountsByAuthor already has merge method, it will merge new values as it added.
  def addNewBlogs(): Unit = {

  }
  */

  object Helper extends Serializable {
    //Since all posts of authors are already organized in one xml,
    // we can take an advantage of it and process for one author in one shot.
    // to avoid heavy shuffling operation (groupBy author).
    //Expected output (word, (author: doc_indexes {0, 5, 10})
    def parseAndGroupWordsByAuthor(fileName: String, xml:String): Map[String, WordPostCountsByAuthor] = {
      //Extract authorInfo from fileName
      val pattern = ".+/(.+)\\..+".r
      val pattern(author) = fileName

      val lines = xml.split("\\r?\\n")
      val postsWithIndex = lines.filter(x => !( (x contains ">") || (x.trim().length == 0))).zipWithIndex

      //Also append ##ALL## to make sure we can get all posts later with word "##ALL##"
      postsWithIndex
        .flatMap{case (text, docIndex)=> plainTextToTerms(text + " ##ALL##", docIndex)}
        .groupBy(x=>x._1)
        .mapValues(_.map(_._2))
        .mapValues(x => new WordPostCountsByAuthor(Array((author,x))) )
    }

    def plainTextToTerms(text: String, docIndex: Int): Array[(String, Int)] = {
      val stopWords = Set("you", "to", "a", "if", "and", "or", "is", "do")
      val terms = text.split("\\s+")
        .map( _.replaceAll("""[\p{Punct}]""", "").trim().toLowerCase)
        .filter(x=> (x.length() > 1) &&  (x.matches("^[a-zA-Z]*$") && !stopWords.contains(x) ))
      val uniqueTerms = terms.distinct
      uniqueTerms.map((_,docIndex))
    }

  }
  //Parse blogs
  //Parse and split posts into text and group by words
  def parseBlogs(): RDD[(String, WordPostCountsByAuthor)] = {
    sc.wholeTextFiles(path)
      .sample(false, 0.05)
      .flatMap {case (fileName, text) =>
        Helper.parseAndGroupWordsByAuthor(fileName, text).toList
      }.groupBy(x=>x._1)
      .mapValues(_.map(_._2).reduce((a,b)=> a.combine(b)))
  }

  def getAllPostsWithWord(word: String): Set[String] = {
    val wordsmap = termsToDocs.lookup(word).toList(0).wordsMap
    wordsmap.flatMap{ case (author, posts) => posts.map((post)=> author + "_" + post)}.toSet
  }

  def findSimilarity(x: String, y:String): Unit = {
    //Since there would be only one value for key, take first value from lookup
    val xPosts = getAllPostsWithWord(x)
    val yPosts = getAllPostsWithWord(y)

    /*
      For given words x and y
      Create contingency table using matrix as follows:
                  yes-y    Absent-y
       yes-x      X         X
       Absent-x   X         X

       We will represent yes-x, Asbent-x as x and nx respectively.
     */

    val xy = xPosts.intersect(yPosts).size
    val nx = xPosts.diff(yPosts).size
    val ny = yPosts.diff(xPosts).size
    val nxny = totalPosts - (xy + nx + ny)


    val dm: Matrix = Matrices.dense(2, 2, Array(xy, nx, ny, nxny))
    val testResult: G2TestResult = G2Test.g2TestMatrix(dm)
    return testResult
  }



}


