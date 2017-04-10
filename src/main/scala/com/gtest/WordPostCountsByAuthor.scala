package com.gtest

/**
  * Created by snehal.mistry on 4/5/17.
  */


class WordPostCountsByAuthor(var wordsMap :Array[(String, Array[Int])]) extends Serializable  {
  def combine(other: WordPostCountsByAuthor): WordPostCountsByAuthor = {
    //For now, it looks like there would be no conflict in keys for blogs data due to one xml per author, but
    // for other kind of data if required add merge logic to combine key results.
    wordsMap = wordsMap ++ other.wordsMap
    this
  }
}




