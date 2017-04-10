package com.gtest

import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class G2TestResult(pValue: Double, statistic: Double) {
  override def toString() = s"pValue: $pValue Statistic: $statistic"
}

object G2Test {

  def test(textFile: RDD[String]): Unit = {
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
  }

  def g2TestMatrix(counts: Matrix): G2TestResult = {
    val numRows = counts.numRows
    val numCols = counts.numCols

    // get row and column sums
    val colSums = new Array[Double](numCols)
    val rowSums = new Array[Double](numRows)
    val colMajorArr = counts.toArray
    val colMajorArrLen = colMajorArr.length

    var i = 0
    while (i < colMajorArrLen) {
      val elem = colMajorArr(i)
      if (elem < 0.0) {
        throw new IllegalArgumentException("Contingency table cannot contain negative entries.")
      }
      colSums(i / numRows) += elem
      rowSums(i % numRows) += elem
      i += 1
    }
    val total = colSums.sum

    // second pass to collect statistic
    var statistic = 0.0
    var j = 0
    while (j < colMajorArrLen) {
      val col = j / numRows
      val colSum = colSums(col)
      if (colSum == 0.0) {
        throw new IllegalArgumentException("Chi-squared statistic undefined for input matrix due to"
          + s"0 sum in column [$col].")
      }
      val row = j % numRows
      val rowSum = rowSums(row)
      if (rowSum == 0.0) {
        throw new IllegalArgumentException("Chi-squared statistic undefined for input matrix due to"
          + s"0 sum in row [$row].")
      }
      val expected = colSum * rowSum / total
      statistic += (colMajorArr(j) * math.log(  colMajorArr(j) / expected))
      j += 1
    }

    val df = (numCols - 1) * (numRows - 1)
    if (df == 0) {
      // 1 column or 1 row. Constant distribution is independent of anything.
      // pValue = 1.0 and statistic = 0.0 in this case.
      new G2TestResult(1.0, 0.0)
    } else {
      val pValue = 1.0 - new ChiSquaredDistribution(df).cumulativeProbability(statistic)
      new G2TestResult(pValue, statistic)
    }

  }

}
