package com.cadlabs.spark.wordcount

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  val conf = new SparkConf().setAppName("Word Counter").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val inputPath = getClass.getResource("/textfile.txt").getPath
  val outputPath = "./spark_word_counts"

  def main(args: Array[String]): Unit = {
    val textFile = sc.textFile(inputPath)
    val result = textFile.flatMap(line => line.split("\\W"))
      .map(word => word -> 1)
      .groupByKey()
      .map(i => i._1 -> i._2.sum)
      .sortBy(_._2, ascending = false)


    val outputFile = new File(outputPath)
    outputFile.deleteOnExit()
    result.saveAsTextFile(outputFile.getPath)
  }
}
