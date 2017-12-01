package com.cadlabs.spark.pagerank

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

object PageRankProgram {
  val conf = new SparkConf().setAppName("Page Rank").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val inputPath = getClass.getResource("/wiki-micro.txt").getPath
  val outputPath = "./spark_page_rank"

  def main(args: Array[String]): Unit = {
    val textFile = sc.textFile(inputPath)
    val edges = textFile.flatMap(line => ItemExtractor.extractItem(line))

    val iterations = 10
    var pageRank = edges.flatMap(e => List(e.to, e.from)).distinct().map(s => s -> 1.0)

    for (_ <- 1 to iterations) {
      val pageMap = edges.map(e => e.to -> e).join(pageRank).groupBy(_._1).map {
        case (to, info) =>
          val multiplier = info.map { case (_, (Edge(_, _, w), pr)) => pr  / w }.sum
          to -> (0.15 + 0.85 * multiplier)
      }

      pageRank = pageMap
    }

    val result = pageRank.sortBy(_._2, ascending = false)

    val outputFile = new File(outputPath)
    outputFile.deleteOnExit()
    result.saveAsTextFile(outputFile.getPath)
  }
}
