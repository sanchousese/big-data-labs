package com.cadlabs.spark.pagerank

case class Edge(from: String, to: String, weight: Int)

object ItemExtractor {
  def extractItem(line: String): List[Edge] = {
    val titlePattern = "<(title)>(.*)<\\/\\1>".r
    val title = titlePattern.findFirstMatchIn(line).map(_.group(2))

    val bodyPattern = "<(text).*>(.*)<\\/\\1>".r
    val body = bodyPattern.findFirstMatchIn(line).map(_.group(2))

    val referencePattern = "\\[{2}([^\\[]*)\\]{2}".r

    val maybeEdges = for {
      t <- title
      b <- body
      rc = referencePattern.findAllMatchIn(b)
        .map(m => m.group(1) -> 1).toList
        .groupBy(_._1)
        .map(i => i._1 -> i._2.map(_._2).sum)
        .map { case (ref, count) => Edge(t, ref, count) }
        .toList
    } yield rc

    maybeEdges.getOrElse(Nil)
  }
}
