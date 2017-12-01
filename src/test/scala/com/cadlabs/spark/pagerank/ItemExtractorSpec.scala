package com.cadlabs.spark.pagerank

import org.scalatest.{FunSpec, Matchers}


class ItemExtractorSpec extends FunSpec with Matchers {
  describe("ItemExtractor") {
    it("should extract Item") {
      val string = "<title>Keroberos(Cardcaptor Sakura)</title>     <id>4190875</id>     <revision>       <id>47434837</id>       <timestamp>2006-04-07T17:55:16Z</timestamp>       <contributor>         <username>Eskimbot</username>         <id>477460</id>       </contributor>       <minor />       <comment>Robot: Fixing double redirect</comment>       <text xml:space=\"preserve\">#REDIRECT [[Cerberus (Cardcaptor Sakura)]] [[Cerberus (Cardcaptor Sakura)]] [[test]]</text>     </revision>"
      val maybeItem = ItemExtractor.extractItem(string)
      maybeItem should contain theSameElementsAs List(Edge("Keroberos(Cardcaptor Sakura)", "Cerberus (Cardcaptor Sakura)", 2), Edge("Keroberos(Cardcaptor Sakura)", "test", 1))
    }
  }
}
