/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.algorithms

import org.apache.spark.algorithms.util.SparkTestContext
import org.apache.spark.graphx.util.GraphGenerators
import org.scalatest.FunSuite

class PageRankSuite extends FunSuite with SparkTestContext {
  /**
    * Example from https://en.wikipedia.org/wiki/PageRank
    *
    */
  test ("Wikipedia pagerank example") {
    val trueRanks = Map("A" -> 3.3, "B" -> 38.4, "C" -> 34.3, "D" -> 3.9, "E" -> 8.1,
      "F" -> 3.9, "G" -> 1.6, "H" -> 1.6, "I" -> 1.6, "J" -> 1.6, "K" -> 1.6)
    val lines = sc.textFile("data/pagerank.txt", 1)
    val edges = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }
    val ranks = PageRank.run(edges, 50)
    val output = ranks.collect()
    val eps = 0.01
    output.foreach { case (id, rank) => assert(trueRanks(id) / 100 - rank < eps)}
  }

  test ("Generated graph test") {
    val numVertices = 10
    val graph = GraphGenerators.logNormalGraph(sc, numVertices)
    val edges = graph.edges.map( edge => (edge.srcId.toLong, edge.dstId.toLong))
    val ranks = PageRank.run(edges, 50)
    ranks.take(20).foreach{ case (id, rank) => println(id + " has rank: " + rank) }
  }
}
