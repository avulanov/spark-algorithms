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

import org.apache.log4j.{Logger, Level}
import org.apache.spark.algorithms.util.SparkTestContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.graphx.util.GraphGenerators
import org.scalatest.FunSuite
import org.apache.spark.graphx.lib.{PageRank => PageRankGraphX}

class PageRankSuite extends FunSuite with SparkTestContext {
  /**
    * Example from https://en.wikipedia.org/wiki/PageRank
    *
    */
//  test ("Wikipedia pagerank example") {
//    val trueRanks = Map("A" -> 3.3, "B" -> 38.4, "C" -> 34.3, "D" -> 3.9, "E" -> 8.1,
//      "F" -> 3.9, "G" -> 1.6, "H" -> 1.6, "I" -> 1.6, "J" -> 1.6, "K" -> 1.6)
//    val lines = sc.textFile("data/wiki.txt", 1)
//    val edges = lines.map{ s =>
//      val parts = s.split("\\s+")
//      (parts(0), parts(1))
//    }
//    val ranks = PageRank.run(edges, 50)
//    val output = ranks.collect()
//    val eps = 0.01
//    output.foreach { case (id, rank) => assert(trueRanks(id) / 100 - rank < eps)}
//  }

  test ("Generated graph test") {
    val numVertices = 1000
    val numIter = 100
    val graph = GraphGenerators.logNormalGraph(sc, numVertices, seed = 11L).cache()
    graph.edges.foreachPartition( x => {} )
    val edges = graph.edges.map( edge => (edge.srcId.toLong, edge.dstId.toLong)).cache()
    edges.count()
    var t = System.nanoTime()
    val ranks = PageRank.run(edges, numIter)
    t = System.nanoTime() - t
    println("RDD Pagerank t:" + t / 10e9 + " s")
    edges.unpersist()
    ranks.unpersist()
    var tGraphX = System.nanoTime()
    val ranksGraphX = PageRankGraphX.run(graph, numIter)
    tGraphX = System.nanoTime() - tGraphX
    println("GraphX Pagerank t:" + tGraphX / 10e9 + " s")
    graph.unpersist()
    ranksGraphX.unpersist()
  }

//    test ("Wikipedia pagerank example for GraphX") {
//      val trueRanks = Map(1L -> 3.3, 2L -> 38.4, 3L -> 34.3, 4L -> 3.9, 5L -> 8.1,
//        6L -> 3.9, 7L -> 1.6, 8L -> 1.6, 9L -> 1.6, 10L -> 1.6, 11L -> 1.6)
//      val lines = sc.textFile("data/wiki-long.txt", 1)
//      val edges = lines.map{ s =>
//        val parts = s.split("\\s+")
//        (parts(0).toLong, parts(1).toLong)
//      }
//      val numIter = 50
//      val vertices = edges.flatMap { case (a, b) => Seq(a, b) }.distinct().map(x => (x, true))
//      val graphEdges = edges.map{ case (a, b) => Edge(a, b, true)}
//      graphEdges.count()
//      val graph = Graph(vertices, graphEdges)
//      val ranksGraphX = PageRankGraphX.run(graph, numIter)
//      val outputGraphX = ranksGraphX.vertices.collectAsMap()
//      val sum = outputGraphX.values.sum
//      val eps = 0.01
//      outputGraphX.foreach { case (id, rank) => assert(rank / sum - trueRanks(id) < eps) }
//    }
}
