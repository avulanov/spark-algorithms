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

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.reflect.ClassTag

// TODO: Add slides
/*
 * Implements PageRank
 * as described in Mining Massive Datasets course
 * https://class.coursera.org/mmds-002/lecture
 *
 */
object PageRank {

  // TODO: make it usable through spark-submit
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("PageRank").setMaster("local")
    val iters = args(1).toInt
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0), 1)
    val edges = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }
    val ranks = run(edges, iters)
    val output = ranks.take(20).sortBy(-_._2)
    output.foreach { case (id, rank) => println(id + " has rank: " + rank) }
    ctx.stop()
  }

  // TODO: implement withe epsilon instead of max iterations
  def run[T](edges: RDD[(T, T)], iters: Int)(implicit m: ClassTag[T]): RDD[(T, Double)] = {
    val d = 0.85
    val vertices = edges.flatMap{ case(a, b) => Seq(a, b) }.distinct()
    val n = vertices.count()
    val defaultRank = 1.0 / n
    var ranks = vertices.map(v => (v, defaultRank))
    val outGoingSizes = edges.map( x => (x._1, 1)).reduceByKey(_ + _)
    val outgoingEdges = edges.join(outGoingSizes)
    // TODO: perform caching
    for (i <- 1 to iters) {
      val inboundRanks = outgoingEdges.join(ranks).map {
        case (start, ((end, startOutGoingSize), startRank)) =>
          (end, d * startRank / startOutGoingSize)
      }.reduceByKey(_ + _)
      val leakedRank = (1 - inboundRanks.values.sum) / n
      ranks = ranks.leftOuterJoin(inboundRanks).map {
        case (id, (_, option)) =>
          (id, option.getOrElse(0.0) + leakedRank)
      }
      ranks.count()
    }
    ranks
  }
}
