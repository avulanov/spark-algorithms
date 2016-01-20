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

import org.apache.spark.{SparkContext, SparkConf}

object PageRank {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }

    val d = 0.85
    val sparkConf = new SparkConf().setAppName("PageRank").setMaster("local")
    val iters = if (args.length > 1) args(1).toInt else 10
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0), 1)
    val edges = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }
    val vertices = edges.flatMap { case(a, b) => Array(a, b) }.distinct()
    val n = vertices.count()
    val defaultRank = 1.0 / n
    var ranks = vertices.map(v => (v, defaultRank))
    val outGoingSizes = edges.map( x => (x._1, 1)).reduceByKey(_ + _)
    val outgoingEdges = edges.join(outGoingSizes)
    for (i <- 1 to iters) {
      val inboundRanks = outgoingEdges.join(ranks).map {
        case (start, ((end, startOutGoingSize), startRank)) =>
          (end, d * startRank / startOutGoingSize)
      }.reduceByKey(_ + _)
      val leakedRank = (1 - inboundRanks.values.sum) / n
      ranks = ranks.leftOuterJoin(inboundRanks).map { case (id, (_, option)) => (id, option.getOrElse(0.0) + leakedRank)}
      ranks.count()
    }
    val output = ranks.sortByKey().collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2))
    ctx.stop()
  }
}
