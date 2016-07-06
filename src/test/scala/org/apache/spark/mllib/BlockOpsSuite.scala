package org.apache.spark.mllib

import org.apache.spark.algorithms.util.SparkTestContext
import org.apache.spark.graphx.util.GraphGenerators
import org.scalatest.FunSuite

class BlockOpsSuite extends FunSuite with SparkTestContext {

  test ("load") {
    val numVertices = 1000
    val blockSize = 500
    val graph = GraphGenerators.logNormalGraph(sc, numVertices, seed = 11L)
    val edges = graph.edges.map( e => ((e.srcId.toInt, e.dstId.toInt), 1.0))
    val A = BlockOps.fromCOO(edges, numVertices, blockSize).cache()
    A.blocks.count
  }

  test ("one") {
    val numVertices = 1000
    val blockSize = 500
    val ones = BlockOps.one(numVertices, blockSize, sc)
    ones.blocks.count
  }

  test ("multiply") {
    val numVertices = 1000
    val blockSize = 500
    val graph = GraphGenerators.logNormalGraph(sc, numVertices, seed = 11L)
    val edges = graph.edges.map( e => ((e.srcId.toInt, e.dstId.toInt), 1.0))
    val A = BlockOps.fromCOO(edges, numVertices, blockSize).cache()
    val x = BlockOps.one(numVertices, blockSize, sc)
    val Ax = A.multiply(x)
    Ax.blocks.count
  }

}
