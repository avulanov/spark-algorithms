package org.apache.spark.mllib

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, GridPartitioner}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrix, SparseMatrix}
import org.apache.spark.rdd.RDD

object BlockOps {

  def fromCOO(edges: RDD[((Int, Int), Double)], numVertices: Int, blockSize: Int): BlockMatrix = {
    val odd = numVertices % blockSize != 0
    // TODO: everything is squared
    val hBlocks = math.ceil(numVertices * 1.0 / blockSize).toInt
    val gridEdges = edges.partitionBy(
      new GridPartitioner(numVertices, numVertices, blockSize, blockSize))
    val gridMatrices = gridEdges.mapPartitionsWithIndex[((Int, Int), Matrix)] {
      case (index, it) =>
        val i = index / hBlocks
        val j = index % hBlocks
        val rows =
          if ((i == hBlocks - 1) && odd) numVertices % blockSize else blockSize
        val columns =
          if ((j == hBlocks - 1) && odd) numVertices % blockSize else blockSize
        val seq = it.map(x => (x._1._1 % blockSize, x._1._2 % blockSize, x._2)).toIterable
        Iterator(((i, j), SparseMatrix.fromCOO(rows, columns, seq)))
    }
    // TODO: check that matrices form correct blocks (by dimensions)
    val blockMatrix = new BlockMatrix(gridMatrices, blockSize, blockSize)
    blockMatrix
  }

  def one(numVertices: Int, blockSize: Int, sc: SparkContext): BlockMatrix = {
    val odd = numVertices % blockSize != 0
    val hBlocks = math.ceil(numVertices * 1.0 / blockSize).toInt
    val oneRdd = sc.parallelize(1 to hBlocks, hBlocks).mapPartitionsWithIndex[((Int, Int), Matrix)] {
      case (index, it) =>
        val i = index % hBlocks
        val j = 0
        val numElements =
          if ((i == hBlocks - 1) && odd) numVertices % blockSize else blockSize
        Iterator((((i, j)), DenseMatrix.ones(numElements, 1)))
    }
    new BlockMatrix(oneRdd, blockSize, 1)
  }
}
