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

package org.apache.spark.mllib

import org.apache.spark.algorithms.util.SparkTestContext
import org.apache.spark.mllib.linalg.distributed.{GridPartitioner, BlockMatrix}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrix, SparseMatrix}
import org.scalatest.FunSuite

class BlockMultiplySuite extends FunSuite with SparkTestContext {

//  test ("A times x") {
//    val seq = Seq((0, 0, 1.0), (1, 1, 1.0))
//    val sm = SparseMatrix.fromCOO(2, 2, seq)
//    val seq0 = Seq((0, 0, 0.0))
//    val sm0 = SparseMatrix.fromCOO(2, 2, seq0)
//    val blocks: Seq[((Int, Int), Matrix)] = Seq(
//      ((0, 0), sm),
//      ((0, 1), sm0),
//      ((1, 0), sm0),
//      ((1, 1), sm)
//    )
//    val xBlocks: Seq[((Int, Int), Matrix)] = Seq(
//      ((0, 0), new DenseMatrix(2, 1, Array(2.0, 2.0))),
//      ((1, 0), new DenseMatrix(2, 1, Array(3.0, 3.0)))
//    )
//    val rdd = sc.parallelize(blocks, 2)
//    val xRdd = sc.parallelize(xBlocks, 2)
//    val A = new BlockMatrix(rdd, 2, 2)
//    val x = new BlockMatrix(xRdd, 2, 1)
//    val Ax = A.multiply(x)
//    println(Ax.toLocalMatrix())
//  }

  test ("Load edges into the sparse matrix") {
    val lines = sc.textFile("data/wiki-long2.txt")
    val edges = lines.map{ s =>
      val parts = s.split("\\s+")
      ((parts(0).toInt, parts(1).toInt), 1.0)
    }
    // TODO: needs GridPartitioner with Long indexes
    val numVertices = 11
    val blockSize = 6
    // TODO: automatic grid repartition
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
    println(blockMatrix.toBreeze())
    // TODO: need 1 vector
    val oneRdd = sc.parallelize(1 to hBlocks, hBlocks).mapPartitionsWithIndex[((Int, Int), Matrix)] {
      case (index, it) =>
        val i = index % hBlocks
        val j = 0
        val numElements =
          if ((i == hBlocks - 1) && odd) numVertices % blockSize else blockSize
        Iterator((((i, j)), DenseMatrix.ones(numElements, 1)))
    }
    // TODO: need elementwise multiplication
    gridMatrices.collect().foreach { case ((i, j), mat) => println(i + " " + j + " "); println(mat.toBreeze)}
    println("--------")
    oneRdd.collect().foreach { case ((i, j), mat) => println(i + " " + j + " "); println(mat.toBreeze)}
    println("--------")
    val blockOne = new BlockMatrix(oneRdd, blockSize, 1)
    println(blockOne.toBreeze())
    val Ax = blockMatrix.multiply(blockOne)
    println(Ax.toBreeze())
  }
//    val collected = gridEdges.collectPartitions()
//    val size = collected.size
//    var i = 0
//    while (i < size) {
//      println("P# " + i)
//      collected(i).foreach(x => println(x._1 + " " + x._2))
//      i += 1
//    }
//
//    val matCollected = gridMatrices.collectPartitions()
//    val s = matCollected.size
//    i = 0
//    while (i < s) {
//      val data = matCollected(i)(0)
//      println("P# " + data._1 + " " + data._2)
//      i += 1
//    }
//  }

}
