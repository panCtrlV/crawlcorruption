/**
 * Created by panc25 on 11/3/15.
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util._


// Initialize Spark
val conf = new SparkConf().setAppName("multiclasslogistic").setMaster("local[2]")
val sc = new SparkContext(conf)

/**
 * Data should be created as an RDD.
 * While creating data, one manually specifies the number of batches one desire.
 * Then one simulate data for each batch which are save as an RDD. Each RDD
 * partition can contain multiple batches of data points.
 *
 * First we simulate for the covariates, then generate response.
 */
val rng = new Random()

val k =3  // number of response categories
val p = 5  // number of covariates
val beta = (1 to k-1).map(_ => (1 to p).map(_ => rng.nextGaussian()).toArray).toArray
val broadcastBeta = sc.broadcast(beta)

val batchSize = 1000  // sample size for one batch
var batchIndices = sc.parallelize(1 to 10)  // number of batches

// Simulate one sample
// By using .nextXXX method, the generator's state is updated each time ?
def simulateDataPoint(rng: Random, p: Int) = (1 to p).map(_ => rng.nextGaussian()).toArray

// Simulate samples for a batch (i.e. one RDD partition)
def simulateDataForBatch(rng: Random, batchIndex: Int, p: Int, batchSize: Int) =
  (1 to batchSize).map(_ => simulateDataPoint(rng, p))

val simulatedData = batchIndices.flatMap(i => simulateDataForBatch(rng, i, p, batchSize))
//simulatedData.collect().length
//simulatedData.take(10).map(x => x.mkString("\t")).foreach(println)
// TODO: pretty print each array by formatting the number of digits after the decimal

val indexedData = simulatedData.zipWithIndex().map{_.swap}.sortByKey()
val column1 = indexedData.map(row => row._2(0))
column1.collect()
indexedData.first()
indexedData.count()
indexedData.filter(x => x._1 == 3).collect()


/**
 * Matrix class
 *
 * @param input
 */
case class MatrixRDD(input: RDD[Array[Double]]) extends Serializable {
  val value = input.zipWithIndex().map{_.swap}.sortByKey()
  val shape = (value.count(), value.lookup(1).head.length)

  // Access the value at `row`-th row and `col`-th column,
  // where row and column start from 0.
  def apply(row: Int, col: Int) = value.lookup(row).head(col)

  def getRow(row: Int) = value.lookup(row).head

  def getCol(col: Int) = value.map(row => row._2(col))

  def getRows(rows: Array[Int]) = rows.map(ind => getRow(ind))

  def getCols(cols: Array[Int]) = cols.map(ind => getCol(ind))

  def transpose() = ???

  // Matrix multiplication
  def *(other: MatrixRDD) = {
    // TODO: check if two matrices are conformable
    ???
  }
}

val dataMatrix = new MatrixRDD(simulatedData)
dataMatrix(0, 0)
dataMatrix.shape
dataMatrix.getRow(1)
dataMatrix.getCol(0).collect()
dataMatrix.getRows(Array(0, 2, 4))
dataMatrix.getCols(Array(0, 2))

