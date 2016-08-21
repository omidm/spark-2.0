
import java.util.Random
import org.apache.spark.{SparkConf, SparkContext}


object MyKMeans {

  case class WeightedCenter(vec: Array[Double], weight: Double)

  // A psuedo random function.
  def prf(input: Long) : Double = {
    var x = input + 4698;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x);
    return x;
  }

  def spinWait(wait_us: Int) = {
    // println("***** SpinWait us: " + wait_us)
    var start = System.nanoTime()
    var stop  = System.nanoTime()
    var loop = 0
    while ((stop - start) < wait_us * 1000) {
      var x = 0;
      for (i <- 1 to 10) {
        x = x + i;
      }
      stop = System.nanoTime()
      loop = loop + 1
    }
    // println("****** looped: " + loop)
  }


  def vectorAdd(x: Array[Double], y: Array[Double]) : Array[Double] = {
    // return Array.tabulate(x.length){i => x(i) + y(i)};
    return (x zip y).map({case (a,b) => a + b});
  }

  def vectorScale(x:Array[Double], s: Double) : Array[Double] = {
    // return Array.tabulate(x.length){i => x(i) * s}
    return x.map(s*_);
  }

  def squaredDistance(x: Array[Double], y: Array[Double]): Double = {
    // var d : Double = 0
    // for (i <- 0 until x.length) {
    //   d += (x(i) - y(i)) * (x(i) - y(i))
    // }
    // return d
    return (x zip y).map({case (a,b) => (a - b) * (a - b)}).sum;
  }

  def closestPoint(p: Array[Double], centers: Array[Array[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  def processPartition(points: Iterator[Array[Double]], centers: Array[Array[Double]], wait_us: Int)
    : Iterator[Array[WeightedCenter]] = {
    var result = Array.tabulate(centers.length)(i => {
                                                        val vec = Array.tabulate(centers(i).length)(j => 0.0)
                                                        val weight = 0
                                                        WeightedCenter(vec, weight)
                                                      }
                                               )
    if (wait_us != 0) {
      spinWait(wait_us)
      return Iterator(result)
    }

    for (p <- points) {
      val idx = closestPoint(p, centers)
      var v = result(idx).vec
      var w = result(idx).weight
      result(idx) = WeightedCenter(vectorAdd(v, p), w + 1)
    }

    return Iterator(result)
  }

  def reduceWeightedCenters(x: Array[WeightedCenter], y: Array[WeightedCenter]) : Array[WeightedCenter] = {
    Array.tabulate(x.length)(i => {WeightedCenter(vectorAdd(x(i).vec, y(i).vec), x(i).weight + y(i).weight)})
  }

  def print_help() {
    println("Usage: pass 6 arguments")
    println("   <Int dimension>")
    println("   <Int cluster_num>")
    println("   <Int iteration_num>")
    println("   <Int partition_num>")
    println("   <Float sample_num in million>")
    println("   <Int spin_wait in micro seconds>")
  }

  def main(args: Array[String]) {

    var (kDimension          : Int,
         kClusterNum         : Int,
         kIterationNum       : Int,
         kPartitionNum       : Int,
         kSampleNumInMillion : Float,
         kSpinWait           : Int) =
    args.length match {
      case 6 => (args(0).toInt, args(1).toInt, args(2).toInt, args(3).toInt, args(4).toFloat, args(5).toInt)
      case default => print_help();
    }

    var kSampleNum          : Int = (kSampleNumInMillion.toFloat * 1000000).toInt
    var kSamplePerPartition : Int = (kSampleNum.toFloat / kPartitionNum.toFloat).toInt

    
    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)


    // Generate points.
    val input_seed = Array.tabulate(kPartitionNum)(x => x)
    val points = sc.parallelize(input_seed, kPartitionNum)
                   .flatMap(x => Array.tabulate(kSamplePerPartition)(y => x*kSamplePerPartition + y))
                   .map(x => Array.tabulate(kDimension)(y => prf(x*kDimension+y)))

    points.persist();

    // initialize centers to a random vector
    var rand = new Random(42)
    var centers: Array[Array[Double]] = Array.tabulate(kClusterNum)(i => Array.tabulate(kDimension)(j => rand.nextDouble))


    // Run k-means.
    for (i <- 1 to kIterationNum) {
      val newC = points.mapPartitions( ps => processPartition(ps, centers, kSpinWait))
                       .reduce( (wc1, wc2) => reduceWeightedCenters(wc1, wc2))

      for (idx <- 0 until centers.length) {
        centers(idx) = vectorScale(newC(idx).vec, (1.0 / newC(idx).weight))
      }
    }

    for (i <- 0 to kClusterNum - 1) {
        println("**** Centroid "  + i + ": ")
      for (j <- 0 to kDimension - 1) {
        println("  ** c[" + j + "] = "  + centers(i)(j))
      }
    }

    sc.stop()
  }
}
