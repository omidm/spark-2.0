
import java.util.Random
import org.apache.spark.{SparkConf, SparkContext}


object MyLogisticRegression {

  case class Sample(vec: Array[Double], label: Double)

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


  def vectorAdd(x: Seq[Double], y: Seq[Double]) : Seq[Double] = {
    // return Array.tabulate(x.length){i => x(i) + y(i)};
    return (x zip y).map({case (a,b) => a + b});
  }

  def vectorScale(x: Seq[Double], s: Double) : Seq[Double] = {
    // return Array.tabulate(x.length){i => x(i) * s}
    return x.map(s*_);
  }


  def vectorDot(x: Seq[Double], y: Seq[Double]) : Double = {
    // var d : Double = 0
    // for (i <- 0 until x.length) {
    //   d += x(i) * y(i)
    // }
    // return d
    return (x zip y).map({case (a,b) => a * b}).sum;
  }


  def processPartition(samples: Iterator[org.apache.spark.sql.Row], weight: Seq[Double], wait_us: Int)
    : Iterator[Seq[Double]] = {

    var local_gradient: Seq[Double] = Array.tabulate(weight.length)(i => 0.0)

    if (wait_us != 0) {
      spinWait(wait_us)
      return Iterator(local_gradient)
    }

    for (s <- samples) {
      val vec = s(0).asInstanceOf[Seq[Double]]
      val label = s(1).asInstanceOf[Double]
      val factor =  (1 / (1 + math.exp(label * (vectorDot(weight, vec)))) - 1) * label;
      val scaled = vectorScale(vec, factor)
      local_gradient = vectorAdd(scaled, vec)
    }

    return Iterator(local_gradient)
  }


  def print_help() {
    println("Usage: pass 5 arguments")
    println("   <Int dimension>")
    println("   <Int iteration_num>")
    println("   <Int partition_num>")
    println("   <Float sample_num in million>")
    println("   <Int spin_wait in micro seconds>")
  }


  def main(args: Array[String]) {

    var (kDimension          : Int,
         kIterationNum       : Int,
         kPartitionNum       : Int,
         kSampleNumInMillion : Float,
         kSpinWait           : Int) =
    args.length match {
      case 5 => (args(0).toInt, args(1).toInt, args(2).toInt, args(3).toFloat, args(4).toInt)
      case default => print_help();
    }

    var kSampleNum          : Int = (kSampleNumInMillion.toFloat * 1000000).toInt
    var kSamplePerPartition : Int = (kSampleNum.toFloat / kPartitionNum.toFloat).toInt

    
    val conf = new SparkConf().setAppName("LogisticRegression")
    val sc = new SparkContext(conf)


    // Generate training data.
    val input_seed = Array.tabulate(kPartitionNum)(x => x)
    val rdd_samples = sc.parallelize(input_seed, kPartitionNum)
                        .flatMap(x => Array.tabulate(kSamplePerPartition)(y => x*kSamplePerPartition + y))
                        .map(x => {
                                    val label = if (x % 2 == 0) -1.0 else 1.0
                                    val vec = Array.tabulate(kDimension)(y => prf(x*kDimension+y))
                                    Sample(vec, label)
                                  }
                            )
    rdd_samples.persist();

    // translate rdd in to dataframes
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val input_samples = rdd_samples.toDF();


    // initialize weight to a random vector
    var rand = new Random(42)
    var weight: Seq[Double] = Array.tabulate(kDimension)(i => rand.nextDouble)


    // Run logistic regression.
    for (i <- 1 to kIterationNum) {
      val gradient = input_samples
        .mapPartitions(samples => processPartition(samples, weight, kSpinWait))
        .reduce(vectorAdd(_,_))
      weight = vectorAdd(weight, gradient)
    }

    for (i <- 0 to kDimension - 1) {
      println("***** Final weight[" + i + "] = "  + weight(i))
    }

    sc.stop()
  }
}
