
import java.util.Random
import org.apache.spark.{SparkConf, SparkContext}


object LogisticRegression {

  case class Sample(vec: Array[Double], label: Double)

  // A psuedo random function.
  def prf(input: Long) : Double = {
    var x = input + 4698;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x);
    return x;
  }

  def vector_add(x: Seq[Double], y: Seq[Double]) : Seq[Double] = {
    return (x zip y).map({case (a,b) => a + b});
  }

  def vector_dot(x: Seq[Double], y: Seq[Double]) : Double = {
    return (x zip y).map({case (a,b) => a * b}).sum;
  }

  def vector_mul(x: Double, y:Seq[Double]) : Seq[Double] = {
    return y.map(x*_);
  }

  def spin_wait(x: org.apache.spark.sql.Row, wait_us: Int, dummy_w: Seq[Double]) : org.apache.spark.sql.Row = {
    if (!MASTER_RUNNING_CODE) {
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
    return x;
  }

  def print_help() {
    println("Usage: pass 5 arguments")
    println("   <Int dimension>")
    println("   <Int iteration_num>")
    println("   <Int partition_num>")
    println("   <Float sample_num in million>")
    println("   <Int spin_wait in micro seconds>")
  }

  // The following global variable is used to differentiate the reduction through spin_wait between
  // master and slaves. The global variable, set within the main function of the driver will be seen
  // only by master and not the slaves! -omidm
  var MASTER_RUNNING_CODE = false 

  def main(args: Array[String]) {

    MASTER_RUNNING_CODE = true

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
                                    val label = if (x % 2 == 0) -1 else 1
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


    // initialize w to a random vector
    var rand = new Random(42)
    var w: Seq[Double] = Array.tabulate(kDimension)(i => rand.nextDouble)


    // Run logistic regression.
    for (i <- 1 to kIterationNum) {
      if (kSpinWait == 0) {
        val gradient = input_samples
          .map(p => {
                      val vec = p(0).asInstanceOf[Seq[Double]]
                      val label = p(1).asInstanceOf[Double]
                      val factor =  (1 / (1 + math.exp(label * (vector_dot(w, vec)))) - 1) * label;
                      vector_mul(factor, p(0).asInstanceOf[Seq[Double]])
                    }
              )
          .reduce(vector_add(_,_))
        w = vector_add(w, gradient)
      } else {
        val gradient = input_samples
          .sample(false, 2/kSamplePerPartition.toFloat, 0)
          .reduce((x, y) => spin_wait(x, kSpinWait, w))(0).asInstanceOf[Seq[Double]]
        w = vector_add(w, gradient)
      }
    }

    // Uncomment to check the number of samples picked in case of spin wait -omidm
    // val count = input_samples.sample(false, 2/kSamplePerPartition.toFloat, 0).count()
    // println("***** Sample Count: " + count + " kSamplePerPartition: " + kSamplePerPartition)

    for (i <- 0 to kDimension - 1) {
      println("***** Final w[" + i + "] = "  + w(i))
    }

    sc.stop()
  }
}
