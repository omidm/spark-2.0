
import java.util.Random
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.classification.LogisticRegression


object MyLogisticRegression {

  // A psuedo random function.
  def prf(input: Long) : Double = {
    var x = input + 4698;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x);
    return x;
  }


  def print_help() {
    println("Usage: pass 4 arguments")
    println("   <Int dimension>")
    println("   <Int iteration_num>")
    println("   <Int partition_num>")
    println("   <Float sample_num in million>")
  }

  def main(args: Array[String]) {

    var (kDimension          : Int,
         kIterationNum       : Int,
         kPartitionNum       : Int,
         kSampleNumInMillion : Float) =
    args.length match {
      case 4 => (args(0).toInt, args(1).toInt, args(2).toInt, args(3).toFloat)
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
                                    val label = if (x % 2 == 0) 0.0 else 1.0
                                    val vec = Vectors.dense(label, 1.1, 0.1)
                                    // val vec = Array.tabulate(kDimension)(y => prf(x*kDimension+y))
                                    (label, vec)
                                  }
                            )
    rdd_samples.persist();

    // translate rdd in to dataframes
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val input_samples = rdd_samples.toDF("label", "features");

    // train the model
    val lr = new LogisticRegression()
    lr.setMaxIter(kIterationNum).setRegParam(0.01)
    val model = lr.fit(input_samples)


    // for (i <- 0 to kDimension - 1) {
    //   println("***** Final w[" + i + "] = "  + w(i))
    // }

    sc.stop()
  }
}
