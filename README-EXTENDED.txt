
---------------------------------------------
Author: Omid Mashayekhi <omidm@stanford.edu>
---------------------------------------------

In this file I will have some information about the improvements in Spark 2.0,
examples on How to use DataFrames and Spark MLlib. Some of the code here could
directly run in the Spark interactive shell. There are extra examples,
documents and information in the "extended/" and specifically "extended/docs/"
folders.

For further information on building and launching Spark clusters refer to the
README-EXTENDED.txt under spark-1.6 folder, where almost all the directives are
applicable to Spark 2.0 as well. I highly recommend going through that file,
first.  For comprehensive documentation refer to:
http://spark.apache.org/documentation.html (Spark 2.0.0)


-------------------------------------------------------------------------------
Spark 2.0 improvements
-------------------------------------------------------------------------------

Here I summarize the changes in Spark 2.0. I refer to documents from
Databricks; there are copies of the documents in the "extended/docs/" folder as
well. Please refer to: extended/docs/[spark-2.0, spark-as-compiler, tungsten,
catalyst, whole-stage-code].html

A. Technical Preview of Apache Spark 2.0 Now on Databricks
Easier, Faster, and Smarter

https://databricks.com/blog/2016/05/11/apache-spark-2-0-technical-preview-easier-faster-and-smarter.html

Spark 2.0 ships with the second generation Tungsten engine. This engine builds
upon ideas from modern compilers and MPP databases and applies them to data
processing. The main idea is to emit optimized bytecode at runtime that
collapses the entire query into a single function, eliminating virtual function
calls and leveraging CPU registers for intermediate data. We call this technique
“whole-stage code generation.”


B. Apache Spark as a Compiler: Joining a Billion Rows per Second on a Laptop
Deep dive into the new Tungsten execution engine

https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html

1. No virtual function dispatches:
2. Intermediate data in memory vs CPU registers
3. Loop unrolling and SIMD:

... That is, code generation in the past only sped up the evaluation of
expressions such as “1 + a”, whereas today whole-stage code generation actually
generates code for the entire query plan.


C. Whole-stage code generation: Spark 2.0's Tungsten engine

https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6122906529858466/293651311471490/5382278320999420/latest.html

To activate/deactivate the whole-stage optimization, you can set the
"spark.sql.codegen.wholeStage" option, in the shell as

    spark.conf.set("spark.sql.codegen.wholeStage", false)

Or probably through the "spark-submit" script (not tested)

    $ spark-submit
        <...>
        --conf spark.sql.codegen.wholeStage=false
        <...>


-------------------------------------------------------------------------------
How to launch a Spark interactive shell
-------------------------------------------------------------------------------

Please refer to: extended/docs/[overview].html 

You can simply lunch the Spark shell while at the Spark root folder:

    $ ./bin/spark-shell --master local[2]

You will have "SparkSession" as "spark" and "SparkContext" as "sc" available
automatically through the shell.

    

-------------------------------------------------------------------------------
Spark DataFrames
-------------------------------------------------------------------------------

Please refer to: extended/docs/[spark-sql].html

DataFrames are basically tables with rows and columns. To understand the
concepts, and the relation between RDD and DataFrame try the following code
snippet through the Spark interactive shell. This code generates RDDs and then
translates them into DataFrames. The code is inspired from:
http://spark.apache.org/docs/latest/sql-programming-guide.html

    $ ./bin/spark-shell --master local[2]
       
    scala> case class Sample(vec: Array[Double], label: Double)

    scala> val seeds = Array.tabulate(5)(x => x)
    scala> val rdd_1 = sc.parallelize(seeds, 5)
    scala> val rdd_2 = rdd_1.flatMap(x => Array.tabulate(2)(y => x*10 +y))
    scala> val rdd_3 = rdd_2.map(x => Sample(Array.tabulate(3)(y => x), 17))

Print the RDD to get a hang of what it looks like:

    scala> rdd_3.foreach(println)
    scala> rdd_3.foreach(e => {println(e.vec);})
    scala> rdd_3.foreach(e => {println(e.vec(0));})

Translate the RDD into DataFrame, for implicit translation from RDD to Data
Frame you will need to import "spark.implicits._":

    scala> import spark.implicits._
    scala> val df = rdd_3.toDF()

Print the DataFrame, notice the content of each row: the type "WrappedArray" 

    scala> df.show() 
    scala> df.foreach(e => {println(e);})

To get each column in a row you can use the index:

    scala> df.foreach(e => {println(e(0));})

But, each column entry is basically a String, you need type casting to go any
further. Note that WrappedAraay should be cast to Seq not Array.

    scala> df.foreach(e => {println(e(0).asInstanceOf[Seq[Double]]);})
    scala> df.foreach(e => {println(e(0).asInstanceOf[Seq[Double]](0));})

As a batch example, in "spark-1.6/extended" folder I wrote a logistic regression app
with the RDDs. Now In the "extended/lr-dataframe" folder here I added the version that
works with DataFrames. In addition to RDD to DataFrame translation it has
proper casting to get the values from DataFrames. Look at the following
snippet:


    10     import org.apache.spark.sql.SparkSession
    10     import spark.implicits._
    10
    10     val spark = SparkSession
    10       .builder()
    10       .appName("Spark SQL Example")
    10       .getOrCreate()
    11 
    11     val df_samples = rdd_samples.toDF();
    
    
    14                       vector_mul(factor, p(0).asInstanceOf[Seq[Double]])


** NOTE: to compile with sbt you will need to add "spark-sql" as dependencies.
Also, do not forget the correct version for Scala (2.11.8) and Spark 2.0.0. For
example the sbt in "extented/lr-dataframe" folder is:
    
    1 name := "Logistic Regression"
    2 
    3 version := "1.0"
    4 
    5 scalaVersion := "2.11.8"
    6 
    7 libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
    8 libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0

** NOTE: I did not see any improvements in Spark-2.0 for my own RDD
implementation of logistic regression. Also, changing to DataFrames actually
slowed things down, most probably due to extra casting. Perhaps the
"whole-satge" code generation does not kick in and there is a extra casting
cost. Next, I will try to use MLlib directly on DataFrames to see possible
improvements over my base RDD implementations.







===========================================================================================
======> TO BE COMPLETED

http://spark.apache.org/docs/latest/ml-pipeline.html
http://spark.apache.org/docs/latest/mllib-data-types.html
===========================================================================================
./bin/spark-shell --master local[2]

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row

case class Sample(vec: org.apache.spark.mllib.linalg.Vector, label: Double)

val input_seed = Array.tabulate(5)(x => x)
val input_samples_1 = sc.parallelize(input_seed, 5)
val input_samples_2 = input_samples_1.flatMap(x => Array.tabulate(2)(y => x*10 +y))

val input_samples_3 = input_samples_2.map(x => Sample(Vectors.dense(1.0, 0.0, 3.0), 17))
input_samples_3.foreach(e => {println(e.vec(0));})


val df_3 = input_samples_3.toDF()
df_3.foreach(e => {println(e(0).asInstanceOf[org.apache.spark.mllib.linalg.Vector](0));})


val df_t = spark.createDataFrame(Seq((1.0, Vectors.dense(0.0, 1.1, 0.1)), (0.0, Vectors.dense(2.0, 1.0, -1.0)))).toDF("label", "features")
df_t.show()


val input_samples_4 = input_samples_2.map(x => (1.0, Vectors.dense(0.0, 1.1, 0.1)))
input_samples_4.foreach(e => {println(e);})

val df_4 = input_samples_4.toDF()
df_4.show();

val df_5 = input_samples_4.toDF("label", "features")
df_5.show();


val lr = new LogisticRegression()
lr.setMaxIter(10)
lr.setRegParam(0.01)

val model1 = lr.fit(df_t)
val model2 = lr.fit(df_5)


===========================================================================================



