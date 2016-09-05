
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

    // Print the RDD to get a hang of what it looks like:
    scala> rdd_3.foreach(println)
    scala> rdd_3.foreach(e => {println(e.vec);})
    scala> rdd_3.foreach(e => {println(e.vec(0));})

    // Translate the RDD into DataFrame, for implicit translation from RDD to
    // Data Frame you will need to import "spark.implicits._":
    scala> import spark.implicits._
    scala> val df = rdd_3.toDF()

    // Print the DataFrame, notice the content of each row: the type "WrappedArray" 
    scala> df.show() 
    scala> df.foreach(e => {println(e);})

    // To get each column in a row you can use the index:
    scala> df.foreach(e => {println(e(0));})

    // But, each column entry is basically a String, you need type casting to go
    // any further. Note that WrappedAraay should be cast to Seq (not Array).
    scala> df.foreach(e => {println(e(0).asInstanceOf[Seq[Double]]);})
    scala> df.foreach(e => {println(e(0).asInstanceOf[Seq[Double]](0));})


As a batch example, I have written a logistic regression application that uses
DataFrames in "extended/lr-dataframe" folder. It is similar to the one in
"extended/lr-rdd" folder, except that instead of RDDs it uses DataFrames.
In addition to RDD to DataFrame translation it has proper casting to get the
values from DataFrames. Look at the following snippets:


    57   def processPartition(samples: Iterator[org.apache.spark.sql.Row], weight: Seq[Double], wait_us: Int)
    58     : Iterator[Seq[Double]] = {
    59 
    60     var local_gradient: Seq[Double] = Array.tabulate(weight.length)(i => 0.0)
    61 
    62     if (wait_us != 0) {
    63       spinWait(wait_us)
    64       return Iterator(local_gradient)
    65     }
    66 
    67     for (s <- samples) {
    68       val vec = s(0).asInstanceOf[Seq[Double]]
    69       val label = s(1).asInstanceOf[Double]
    70       val factor =  (1 / (1 + math.exp(label * (vectorDot(weight, vec)))) - 1) * label;
    71       val scaled = vectorScale(vec, factor)
    72       local_gradient = vectorAdd(scaled, vec)
    73     }
    74 
    75     return Iterator(local_gradient)
    76   }


    121     // translate rdd in to dataframes
    122     import org.apache.spark.sql.SparkSession
    123     val spark = SparkSession.builder().getOrCreate()
    124     import spark.implicits._
    125 
    126     val input_samples = rdd_samples.toDF();


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
"whole-satge" code generation does not kick in and there is an extra casting
cost. Next, I will try to use MLlib directly on DataFrames to see possible
improvements over my base RDD implementations.



-------------------------------------------------------------------------------
Spark MLlib
-------------------------------------------------------------------------------

Please refer to: extended/docs/[ml-pipelines, ml-data-types, regression, clustering].html

Here, I will go over an example that uses MLLib pipelines for regression. The
code will run in the Spark interactive shell. It is inspired by:
http://spark.apache.org/docs/latest/ml-pipeline.html
http://spark.apache.org/docs/latest/mllib-data-types.html
http://spark.apache.org/docs/latest/ml-clustering.html
http://spark.apache.org/docs/latest/ml-classification-regression.html

    $ ./bin/spark-shell --master local[2]

    scala> import org.apache.spark.ml.classification.LogisticRegression
    scala> import org.apache.spark.ml.linalg.{Vector, Vectors}

    scala> import spark.implicits._
    scala> val training = spark.createDataFrame(Seq(
                (1.0, Vectors.dense(0.0, 1.1, 0.1)),
                (0.0, Vectors.dense(2.0, 1.0, -1.0))
                )).toDF("label", "features")
    scala> training.show()

    scala> val lr = new LogisticRegression()
    scala> lr.setMaxIter(10).setRegParam(0.01)
    scala> val model = lr.fit(training)

    scala> println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")


Now, another more complex example. First generate the training data using RDDs,
and then transform them into DataFrames. Note that I do not know how to make,
parallelize, and partition the training data directly from DataFrames. Perhaps
there is an easier way to do this.

    $ ./bin/spark-shell --master local[2]

    scala> import org.apache.spark.ml.classification.LogisticRegression
    scala> import org.apache.spark.ml.linalg.{Vector, Vectors}

    scala> val seeds = Array.tabulate(5)(x => x)
    scala> val rdd_1 = sc.parallelize(seeds, 5)
    scala> val rdd_2 = rdd_1.flatMap(x => Array.tabulate(2)(y => x*10 +y))
    scala> val rdd_3 = rdd_2.map(x => ((x%2).asInstanceOf[Double], Vectors.dense(x%2, 1.1, 0.1)))

    // Note that in generating the DataFrames for MLlib, it expects the columns
    // to be named "label" and "features" as follows:

    scala> import spark.implicits._
    scala> val df = rdd_3.toDF("label", "features")
    scala> df.show()
    scala> df.foreach(e => {println(e(1).asInstanceOf[org.apache.spark.ml.linalg.Vector](0));})

    scala> val lr = new LogisticRegression()
    scala> lr.setMaxIter(10).setRegParam(0.01)
    scala> val model = lr.fit(df)

    scala> println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")


Here is additional code to the example above that implements KMeans clustering
algorithm.

    scala> import org.apache.spark.ml.clustering.KMeans

    scala> val kmeans = new KMeans().setK(2).setMaxIter(10)
    scala> val model = kmeans.fit(df)

    scala> println("Cluster Centers: ")
    scala> model.clusterCenters.foreach(println)


**NOTE: logically, KMeans should only require "features", but it seems that the
implementation of "toDF()" requires the RDD elements to be tuples. Even the
example data form Spark documentation generates dataset as DataFrames
with integer "label" and a vector "features". See the example at:
http://spark.apache.org/docs/latest/ml-clustering.html

    val dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")


I have also witten a batch version of the code in "extented/lr-mllib" and
"extented/kmeans-mllib" folders respectively.

** NOTE: to compile with sbt you will need to add "spark-sql" and "spark-mllib" as dependencies.
Also, do not forget the correct version for Scala (2.11.8) and Spark 2.0.0. For
example the sbt in "extented/lr-mllib" folder is:
    
    1 name := "Logistic Regression"
    2 
    3 version := "1.0"
    4 
    5 scalaVersion := "2.11.8"
    6 
    7 libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
    8 libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0
    9 libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.0


** NOTE: from the following two option for importing Vector(s), only the second
one is acceptable for MLlib with DataFrames API:

        RIGHT> import org.apache.spark.ml.linalg.{Vector, Vectors}
        WRONG> import org.apache.spark.mllib.linalg.{Vector, Vectors}

If you use the second one you get an exception like this:

    $ java.lang.IllegalArgumentException: requirement failed: Column features must be
      of type org.apache.spark.ml.linalg.VectorUDT@3bfc3ba7 but was actually
      org.apache.spark.mllib.linalg.VectorUDT@f71b0bce.

The explanation from MLlib website:
http://spark.apache.org/docs/latest/ml-guide.html
"Spark’s linear algebra dependencies were moved to a new project, mllib-local
(see SPARK-13944). As part of this change, the linear algebra classes were
copied to a new package, spark.ml.linalg. The DataFrame-based APIs in spark.ml
now depend on the spark.ml.linalg classes, leading to a few breaking changes,
predominantly in various model classes (see SPARK-14810 for a full list)."


-------------------------------------------------------------------------------
Spark 2.0 Large Event Logs Problem
-------------------------------------------------------------------------------

By setting spark.eventLog.enabled to "true", the event logs will be saved in to
a file at the master, when application ends. While I did not have any problem
with the size of logs with Spark 1.6, the files are extremely big in Spark 2.0.
For example for 100 slaves experiment the final logs only for 2 iteration is
about 8.3 GB. This is a problem for parsing and also serialization time for JSON
objects is too excessive. That made it very confusing why applications where
taking too long to finish with Spark 2.0.

The main problem was "SparkListenerTaskEnd" and specifically the "Accumulables"
in the "Task Info" field. Also there was extra info in the "Task Metrics"
field. I removed the excessive data in the source and recompiled the spark.

Currently the source has been changed compared to the downloded file from the
Spark website. I had to make changes in the following file (the changes diffs
is attached below):

    core/src/main/scala/org/apache/spark/util/JsonProtocol.scala

NOTE: spark needs to be compiled again after source changes:

    $ build/mvn -DskipTests clean package



    diff --git a/src/spark-2.0/core/src/main/scala/org/apache/spark/util/JsonProtocol.scala b/src/spark-2.0/core/src/main/scala/org/apache/spark/util/JsonProtocol.scala
    index 18547d4..3be0429 100644
    --- a/src/spark-2.0/core/src/main/scala/org/apache/spark/util/JsonProtocol.scala
    +++ b/src/spark-2.0/core/src/main/scala/org/apache/spark/util/JsonProtocol.scala
    @@ -279,8 +279,8 @@ private[spark] object JsonProtocol {
         ("Speculative" -> taskInfo.speculative) ~
         ("Getting Result Time" -> taskInfo.gettingResultTime) ~
         ("Finish Time" -> taskInfo.finishTime) ~
    -    ("Failed" -> taskInfo.failed) ~
    -    ("Accumulables" -> JArray(taskInfo.accumulables.map(accumulableInfoToJson).toList))
    +    ("Failed" -> taskInfo.failed)
    +    // ("Accumulables" -> JArray(taskInfo.accumulables.map(accumulableInfoToJson).toList))
       }
     
       def accumulableInfoToJson(accumulableInfo: AccumulableInfo): JValue = {
    @@ -324,22 +324,22 @@ private[spark] object JsonProtocol {
     
       def taskMetricsToJson(taskMetrics: TaskMetrics): JValue = {
         val shuffleReadMetrics: JValue =
    -      ("Remote Blocks Fetched" -> taskMetrics.shuffleReadMetrics.remoteBlocksFetched) ~
    -        ("Local Blocks Fetched" -> taskMetrics.shuffleReadMetrics.localBlocksFetched) ~
    -        ("Fetch Wait Time" -> taskMetrics.shuffleReadMetrics.fetchWaitTime) ~
    -        ("Remote Bytes Read" -> taskMetrics.shuffleReadMetrics.remoteBytesRead) ~
    -        ("Local Bytes Read" -> taskMetrics.shuffleReadMetrics.localBytesRead) ~
    -        ("Total Records Read" -> taskMetrics.shuffleReadMetrics.recordsRead)
    +      ("Remote Blocks Fetched" -> taskMetrics.shuffleReadMetrics.remoteBlocksFetched)
    +        // ("Local Blocks Fetched" -> taskMetrics.shuffleReadMetrics.localBlocksFetched) ~
    +        // ("Fetch Wait Time" -> taskMetrics.shuffleReadMetrics.fetchWaitTime) ~
    +        // ("Remote Bytes Read" -> taskMetrics.shuffleReadMetrics.remoteBytesRead) ~
    +        // ("Local Bytes Read" -> taskMetrics.shuffleReadMetrics.localBytesRead) ~
    +        // ("Total Records Read" -> taskMetrics.shuffleReadMetrics.recordsRead)
         val shuffleWriteMetrics: JValue =
    -      ("Shuffle Bytes Written" -> taskMetrics.shuffleWriteMetrics.bytesWritten) ~
    -        ("Shuffle Write Time" -> taskMetrics.shuffleWriteMetrics.writeTime) ~
    -        ("Shuffle Records Written" -> taskMetrics.shuffleWriteMetrics.recordsWritten)
    +      ("Shuffle Bytes Written" -> taskMetrics.shuffleWriteMetrics.bytesWritten)
    +        // ("Shuffle Write Time" -> taskMetrics.shuffleWriteMetrics.writeTime) ~
    +        // ("Shuffle Records Written" -> taskMetrics.shuffleWriteMetrics.recordsWritten)
         val inputMetrics: JValue =
    -      ("Bytes Read" -> taskMetrics.inputMetrics.bytesRead) ~
    -        ("Records Read" -> taskMetrics.inputMetrics.recordsRead)
    +      ("Bytes Read" -> taskMetrics.inputMetrics.bytesRead)
    +        // ("Records Read" -> taskMetrics.inputMetrics.recordsRead)
         val outputMetrics: JValue =
    -      ("Bytes Written" -> taskMetrics.outputMetrics.bytesWritten) ~
    -        ("Records Written" -> taskMetrics.outputMetrics.recordsWritten)
    +      ("Bytes Written" -> taskMetrics.outputMetrics.bytesWritten)
    +        // ("Records Written" -> taskMetrics.outputMetrics.recordsWritten)
         val updatedBlocks =
           JArray(taskMetrics.updatedBlockStatuses.toList.map { case (id, status) =>
             ("Block ID" -> id.toString) ~
    @@ -349,14 +349,14 @@ private[spark] object JsonProtocol {
         ("Executor Run Time" -> taskMetrics.executorRunTime) ~
         ("Result Size" -> taskMetrics.resultSize) ~
         ("JVM GC Time" -> taskMetrics.jvmGCTime) ~
    -    ("Result Serialization Time" -> taskMetrics.resultSerializationTime) ~
    -    ("Memory Bytes Spilled" -> taskMetrics.memoryBytesSpilled) ~
    -    ("Disk Bytes Spilled" -> taskMetrics.diskBytesSpilled) ~
    -    ("Shuffle Read Metrics" -> shuffleReadMetrics) ~
    -    ("Shuffle Write Metrics" -> shuffleWriteMetrics) ~
    -    ("Input Metrics" -> inputMetrics) ~
    -    ("Output Metrics" -> outputMetrics) ~
    -    ("Updated Blocks" -> updatedBlocks)
    +    ("Result Serialization Time" -> taskMetrics.resultSerializationTime)
    +    // ("Memory Bytes Spilled" -> taskMetrics.memoryBytesSpilled) ~
    +    // ("Disk Bytes Spilled" -> taskMetrics.diskBytesSpilled) ~
    +    // ("Shuffle Read Metrics" -> shuffleReadMetrics) ~
    +    // ("Shuffle Write Metrics" -> shuffleWriteMetrics) ~
    +    // ("Input Metrics" -> inputMetrics) ~
    +    // ("Output Metrics" -> outputMetrics) ~
    +    // ("Updated Blocks" -> updatedBlocks)
       }
     
       def taskEndReasonToJson(taskEndReason: TaskEndReason): JValue = {


