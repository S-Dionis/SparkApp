import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}

object SparkApp extends App {

  var conf = new SparkConf().setMaster("local").setAppName("Demo.app")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val jsons = Seq(
    "data/emp/developers/111.json",
    "data/emp/hr/777.json",
    "data/emp/managers/555.json"
  )

  readEmployeesFromJson(jsons)

  sc.stop()

  private def readFile(file: File): DataFrame = {
    val name = file.getParentFile.getName;
    sqlContext.read.json(sc.wholeTextFiles(file.getAbsolutePath).values).withColumn("department", lit(name))
  }

  private def readJson(seq: Seq[String], executor: ExecutorService): Seq[DataFrame] = {
    implicit val executionContext: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(executor)

    val futures = for (path <- seq) yield Future {
      readFile(new File(path))
    }

    val future = Future.sequence(futures)
    Await.result(future, Duration.Inf)
  }


  def readEmployeesFromJson(seq: Seq[String]): Unit = {
    val executorService = Executors.newFixedThreadPool(4)

    try {
      val frame = readJson(jsons, executorService)
        .reduce((a, b) => a.unionAll(b))
      frame.registerTempTable("EMP")

      frame.coalesce(1)
        .write
        .format("com.databricks.spark.csv")
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .save("csv")

      println("saved to csv")

      println("employee with max salary:")
      sqlContext.sql("SELECT name, department, salary FROM EMP ORDER BY salary desc LIMIT 1").show()

      println("salary summary:")
      sqlContext.sql("SELECT sum(salary) as SUM FROM EMP").show()
    } finally {
      executorService.shutdown()
    }
  }

}
