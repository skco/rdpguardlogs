import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object RDPGuardLogs {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("RDPGuardLogs")
      .master("local[*]")
      .getOrCreate()

       import spark.implicits._

       val pattern: String = "( for user )|( RDP: )|( from )|\\["
       val logs:Dataset[Row] =  spark.read.text("E:/RDPLogs")
                                     .filter(!col("value").contains( "TRIAL EXPIRED"))
                                     .withColumn("value",substring(col("value"),6,1000))
                                     .withColumn("value",regexp_replace($"value","\\]|\\[",""))
                                     .withColumn("value",split(col("value"),pattern))
                                     .withColumn("datetime", $"value".getItem(0))
                                     .withColumn("datetime", to_timestamp(col("datetime")))
                                     .withColumn("operation", $"value".getItem(1))
                                     .withColumn("ip_address", $"value".getItem(2))
                                     .withColumn("user", $"value".getItem(3)).drop("value")
                                     .withColumn("day", date_format(col("datetime"), "yyyy-MM-dd").alias("day"))
                                     .withColumn("hour", date_format(col("datetime"), "yyyy-MM-dd hh").alias("hour"))
                                     .withColumn("minute", date_format(col("datetime"), "yyyy-MM-dd hh:mm").alias("minute"))
                                     .withColumn("sec", date_format(col("datetime"), "yyyy-MM-dd hh:mm:ss").alias("sec"))

       logs.printSchema()
       logs.show(10000,truncate = false)
       println(logs.count())

       //failed login attempts per second
       logs.na.drop().groupBy("sec").count().sort("sec").coalesce(1).write.csv("E:/RDPLogs/perSec.csv")

  }

}
