import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.datasources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object GCOil_Batch{
def main(args: Array[String]) {
val spark = SparkSession.builder.appName("Gulf Coast OIL Use Case").getOrCreate()
spark.conf.set("spark.hadoop.yarn.resourcemanager.hostname","resourcemanager");
spark.conf.set("spark.hadoop.yarn.resourcemanager.address","resourcemanager:8030");
val conf = HBaseConfiguration.create()
conf.set("hbase.zookeeper.quorum", "zookeeper:2181")
new HBaseContext(spark.sparkContext, conf)

import spark.implicits._
val sensor_data = readFromHBaseTable(spark,data_catalog).drop("rowkey").withColumn("rowkey",concat_ws("_",$"resID",$"date"))
val casted_data = sensor_data.withColumn("hz",col("hz").cast("double")).withColumn("psi",col("psi").cast("double").alias("psi"))
val agg_data    = casted_data.groupBy("rowkey").agg(
    avg("hz").as("hz_avg"),avg("psi").as("psi_avg"),
    min("hz").as("hz_min"),min("psi").as("psi_min"),
    max("hz").as("hz_max"),max("psi").as("psi_max")
    )
writeToHBaseTable(agg_data,agg_catalog)
}

def writeToHBaseTable(dfToWrite: DataFrame, schema: String) = {
    dfToWrite
      .write.options(Map(HBaseTableCatalog.tableCatalog -> schema, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.hadoop.hbase.spark")
      .save()
  }

def readFromHBaseTable(spark:SparkSession,schema: String): DataFrame = {
    spark
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> schema))
      .format("org.apache.hadoop.hbase.spark")
      .load()
  }
  
def data_catalog = s"""{
    |"table":{"namespace":"default", "name":"sensor"},
    |"rowkey":"key",
    |"columns":{
    |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
    |"resID":{"cf":"data", "col":"resID", "type":"string"},
    |"date":{"cf":"data", "col":"date", "type":"string"},
    |"time":{"cf":"data", "col":"time", "type":"string"},
    |"hz":{"cf":"data", "col":"hz", "type":"string"},
    |"disp":{"cf":"data", "col":"disp", "type":"string"},
    |"flo":{"cf":"data", "col":"flo", "type":"string"},
    |"sedPPM":{"cf":"data", "col":"sedPPM", "type":"string"},
    |"psi":{"cf":"data", "col":"psi", "type":"string"},
    |"chlPPM":{"cf":"data", "col":"chlPPM", "type":"string"}
    |}
|}""".stripMargin
    
def agg_catalog = s"""{
    |"table":{"namespace":"default", "name":"sensor"},
    |"rowkey":"key",
    |"columns":{
    |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
    |"hz_avg":{"cf":"stats", "col":"hz_avg", "type":"string"},
    |"hz_min":{"cf":"stats", "col":"hz_min", "type":"string"},
    |"hz_max":{"cf":"stats", "col":"hz_max", "type":"string"},
    |"psi_avg":{"cf":"stats", "col":"psi_avg", "type":"string"},
    |"psi_min":{"cf":"stats", "col":"psi_min", "type":"string"},
    |"psi_max":{"cf":"stats", "col":"psi_max", "type":"string"}
    |}
|}""".stripMargin
}