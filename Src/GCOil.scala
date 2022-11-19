import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.datasources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{concat_ws, lit}

object GCOil{
def main(args: Array[String]) {
val spark = SparkSession.builder.appName("Gulf Coast OIL Use Case").getOrCreate()
spark.conf.set("spark.hadoop.yarn.resourcemanager.hostname","resourcemanager");
spark.conf.set("spark.hadoop.yarn.resourcemanager.address","resourcemanager:8030");
val conf = HBaseConfiguration.create()
conf.set("hbase.zookeeper.quorum", "zookeeper:2181")
new HBaseContext(spark.sparkContext, conf)

val ddlString = "`resID` STRING, `date` STRING, `time` STRING, `hz` FLOAT, `disp` FLOAT, `flo` INT, `sedPPM` FLOAT, `psi` INT, `chlPPM` FLOAT"
val ddlSchema = StructType.fromDDL(ddlString)
val csvDF = spark.readStream.option("sep", ",").schema(ddlSchema).csv("hdfs://namenode:9000/input/gcoil/")
import spark.implicits._
val sensor_data = csvDF.select("*").withColumn("rowkey",concat_ws("_",$"resID",$"date",$"time"))
//csvDF.select("*").withColumn("rowkey",concat_ws("_",$"resID",$"date",$"time")).writeStream.format("console").start()  
sensor_data.writeStream.option("checkpointLocation", "checkpoint/data").foreachBatch{
    (batchDF: DataFrame, batchId: Long)=>
    writeToHBaseTable(batchDF,data_catalog)
    val alert_data = batchDF.select("rowkey","psi")
    writeToHBaseTable(alert_data,alert_catalog)
}.start().awaitTermination()
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

def alert_catalog = s"""{
    |"table":{"namespace":"default", "name":"sensor"},
    |"rowkey":"key",
    |"columns":{
    |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
    |"psi":{"cf":"alert", "col":"psi", "type":"string"}
    |}
|}""".stripMargin
}
