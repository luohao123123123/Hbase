package Spark_Hbase

import HbaseApi.util.HbaseUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import java.util

object SparkReadHbase_01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)

    val tableName = "Test1:test1"

    val config=HbaseUtil.getConfig
    config.set(TableInputFormat.INPUT_TABLE, tableName)

    val data = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    data.map(x=>{
      val rowkey=Bytes.toString(x._2.getRow)
      val value=Bytes.toString(x._2.getValue("rowkey".getBytes(),"id".getBytes()))
      (rowkey,value)
    })
      .foreach(println(_))

    //todo:批量查询rowkey
    val conn=HbaseUtil.getConnection
    val results = conn.getTable(TableName.valueOf(tableName)).get(new util.ArrayList[Get]())
    results.map(x=>x.getRow)


    sc.stop()
  }
}
