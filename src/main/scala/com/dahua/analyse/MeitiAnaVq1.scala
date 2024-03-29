package com.dahua.analyse

import com.dahua.bean.Log
import com.dahua.util.{JedisUtil, TerritoryTool}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object MeitiAnaVq1 {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println(
        """
          |com.dahua.bz2parquet.Bz2Parquet
          |缺少参数
          |* loginputPath
          |     * logoutputPath
          |""".stripMargin)
      sys.exit()
    }
    //接收参数
    val Array(inputPath,outputPath)=args
    //获取sparksession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = spark.sparkContext

    val rdd: RDD[String] = sc.textFile(inputPath)
    val res: RDD[(String, List[Double])] = rdd.map(line => {
      line.split(",", -1)
    }).filter(_.length >= 85).map(Log(_)).filter(log => !log.appid.isEmpty || !log.appname.isEmpty)
      .mapPartitions(ite => {
        //建立连接
        var item = new ListBuffer[(String, List[Double])]
        val jedis = JedisUtil.resource
        ite.foreach(log => {
          var appname: String = log.appname
          if (appname.isEmpty) {
            appname = jedis.get(log.appid)
          }
          val qqs: List[Double] = TerritoryTool.qqsRtp(log.requestmode, log.processnode)
          item += ((appname, qqs))
        })
        item.iterator
      })
    res.reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1+"\t"+t._2.mkString(",")).saveAsTextFile(outputPath)

    sc.stop()
  }
}
