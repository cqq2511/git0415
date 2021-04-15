package com.dahua.analyse

import com.dahua.bean.Log
import com.dahua.util.TerritoryTool
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object MeitiAna {
  def main(args: Array[String]): Unit = {
    if(args.length!=3){
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
    val Array(inputPath,appmapping,outputPath)=args
    //获取sparksession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = spark.sparkContext
    val mapping: RDD[String] = sc.textFile(appmapping)
    val map: Map[String, String] = mapping.map(line => {
      val arr: Array[String] = line.split(":", -1)
      (arr(0), arr(1))
    }).collect().toMap
    ///用广播变量，进行广播
    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(map)

    val rdd: RDD[String] = sc.textFile(inputPath)
    //业务
    val log: RDD[Log] = rdd.map(_.split(",", -1)).filter(_.length >= 85).map(Log(_)).filter(t => !t.appid.isEmpty || !t.appname.isEmpty)
    val res: RDD[(String, List[Double])] = log.map(log => {
      val qqs: List[Double] = TerritoryTool.qqsRtp(log.requestmode, log.processnode)
      //媒体名字
      var appname: String = log.appname
      if (appname == "" || appname.isEmpty) {
        //从广播变量中获得
        appname = broadcast.value.getOrElse(log.appid, "不明确")
      }
      (appname, qqs)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })
    res.saveAsTextFile(outputPath)
    spark.stop()

  }
}
