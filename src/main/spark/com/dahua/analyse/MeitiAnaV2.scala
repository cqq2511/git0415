package com.dahua.analyse

import com.dahua.bean.{Log, Sort}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MeitiAnaV2 {
  def main(args: Array[String]): Unit = {
      // 判断参数。
      if (args.length != 2) {
        println(
          """
            |com.dahua.analyse.ProCityCount
            |缺少参数
            |inputPath
            |appmapping
        """.stripMargin)
        sys.exit()
      }
      // 接收参数
      val Array(inputPath, appmapping) = args
      // 获取SparkSession
      val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
      val sc: SparkContext = spark.sparkContext

    val mapping: RDD[String] = sc.textFile(inputPath)
    val sortBean: RDD[Sort] = mapping.map(line => line.split("[:]",-1)).map(Sort(_))
    val df: DataFrame = spark.createDataFrame(sortBean)
    //创建临时试图
    df.createTempView("sort")
    val log: RDD[String] = sc.textFile(appmapping)
    val logBean: RDD[Log] = log.map(_.split(",", -1)).filter(_.length >=85).map(Log(_))
    val df1: DataFrame = spark.createDataFrame(logBean)
    df1.createTempView("log")//接受参数
    val sql =
      """
        |select case when l.appid=a.appid then a.appname
        |            else l.appname end as appname1 ,
        |        l.appid
        |from log l
        |     left join apn a
        |on l.appid=a.appid
        |
        |""".stripMargin
    spark.sql(sql).show(100)
    sc.stop()

  }

}
