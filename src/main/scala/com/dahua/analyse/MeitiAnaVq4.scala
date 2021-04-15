package com.dahua.analyse

import com.dahua.bean.{Log, Sort}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object MeitiAnaVq4 {
  def main(args: Array[String]): Unit = {
    if(args.length!=3){
      println(
        """
          |com.dahua.bz2parquet.Bz2Parquet
          |缺少参数
          |* inputPath
          |appPath
          |     * outputPath
          |""".stripMargin)
      sys.exit()
    }
    //接收参数
    val Array(inputPath,appPath,outputPath)=args
    //获取sparksession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = spark.sparkContext

    //appname
    val ardd: RDD[String] = sc.textFile(appPath)
    val ardd1: RDD[Sort] = ardd.map(x => x.split(":", -1)).map(Sort(_))
    val adf: DataFrame = spark.createDataFrame(ardd1)
    adf.createTempView("apn")

    val rdd: RDD[String] = sc.textFile(inputPath)
    val rdd1: RDD[Log] = rdd.map(x => x.split(",", -1)).filter(_.length >= 85).map(Log(_))
    val df: DataFrame = spark.createDataFrame(rdd1)
    df.createTempView("log")
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

    spark.sql(sql).repartition(1).write.format("csv").save(outputPath)

    sc.stop()
  }
}
