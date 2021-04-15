package com.dahua.analyse

import com.dahua.bean.{Log, Sort}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object MeitiAnaVq3 {
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
        |select appid,newname,
        |sum(case when requestmode =1 and processnode >=1 then 1 else 0 end )as ysqq,
        |sum(case when requestmode =1 and processnode >=2 then 1 else 0 end )as yxqq,
        |sum(case when requestmode =1 and processnode = 3 then 1 else 0 end )as ggqq,
        |sum(case when iseffective =1 and isbilling = 1 and isbid =1 and adorderid != 0 then 1 else 0 end )as jjx,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then 1 else 0 end )as jjcgs,
        |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )as zss,
        |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )as djs,
        |sum(case when requestmode =2 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjzss,
        |sum(case when requestmode =3 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjdjs,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (winprice*1.0)/1000 else 0 end )as xiaofei,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (adpayment*1.0)/1000 else 0 end )as chengben
        |from
        |(select l.*,
        |(case when l.appname == '' or l.appname is null or l.appname = '其他' then m.appname else l.appname end) as newname
        |from log l
        |left outer join
        |apn m
        |on l.appid = m.appid) a
        |group by appid,newname
        |""".stripMargin

    spark.sql(sql).repartition(1).write.format("csv").save(outputPath)

    sc.stop()
  }
}
