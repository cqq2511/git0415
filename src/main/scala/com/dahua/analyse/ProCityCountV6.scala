package com.dahua.analyse

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties


object ProCityCountV6 {
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
    val df: DataFrame = spark.read.parquet(inputPath)
    df.createTempView("log")
    val sql =
      """
        |select provincename,
        |       cityname,
        |       sum(case when requestmode =1 and processnode >=1 then 1 else 0 end)as ysqq,
        |       sum(case when requestmode =1 and processnode >=2 then 1 else 0 end)as yxqq,
        |       sum(case when requestmode =1 and processnode =3 then 1 else 0 end)as ggqq,
        |       sum(case when iseffective =1 and isbilling =1 and isbid = 1 and adorderid !=0 then 1 else 0 end)as jjx,
        |       sum(case when iseffective =1 and isbilling =1 and iswin = 1 then 1 else 0 end)as jjcgs,
        |       sum(case when requestmode =2 and iseffective =1 then 1 else 0 end)as zss,
        |       sum(case when requestmode =3 and iseffective =1 then 1 else 0 end)as djs,
        |       sum(case when requestmode =2 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjzss,
        |       sum(case when requestmode =3 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjdjs,
        |       sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (winprice*1.0)/1000 else 0 end )as xiaofei,
        |       sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (adpayment*1.0)/1000 else 0 end )as chengben
        |from log
        |group by provincename,
        |         cityname
        |""".stripMargin
    val res: DataFrame = spark.sql(sql)
    val load: Config = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("driver",load.getString("jdbc.driver"))
    properties.setProperty("password",load.getString("jdbc.password"))
    res.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName1"),properties)
    // 关闭对象。

    spark.stop()
  }
}
