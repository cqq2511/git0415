package com.dahua.analyse




import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties


object ProCityCountV2 {
  def main(args: Array[String]): Unit = {
    if(args.length!=1){
      println(
        """
          |com.dahua.bz2parquet.Bz2Parquet
          |缺少参数
          |* loginputPath
          |     * logoutputPath
          |""".stripMargin)
      sys.exit()
    }
    //接受参数
    val Array(inputPath)=args

    //获取SparkSession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()


    //读取数据源
    val df: DataFrame = spark.read.parquet(inputPath)
    //创建临时试图
    df.createTempView("log")
    //编写sql
    val sql = "select provincename,cityname,count(*) from log group by provincename,cityname"
    val procityCount: DataFrame = spark.sql(sql)
    //输出到mysql
    val load: Config = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("driver",load.getString("jdbc.driver"))
    properties.setProperty("password",load.getString("jdbc.password"))
    procityCount.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),properties)

    spark.stop()
  }
}
