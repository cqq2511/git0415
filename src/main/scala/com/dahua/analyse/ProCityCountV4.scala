package com.dahua.analyse

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties


object ProCityCountV4 {
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
    val sql = "select provincename,cityname,row_number() over(partition by provincename order by cityname desc) as count from log group by provincename,cityname"
    spark.sql(sql).show(50)
    // 需求1： 统计各个省份分布情况，并排序。

    // 需求2： 统计各个省市分布情况，并排序。

    // 需求3： 使用RDD方式，完成按照省分区，省内有序。

    // 需求4： 将项目打包，上传到linux。使用yarn_cluster 模式进行提交，并查看UI。

    // 需求5： 使用azkaban ，对两个脚本进行调度。

    spark.stop()
  }
}
