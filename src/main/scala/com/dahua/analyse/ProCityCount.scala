package com.dahua.analyse

import com.dahua.util.ifFile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}





object ProCityCount {
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
    //接受参数
    val Array(inputPath,outputPath)=args

    //获取SparkSession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //读取数据源
    val df: DataFrame = spark.read.parquet(inputPath)
    //创建临时试图
    df.createTempView("log")
    //编写sql
    val sql = "select provincename,cityname,count(*) from log group by provincename,cityname"
    val procityCount: DataFrame = spark.sql(sql)
    //输出到json目录
    //判断输出路径是否存在，存在就删
    ifFile.ifFiles(outputPath,sc)
    procityCount.write.partitionBy("provincename","cityname").json(outputPath)

    spark.stop()
    sc.stop()
  }
}
