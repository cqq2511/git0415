package com.dahua.analyse

import com.dahua.util.ifFile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object ProCityCountV6_1 {
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
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[String] = sc.textFile(inputPath)
    val split: RDD[Array[String]] = rdd.map(_.split(",", -1)).filter(_.length>=85)
    val rdd1: RDD[((String, String, String), Int)] = split.map(x => {
      ((x(24), x(25), x(8)), 1)
    })
    val rb: RDD[((String, String, String), Int)] = rdd1.reduceByKey(_ + _)
    val rb1: RDD[((String, String), Int)] = rb.map(x => ((x._1._1, x._1._2), x._2)).sortBy(x => x._2, false)
    rb1.groupByKey()
    ifFile.ifFiles(outputPath,sc)
    rb1.saveAsTextFile(outputPath)
    spark.stop()
  }
}
