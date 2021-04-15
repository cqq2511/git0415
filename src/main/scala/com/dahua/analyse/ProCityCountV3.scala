package com.dahua.analyse

import com.dahua.util.ifFile
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}


object ProCityCountV3 {
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
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val rdd: RDD[String] = sc.textFile(inputPath)
    val mf: RDD[Array[String]] = rdd.map(line => line.split(",", -1)).filter(arr => arr.length >= 85)
    val proCity: RDD[((String, String), Int)] = mf.map(field => {
      val pro = field(24)
      val city = field(25)
      ((pro, city), 1)
    })
    val reduceByKey: RDD[((String, String), Int)] = proCity.reduceByKey(_ + _)
    val res: RDD[String] = reduceByKey.map(res => {
      res._1._1 + "\t" + res._1._2 + "\t" + res._2
    })

    res.saveAsTextFile(outputPath)
    spark.stop()
    sc.stop()
  }
}
