package com.dahua.analyse

import com.dahua.bean.Log
import com.dahua.util.TerritoryTool
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object TerritoryAnaV3_____ {
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
    val Array(inputPath,inputPath2)=args
    //获取sparksession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[String] = sc.textFile(inputPath)
    val log: RDD[Log] = rdd.map(_.split(",", -1)).filter(_.length >= 85).map(Log(_))
    log.map(log=>{
      TerritoryTool.qqsRtp(log.requestmode,log.processnode)
    })

    spark.stop()
  }
}
