package com.dahua.bz2parquet

import com.dahua.bean.Log
import com.dahua.util.{NumFormat, SchemaUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Bz2ParquetV2 {
  def main(args: Array[String]): Unit = {
    /**
     * 定义参数
     * loginputPath
     * logoutputPath
     */
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
    val Array(loginputPath,logoutputPath)=args
    //需要：SparkContext,SparkSession    开启querIO序列化
    val conf = new SparkConf().set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //将自定义类注册kryo序列
    conf.registerKryoClasses(Array(classOf[Log]))
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("yarn").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    //读取元数据
    val log: RDD[String] = sc.textFile(loginputPath)
    //对数据进行处理ETL
    val arr: RDD[Array[String]] = log.map(_.split(",", -1))
    val filter: RDD[Array[String]] = arr.filter(_.length >= 85)
    val logBean: RDD[Log] = filter.map(Log(_))
    val df: DataFrame = spark.createDataFrame(logBean)
    df.write.parquet(logoutputPath)
    //关闭spark sc
    spark.stop()
    sc.stop()

  }

}
