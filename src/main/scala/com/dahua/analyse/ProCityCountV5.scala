package com.dahua.analyse


import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap


object ProCityCountV5 {
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
    // 需求3： 使用RDD方式，完成按照省分区，省内有序。
    //获取SparkSession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("yarn").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    //读取数据源
    val rdd: RDD[String] = sc.textFile(inputPath)
    val mf: RDD[Array[String]] = rdd.map(line => line.split(",", -1)).filter(arr => arr.length >= 85)
    val proCity: RDD[((String, String), Int)] = mf.map(field => {
      val pro = field(24)
      val city = field(25)
      ((pro, city), 1)
    })
    val reduceByKey: RDD[((String, String), Int)] = proCity.reduceByKey(_ + _)

    val strings: Array[String] = reduceByKey.map(_._1._1).distinct().collect()

    val ama: RDD[(String, (String, Int))] = reduceByKey.map(x => (x._1._1, (x._1._2, x._2))).sortBy(x=>x._2._2,false)

    val pcp: RDD[(String, (String, Int))] = ama.partitionBy(new PCPartition(strings))

    pcp.saveAsTextFile(outputPath)
    //pcp.saveAsTextFile("F:\\kecheng\\java\\20210412dmp\\dmp\\oo1")

    sc.stop()
    spark.stop()
  }

}

class PCPartition(args: Array[String]) extends Partitioner {
  private val partitionMap: HashMap[String, Int] = new HashMap[String, Int]()
  var parId = 0
  for (arg <- args) {
    if (!partitionMap.contains(arg)) {
      partitionMap(arg) = parId
      parId += 1
    }
  }

  override def numPartitions: Int = partitionMap.valuesIterator.length

  override def getPartition(key: Any): Int = {
    val keys: String = key.asInstanceOf[String]
    val sub = keys
    partitionMap(sub)
  }
}