package com.dahua.analyse1

import com.dahua.bean.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object test {

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
    //接收参数
    val Array(inputPath)=args
    //获取sparksession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[String] = sc.textFile(inputPath)
    val res: RDD[Log] = rdd.map(line => {
      line.split(",", -1)
    }).filter(_.length >= 85).map(Log(_))
//    val res1: RDD[((String, String), Int)] = res.map(x => {
//      val id: String = ifID(x.imei, x.mac, x.idfa, x.openudid, x.androidid)
//      val adspacetype: String = ifAdspacetype(x.adspacetype)
//      ((id, adspacetype), 1)
//    }).filter(x => (!x._1._1.isEmpty))
//    val adReduceByKey: RDD[((String, String), Int)] = res1.reduceByKey(_+_)
//    val ad: RDD[String] = adReduceByKey.map(x => {
//      x._1._1 + "," + x._1._2 + "->" + x._2
//    })
//    ad.foreach(println(_))
        val res1: RDD[(String, String)] = res.map(x => {
          val id: String = ifID(x.imei, x.mac, x.idfa, x.openudid, x.androidid)
          val adspacetype: String = ifAdspacetype(x.adspacetype)
          (id, adspacetype)
        }).filter(x => (!x._1.isEmpty))
        val ress: RDD[(String, String)] = res1.reduceByKey((x, y) => {
          var gSum = 0

          val str12: String = x.substring(6)
          val str1: String = x.substring(0, 6)

          val str22: String = y.substring(6)
          val str2: String = y.substring(0, 6)
          if (str1.equals(str2)) {
            gSum = str12.toInt + str22.toInt
          }
          (str1 + gSum)
        })
//        ress.foreach(println(_))
      val res2: RDD[(String, String)] = res.map(x => {
        val id: String = ifID(x.imei, x.mac, x.idfa, x.openudid, x.androidid)
        val adspacetype: String = ifAdspacetypename(x.adspacetype,x.adspacetypename)
        (id, adspacetype)
      }).filter(x => (!x._1.isEmpty))
      val ress1: RDD[(String, String)] = res2.reduceByKey((x, y) => {
        var gSum = 0

        val splics: Array[String] = x.split("->")
        val str12: String = splics(1)
        val str1: String = splics(0)

        val splics1: Array[String] = x.split("->")
        val str22: String = splics1(1)
        val str2: String = splics1(0)
//        println(str12)
//        println(str22)
        if (str1.equals(str2)) {
          gSum = str12.toInt + str22.toInt
        }
        (str1+"->" + gSum)
      })
//      ress1.foreach(println(_))
    val value: RDD[(String, (String, String))] = ress.join(ress1)
    value.foreach(println(_))
    //6c892d8bca7fd1e8
    sc.stop()
  }
  def ifID(imei: String,mac: String,idfa: String,openudid: String, androidid: String): String ={
    if(!imei.isEmpty){
      imei
    }else if(!mac.isEmpty){
      mac
    }else if(!idfa.isEmpty){
      idfa
    }else if(!openudid.isEmpty){
      openudid
    }else{
      androidid
    }
  }
//  def ifAdspacetype(adspacetype: Int):String={
//    if(adspacetype<10){
//      "LC0"+adspacetype
//    }else{
//      "LC"+adspacetype
//    }
//  }
      def ifAdspacetype(adspacetype: Int):String={
        if(adspacetype<10){
          "LC0"+adspacetype+"->1"
        }else{
          "LC"+adspacetype+"->1"
        }
      }
    def ifAdspacetypename(adspacetype: Int,adspacetypename: String):String={
      if(adspacetype<10){
        "LN0"+adspacetype+adspacetypename+"->1"
      }else{
        "LN"+adspacetype+adspacetypename+"->1"
      }
    }
}
