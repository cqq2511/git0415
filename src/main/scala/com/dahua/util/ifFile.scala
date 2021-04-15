package com.dahua.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

object ifFile {
  def ifFiles(outputPath:String,sc:SparkContext): Unit ={
    val config: Configuration = sc.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(config)
    val outpath = new Path(outputPath)
    if(fs.exists(outpath)){
      fs.delete(outpath,true)
    }
  }
}
