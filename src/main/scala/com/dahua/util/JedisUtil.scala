package com.dahua.util


import org.apache.commons.pool2.impl.{GenericKeyedObjectPoolConfig, GenericObjectPoolConfig}
import redis.clients.jedis.JedisPool


object JedisUtil {
   val jedisTool = new JedisPool(new GenericObjectPoolConfig, "192.168.137.51", 6379, 30000, null, 8)
   def resource = jedisTool.getResource

  def main(args: Array[String]): Unit = {
    println(resource)
  }
}
