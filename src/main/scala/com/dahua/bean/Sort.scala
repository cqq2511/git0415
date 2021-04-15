package com.dahua.bean

class Sort(
            val appid:String,
            val appname:String
          ) extends Product with Serializable {
  override def productElement(n: Int): Any = n match {
    case 0 => appid: String
    case 1 => appname: String
  }

  override def productArity: Int = 2

  override def canEqual(that: Any): Boolean = this.isInstanceOf[Sort]
}
object Sort{
  def apply(line: Array[String]): Sort = new Sort(line(0),line(1))
}
