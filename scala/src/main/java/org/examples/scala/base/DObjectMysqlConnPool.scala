package org.examples.scala.base

import java.sql.{Connection, DriverManager}
import scala.collection.mutable.ListBuffer


// 单例对象的常量使用方式
// 通过海牛配置了一次性mysql数据库
// mysql -u root -p
// create database test;
// use test;
// create table tbl_user_info ( id varchar(10), name varchar(100) );
// insert into tbl_user_info values("1", "tecwang");
object Constant {
  val user = "root"
  val pass = "hainiu"
  val url = "jdbc:mysql://11.138.24.176:3306/test"
}

// 单例对象先加载，并且只加载一次
// 因此可以考虑自己创建一个mysql连接池
object MysqlConnectUtil {

  val n_conn = 3
  val conn_list:ListBuffer[Connection] = ListBuffer[Connection]()
  // 连接数据库
  for (i <- 1 to n_conn) {
    conn_list.append(DriverManager.getConnection(Constant.url, Constant.user, Constant.pass))
  }
  println(conn_list)

  def getConn: Connection = {
    if(conn_list.nonEmpty) conn_list.remove(0)
    else {
      println("connection pool hss no more connections !!! ")
      null
    }
  }

  def returnConn(conn:Connection): Any = {
    if (conn == null) {
      println("connection is invalid")
      null
    } else {
      this.conn_list.append(conn)
    }
  }
}

object DObjectMysqlConnPool {
  def main(args: Array[String]): Unit = {
    // 测试conn
    val conn1 = MysqlConnectUtil.getConn
    val conn2 = MysqlConnectUtil.getConn
    val conn3 = MysqlConnectUtil.getConn
    val conn4 = MysqlConnectUtil.getConn
    println(conn1)
    println(conn2)
    println(conn3)
    println(conn4)
    MysqlConnectUtil.returnConn(conn1)
    MysqlConnectUtil.returnConn(conn2)
    MysqlConnectUtil.returnConn(conn3)
    MysqlConnectUtil.returnConn(conn4)

  }
}