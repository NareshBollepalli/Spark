package com.bigdata.spark.sparkcore
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
  import org.apache.spark.sql._
  object wordcount{
  def main(args: Array[String]) {
  //val spark = SparkSession.builder.master("local[*]").appName("${NAME}").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
  val spark = SparkSession.builder.master("local[*]").appName("wordcount").getOrCreate()
  val sc = spark.sparkContext
  val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
  val sqlContext = spark.sqlContext
  import spark.implicits._
  import spark.sql
    val Filerdd = sc.textFile("C:\\Users\\Vbollepalli\\Desktop\\Training\\Datasets\\SalesByCity.csv")
    val Filerdd1 = Filerdd.flatMap(x=>x.split(",")).map(word=>(word,1)).reduceByKey(_+_)
   Filerdd1.collect().foreach(println)
    spark.stop()
}
}

