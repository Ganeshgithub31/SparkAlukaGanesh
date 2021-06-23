package SparkGIT

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object GITJun20Obj {
  
  def main(args:Array[String]):Unit={
			val conf=new SparkConf().setAppName("First").setMaster("local[*]")
					val sc=new SparkContext(conf)
					sc.setLogLevel("Error")

					val spark=SparkSession.builder().getOrCreate()
					import spark.implicits._
  
  
  val arraydata1=spark.read.format("json").option("multiLine","true")
					.load("file:///E:/Hadoop/Spark/Data/Complexdata/array1.json")

					arraydata1.show()
					arraydata1.printSchema()

					println
					println("======= Flaten data =======")

					val flatendf2=arraydata1.select("Students",
							"first_name",
							"second_name",
							"address.Permanent_address",
							"address.temporary_address")

					flatendf2.show()
					flatendf2.printSchema()

					val explodedf1=flatendf2.withColumn("Students", explode(col("Students")))

					println
					println("=====explode data =======")

					explodedf1.show()
					explodedf1.printSchema()

					val finalexplodedf1=explodedf1.select(
					    col("Students.user.address.Permanent_address").alias("stud_Permanent_address"),
					    col("Students.user.address.temporary_address").alias("stud_temporary_address"),
					    col("Students.user.gender").alias("stud_gender"),
					    col("Students.user.name.first").alias("stud_first_name"),
					    col("Students.user.name.last").alias("stud_last_name"),
					    col("Students.user.name.title").alias("stud_title"),
					    col("first_name"),
					    col("second_name"),
					    col("Permanent_address").alias("org_Permanent_address"),
					    col("temporary_address").alias("org_temporary_address"))
					    
					    println
					    println("========Final explode data =========")
							
						finalexplodedf1.show()
						finalexplodedf1.printSchema()
  
  }
}