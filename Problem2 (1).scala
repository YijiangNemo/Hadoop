package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object Problem2 {

	def main(args: Array[String]) {
		val inputFile = args(0)
		val outputFolder = args(1)
		val conf = new SparkConf().setAppName("Problem2").setMaster("local")
		val sc = new SparkContext(conf)
		val file_read = sc.textFile(inputFile)
		val temp = file_read.map(line => line.split("\t"))
		val temp_text = temp.map(line => (line(1)toInt,line(0)toInt))
	
		// try to utilize the "groupbykey" to add the adj
		val text_in_group = temp_text.groupByKey()
		val final_text = text_in_group.sortByKey().map(x => (x._1 + "\t" + x._2.toList.sorted.mkString(",")))
		//try to utilize the "saveAsTextFile" to write the answer at the outputFile
	
		final_text.saveAsTextFile(outputFolder) 
		
	}
}