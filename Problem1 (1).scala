package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object Problem1 {

	def main(args: Array[String]) {
		val inputFile = args(0)
		val outputFolder = args(1)
		val k = args(2).toInt
		val conf = new SparkConf().setAppName("Problem1").setMaster("local")
		val sc = new SparkContext(conf)
		val textFile = sc.textFile(inputFile).map(_.toLowerCase.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+")) 
		//try to seperate the content of txt to follow the required 
		val newTextFile = textFile.map(_.filter(x => (x.length >0 && x.charAt(0) <= 'z' && x.charAt(0) >= 'a'))) 
		//filt the data
		val text_d = newTextFile.map(l => l.distinct)
		
		val temp_text = text_d.flatMap{ l =>
		for{ 
			word <-0 until l.length
		} yield {
				(l(word), 1)	
		}} 
		//try to utilize "reduceByKey"  to add the same key's value
		val reduce_text = temp_text.reduceByKey(_+_)
		//try to utilize "sortwith" into double sort
		val text_tmp = reduce_text.map(_.swap).sortByKey(false).collect().sortWith((x1,x2)=>(x1._1 >x2._1)||((x1._1==x2._1)&&(x1._2<x2._2))).map(x => (x._2 + "\t" + x._1)).take(k) 
		
	   //try to utilize  "makeRDD()" to make the Array[String] be the RDD
	  val finaltext_tmp = sc.makeRDD(text_tmp) 
		
	  finaltext_tmp.saveAsTextFile(outputFolder) 
	  //try to utilize "saveAsTextFile" to write the answer at the outputFolder
	}
}