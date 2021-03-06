package cs.put.pmds.lab6

import org.apache.spark.{SparkConf, SparkContext}

object Utils {
	def getContext(name: String): SparkContext = {
		val conf = new SparkConf() setAppName name setMaster "local"
		SparkContext getOrCreate conf
	}
}
