package cs.put.pmds.lab7

import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.spark.{SparkConf, SparkContext}

object Utils {
	def getContext(name: String): SparkContext = {
		val conf = new SparkConf() setAppName name setMaster "local[*]" setAll Array(
			("spark.driver.host", "127.0.0.1"),
			("spark.local.ip", "127.0.0.1")
		)
		SparkContext getOrCreate conf
	}

	def main(args: Array[String]): Unit = {
		val m = LocalDateTime.ofInstant(Instant.ofEpochMilli(1203083335 * 1000L), ZoneId.of("UTC+1"))
		println(m)
	}
}
