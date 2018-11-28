package cs.put.pmds.lab7

import scala.util.Random

object Statistic extends App {

	case class Aggregator(sum: BigDecimal, squareSum: BigDecimal, count: Long) {
		def +(other: Aggregator) = Aggregator(sum + other.sum, squareSum + other.squareSum, count + other.count)
		def stats = {
			val avg = sum / count
			val avgSquare = squareSum / count
			val variance = avgSquare - avg * avg
			Stats(avg, variance, count)
		}
	}

	case class Stats(avg: BigDecimal, v: BigDecimal, count: Long) {
		override def toString: String = s"avg = $avg, variance = $v, count = $count"
	}

	val groups = Map(
		0 -> (0, 1),
		1 -> (1, 1),
		2 -> (2, 2),
		3 -> (3, 3),
		4 -> (4, 3),
		5 -> (5, 2),
		6 -> (6, 1)
	)
	val n = 1000000
	val random = Random
	val sc = Utils.getContext("Stats")
	val start = System.currentTimeMillis()
	for (_ <- 1 to 2) {
		val cachedGroupedValues = sc
		 .parallelize(for (_ <- 1 to n) yield {
			 val g = random.nextInt(7)
			 val (mu, sigma) = groups(g)
			 (g, BigDecimal(mu + sigma * random.nextGaussian()))
		 })
		 .map { case (i, g) => (i, Aggregator(g, g * g, 1)) }
		 .reduceByKey { case (a1, a2) => a1 + a2 }
		 .cache()
		val total = cachedGroupedValues
 		 .map(_._2)
		 .reduce { case (a1, a2) => a1 + a2 }

		cachedGroupedValues
		 .mapValues(_.stats)
		 .collect().foreach(println)
		println(s"total: ${total.stats}")
	}
	println(s"total time: ${System.currentTimeMillis() - start} ms")
}
