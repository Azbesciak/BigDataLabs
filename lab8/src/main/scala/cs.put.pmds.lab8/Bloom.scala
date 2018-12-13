package cs.put.pmds.lab8

import scala.util.Random


object Bloom extends App {
	Stream(
		Params(n = 10000, m = 100000, k = 1, range = 100000000),
		Params(n = 10000, m = 100000, k = 2, range = 100000000),
		Params(n = 10000, m = 100000, k = 3, range = 100000000),
		Params(n = 10000, m = 100000, k = 4, range = 100000000),
		Params(n = 10000, m = 100000, k = 5, range = 100000000),
		Params(n = 10000, m = 100000, k = 6, range = 100000000),
		Params(n = 10000, m = 100000, k = 7, range = 100000000),
		Params(n = 10000, m = 100000, k = 8, range = 100000000)
	).map(count)
	 .foreach(println)

	def count(param: Params): Result = {
		val Params(n, m, k, range) = param
		val random = new Random(123)
		val set = HashFilter(m)
		val bf = BloomFilter(m, k)
		for (_ <- 0 until n) {
			val value = random nextInt range
			set add value
			bf add value
		}
		var TP = 0
		var FP = 0
		var TN = 0
		var FN = 0
		for (key <- 0 until range) {
			val containsBF = bf contains key
			val containsHS = set contains key

			if (containsBF && containsHS) TP += 1
			else if (!containsBF && !containsHS) TN += 1
			else if (!containsBF && containsHS) FN += 1
			else if (containsBF && !containsHS) FP += 1
		}
		Result(param, TP, TN, FP, FN)
	}
}

case class Params(n: Int, m: Int, k: Int, range: Int) {
	override def toString: String = s"n: $n, m: $m, k: $k, range: $range"
}

case class Result(
	params: Params,
	tp: Int, tn: Int,
	fp: Int, fn: Int
) {
	val expectedFP: Double = math.pow(1 - math.exp(-(params.k * params.n) / params.m.toDouble), params.k)

	override def toString: String =
		f"""
			 |$params
			 |TPR = ${tp / params.n.toDouble}%1.4f  TNR = ${tn / (params.range - params.n).toDouble}%1.4f
			 |FPR = ${fp / (params.range - params.n).toDouble}%1.4f  FNR = ${fn / params.n.toDouble}%1.4f
			 |Expected FP = $expectedFP%1.4f
		""".stripMargin
}

trait Filter {
	def add(key: Int)

	def contains(key: Int): Boolean
}

case class HashFilter(size: Int) extends Filter {
	private val map = new collection.mutable.HashSet[Int]()

	def add(key: Int): Unit = {
		map.add(key)
	}

	def contains(key: Int): Boolean = map contains key
}

case class BloomFilter(m: Int, k: Int) extends Filter {
	private val prime = PrimeFinder(m + 1)
	private val hashFunctions = HashFunctionsGenerator(m, k)

	private val hashesRepo = Array.fill[Boolean](m)(false)

	override def add(key: Int): Unit = {
		generateHashes(key)
		 .foreach(v => hashesRepo(v) = true)
	}

	private def generateHashes(key: Int) =
		hashFunctions
		 .map(f => (f(key) % prime % m).toInt.abs)

	override def contains(key: Int): Boolean = generateHashes(key).forall(v => hashesRepo(v))
}

case class IntHashFunction(a: Int, b: Int) {
	def apply(value: Int): Int = a * value + b
}

object HashFunctionsGenerator {
	private val r = new Random(1234)

	def apply(m: Int, k: Int) = (1 to k)
	 .map(_ => IntHashFunction(r nextInt m, r nextInt m))
	 .toArray
}

object PrimeFinder {
	def apply(start: Long): Long = {
		Stream.iterate(start)(_ + 1)
		 .filter(current => isPrime(current))
		 .head
	}

	private def isPrime(current: Long): Boolean = {
		if (current == 2) return true
		if (current % 2 == 0) return false
		val limit = math.sqrt(current).toInt - 1
		if (limit < 0) return false
		Stream
		 .iterate(2)(_ + 1)
		 .take(limit)
		 .forall(current % _ != 0)
	}
}

object Test extends App {
	val p3 = PrimeFinder(2)
	val p5 = PrimeFinder(3)
	val p7 = PrimeFinder(6)
	val p41 = PrimeFinder(41)
	println(s"$p3 $p5 $p7 $p41")
}