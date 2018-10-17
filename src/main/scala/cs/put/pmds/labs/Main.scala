package cs.put.pmds.labs

import java.util.concurrent.ThreadLocalRandom

import scala.io.StdIn

case class ProgramArgs(
	numOfPersons: Int,
	probabilityOfSleepingInHotel: Double,
	numOfHotels: Int,
	numOfDays: Int
)

case class Person(
	id: Int
)

case class Hotel(id: Int)

case class NightInTheHotel(
	person: Person,
	night: Night
)

case class Night(
	hotel: Hotel,
	day: Int
)

case class NightlyPair(
	p1: Person,
	p2: Person,
	n1: Night,
	n2: Night
)

object CmdArgsSupplier extends ProblemArgsSupplier {
	def produce(): ProgramArgs = {
		println("num of persons:")
		val numOfPersons = StdIn.readInt()
		println("probability of sleeping in hotel:")
		val probabilityOfSleepingInHotel = StdIn.readDouble()
		println("num of hotels:")
		val numOfHotels = StdIn.readInt()
		println("num of days:")
		val numOfDays = StdIn.readInt()
		ProgramArgs(numOfPersons, probabilityOfSleepingInHotel, numOfHotels, numOfDays)
	}
}

object ProvidedArgsSupplier extends ProblemArgsSupplier {
	override def produce() = ProgramArgs(100, 0.1, 10, 100)
}

trait ProblemArgsSupplier {
	def produce(): ProgramArgs
}

class NightsCreator(private val supplier: ProblemArgsSupplier) {
	private def getPersonNights(person: Person): Array[NightInTheHotel] = {
		val random = ThreadLocalRandom.current()
		days.withFilter(_ => random.nextDouble() <= programArgs.probabilityOfSleepingInHotel)
		 .map(d => NightInTheHotel(person, Night(hotels(random.nextInt(hotels.length - 1)), d)))
	}

	private val programArgs = supplier.produce()
	private val persons = (1 to programArgs.numOfPersons).map(Person).toArray
	private val hotels = (1 to programArgs.numOfHotels).map(Hotel).toArray
	private val days = (1 to programArgs.numOfDays).toArray
	val nights: Array[NightInTheHotel] = persons.flatMap(getPersonNights _)
}

object Util {

	implicit class Col[T](obj: Array[T]) {
		def cross(): Array[(T, T)] = {
			obj
			 .map(o1 => obj.map(o2 => (o1, o2)))
			 .flatten
			 .filter(p => p._1 != p._2)
		}
	}

}

class TerroristFinder(private val nights: Array[NightInTheHotel]) {

	import Util._

	private def isPairInTheSameNightAndHotel(p: (NightInTheHotel, NightInTheHotel)) = {
		p._1.person != p._2.person && p._1.night == p._2.night
	}

	def find() = {
		LazyList.from(nights)
		 .flatMap(n1 => LazyList.from(nights).map(n2 => (n1, n2)))
		 .filter(isPairInTheSameNightAndHotel)
		 .groupBy(pair)
		 .view
		 .mapValues(distinctNights)
		 .filter(p => p._2.length > 1)
		 .map(extractNightsPairs)
		 .toArray
	}

	private def extractNightsPairs(p: ((Person, Person), Array[Night])) = {
		(p._1, p._2.cross().distinctBy(n => Set(n._1, n._2)))
	}

	private def distinctNights(v: LazyList[(NightInTheHotel, NightInTheHotel)]) = {
		v.map(_._1.night).distinct.toArray
	}

	private def pair(p: (NightInTheHotel, NightInTheHotel)): (Person, Person) = {
		val p1 = p._1.person
		val p2 = p._2.person
		if (p1.id < p2.id) (p1, p2) else (p2, p1)
	}
}

object Main extends App {
	private val nights = new NightsCreator(ProvidedArgsSupplier).nights
	private val terrorists = new TerroristFinder(nights).find()
	println(s"number of pairs: ${terrorists.length}")
	private val nightPairs = terrorists.map(p => p._2.map(n => NightlyPair(p._1._1, p._1._2, n._1, n._2))).flatten
	println(s"number of all pairs combinations: ${nightPairs.length}")

}
