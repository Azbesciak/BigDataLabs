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
	override def produce() = ProgramArgs(100, 0.5, 10, 100)
}

trait ProblemArgsSupplier {
	def produce(): ProgramArgs
}

case class TerroristsPairs(
	t1: Person,
	t2: Person,
	meetings: Array[Night]
) {
	override def toString: String = s"TerroristPairs($t1, $t2, ${meetings.mkString("[", ",", "]")}"
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
			 .flatMap(o1 => obj.map(o2 => (o1, o2)))
			 .filter(p => p._1 != p._2)
		}
	}
}

class TerroristFinder(private val nights: Array[NightInTheHotel]) {

	import Util._

	private def isPairInTheSameNightAndHotel(p: (NightInTheHotel, NightInTheHotel)) = {
		p._1.person != p._2.person && p._1.night == p._2.night
	}

	def getTerroristsNightsPairs(pairNights: Array[TerroristsPairs]) = {
		pairNights.flatMap(extractNightsPairs)
	}

	def getPairNights() = {
		nights.toStream
		 .flatMap(n1 => nights.toStream.map(n2 => (n1, n2)))
		 .filter(isPairInTheSameNightAndHotel)
		 .groupBy(pair)
		 .mapValues(distinctNights)
		 .filter(p => p._2.length > 1)
		 .toArray
		 .map(p => TerroristsPairs(p._1._1, p._1._2, p._2))
	}

	private def extractNightsPairs(p: TerroristsPairs) = {
		p.meetings.cross()
		 .map(sort)
		 .distinct
		 .map(n => NightlyPair(p.t1, p.t2, n._1, n._2))
	}

	private def sort(t: (Night, Night)): (Night, Night) = {
		if (t._1.day < t._2.day) (t._1, t._2) else (t._2, t._1)
	}

	private def distinctNights(v: Stream[(NightInTheHotel, NightInTheHotel)]) = {
		v.map(_._1.night).distinct.toArray
	}

	private def pair(p: (NightInTheHotel, NightInTheHotel)): (Person, Person) = {
		val p1 = p._1.person
		val p2 = p._2.person
		if (p1.id < p2.id) (p1, p2) else (p2, p1)
	}
}

class HistogramMaker(private val pairs: Array[TerroristsPairs]) {

	import plotly._
	import Plotly._

	def make(): Unit = {
		val results = pairs.groupBy(_.meetings.length).mapValues(_.length).toArray
		val (x, y) = results.toSeq.unzip
		Bar(x, y).plot(
			title = "Pairs meetings to frequency",
			height = 950,
			width = 1900
		)
	}
}

object Main extends App {

	private val finder = new TerroristFinder(new NightsCreator(ProvidedArgsSupplier).nights)
	private val terrorists = finder.getPairNights()
	println(s"number of all potential pairs: ${terrorists.length}")
	private val nightsByPair = finder.getTerroristsNightsPairs(terrorists)
	println(s"number of terrorist pairs and nights pairs (persono-night-pair): ${nightsByPair.length}")
	private val maker = new HistogramMaker(terrorists)
	maker.make()
}
