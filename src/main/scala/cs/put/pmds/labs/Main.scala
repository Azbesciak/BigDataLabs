package cs.put.pmds.labs

import java.util.concurrent.ThreadLocalRandom

import scala.collection.parallel.ParSeq
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
	override def produce() = ProgramArgs(10000, 0.01, 100, 1000)
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

class NightsCreator(private val programArgs: ProgramArgs) {
	private val random = ThreadLocalRandom.current()

	private def getPersonNights(person: Person): Array[NightInTheHotel] = {
		days.withFilter(_ => random.nextDouble() <= programArgs.probabilityOfSleepingInHotel)
		 .map(d => NightInTheHotel(person, Night(hotels(random.nextInt(hotels.length - 1)), d)))
	}

	private val persons = (1 to programArgs.numOfPersons).map(Person).toArray
	private val hotels = (1 to programArgs.numOfHotels).map(Hotel).toArray
	private val days = (1 to programArgs.numOfDays).toArray
	val nights: Array[NightInTheHotel] = persons.par.flatMap(getPersonNights).toArray
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
	private def isPairInTheSameNightAndHotel(n1: NightInTheHotel, n2: NightInTheHotel) =
		n1.person != n2.person && n1.night == n2.night

	def getPairNights() = {
		nights.toStream.par
		 .flatMap(n1 => nights.toStream
			.withFilter(n2 => isPairInTheSameNightAndHotel(n1, n2))
			.map(n2 => (n1, n2))
		 )
		 .groupBy(pair)
		 .mapValues(distinctNights)
		 .withFilter(p => p._2.length > 1)
		 .map(p => TerroristsPairs(p._1._1, p._1._2, p._2))
		 .toArray
	}

	private def distinctNights(v: ParSeq[(NightInTheHotel, NightInTheHotel)]) =
		v.map(_._1.night).distinct.toArray

	private def pair(p: (NightInTheHotel, NightInTheHotel)): (Person, Person) = {
		val p1 = p._1.person
		val p2 = p._2.person
		if (p1.id < p2.id) (p1, p2) else (p2, p1)
	}
}

object HistogramMaker {

	import plotly._
	import Plotly._

	def make(stats: TerroristsStats): Unit = {
		val (x, y) = stats.terroristsMeetingsCount.toSeq.unzip
		Bar(x, y).plot(
			title = "Pairs meetings to frequency",
			height = 950,
			width = 1900
		)
	}
}

case class TerroristsStats(
	terroristsMeetingsCount: Array[(Int, Double)],
	mediumNumberOfPotentialTerrorists: Double,
	mediumNumberOfPersonoNightPairs: Double
)


class TerroristsStatisticMaker {

	import Util._

	private var iterations = 0d
	private var stats = Map[Int, Int]()
	private var personoNightsPairs = 0

	def add(terroristsPairs: Array[TerroristsPairs]): Unit = {
		println(s"number of all potential pairs: ${terroristsPairs.length}")
		addPersonoNightPairStat(terroristsPairs)
		addOccurrenceOfPairMeetingStats(terroristsPairs)
		iterations += 1
	}

	private def addOccurrenceOfPairMeetingStats(terroristsPairs: Array[TerroristsPairs]): Unit = {
		val newOne = terroristsPairs.groupBy(_.meetings.length).mapValues(_.length)
		stats = (newOne.keySet ++ stats.keySet).map(k => (k, stats.getOrElse(k, 0) + newOne.getOrElse(k, 0))).toMap
	}

	private def addPersonoNightPairStat(terroristsPairs: Array[TerroristsPairs]) = {
		val nightsByPair = getTerroristsNightsPairs(terroristsPairs)
		val personoNightsPairsFromPair = nightsByPair.length
		println(s"number of terrorist pairs and nights pairs (persono-night-pair): $personoNightsPairsFromPair")
		personoNightsPairs += personoNightsPairsFromPair
	}

	def getStats(): TerroristsStats = {
		val mediumNumberOfTerrorists = stats.map(t => t._2 / iterations).sum
		println(s"medium number of potential pairs: $mediumNumberOfTerrorists")
		val personoNightPairsMediumNumber = personoNightsPairs / iterations
		println(s"medium number of persono-night-pairs: $personoNightPairsMediumNumber")
		TerroristsStats(
			stats.mapValues(_ / iterations).toArray,
			mediumNumberOfTerrorists,
			personoNightPairsMediumNumber
		)
	}

	def getTerroristsNightsPairs(pairNights: Array[TerroristsPairs]) =
		pairNights.par.flatMap(extractNightsPairs).toArray

	private def extractNightsPairs(p: TerroristsPairs) = {
		p.meetings.cross()
		 .map(sort)
		 .distinct
		 .map(n => NightlyPair(p.t1, p.t2, n._1, n._2))
	}

	private def sort(t: (Night, Night)): (Night, Night) =
		if (t._1.day < t._2.day) (t._1, t._2) else (t._2, t._1)
}

object Main extends App {
	private val tries = 10
	private val arg = ProvidedArgsSupplier.produce()
	private val statisticMaker = new TerroristsStatisticMaker()
	for (_ <- 0 until tries) {
		val finder = new TerroristFinder(new NightsCreator(arg).nights)
		val terrorists = finder.getPairNights()
		statisticMaker.add(terrorists)
	}
	HistogramMaker.make(statisticMaker.getStats())
}
