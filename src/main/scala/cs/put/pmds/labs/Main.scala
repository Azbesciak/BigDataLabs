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
	hotel: Hotel,
	day: Int
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
		 .map(d => NightInTheHotel(person, hotels(random.nextInt(hotels.length - 1)), d))
	}

	private val programArgs = supplier.produce()
	private val persons = (1 to programArgs.numOfPersons).map(Person).toArray
	private val hotels = (1 to programArgs.numOfHotels).map(Hotel).toArray
	private val days = (1 to programArgs.numOfDays).toArray
	val nights: Array[NightInTheHotel] = persons.flatMap(getPersonNights _)
}

class NightsMatcher(private val nights: Array[NightInTheHotel]) {
	private def isPairInTheSameNightAndHotel(p: (NightInTheHotel, NightInTheHotel)) = {
		p._1.person != p._2.person &&
		 p._1.day == p._2.day &&
		 p._1.hotel == p._2.hotel
	}

	def findNightlyPairs() = {
		LazyList.from(nights)
		 .flatMap(n1 => LazyList.from(nights).map(n2 => (n1, n2)))
		 .filter(isPairInTheSameNightAndHotel)
		 .groupBy(p => (p._1.person, p._2.person))
		 .filter { case (_, v) => v.length > 1 }
		 .keys
	}
}

object Main extends App {
	private val nights = new NightsCreator(ProvidedArgsSupplier).nights
	private val value = new NightsMatcher(nights).findNightlyPairs()
	println(value)
}
