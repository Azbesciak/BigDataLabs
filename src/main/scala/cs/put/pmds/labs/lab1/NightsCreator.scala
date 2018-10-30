package cs.put.pmds.labs.lab1

import java.util.concurrent.ThreadLocalRandom._

class NightsCreator(private val programArgs: ProgramArgs) {
	private def getPersonNights(person: Person): Array[NightInTheHotel] = {
		days.withFilter(_ => current().nextDouble() <= programArgs.probabilityOfSleepingInHotel)
		 .map(d => NightInTheHotel(person, Night(hotels(current().nextInt(hotels.length - 1)), d)))
	}

	private val persons = (1 to programArgs.numOfPersons).map(Person).toArray
	private val hotels = (1 to programArgs.numOfHotels).map(Hotel).toArray
	private val days = (1 to programArgs.numOfDays).toArray
	val nights: Array[NightInTheHotel] = persons.par.flatMap(getPersonNights).toArray
}
