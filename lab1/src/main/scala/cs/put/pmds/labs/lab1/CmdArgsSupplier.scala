package cs.put.pmds.labs.lab1

import scala.io.StdIn


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