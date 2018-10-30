package cs.put.pmds.labs.lab1


case class TerroristsStats(
	terroristsMeetingsCount: Array[(Int, Double)],
	mediumNumberOfPotentialTerrorists: Double,
	mediumNumberOfPersonoNightPairs: Double
)

object TheoreticStatisticMaker {
	def getFor(args: ProgramArgs, nrOfNights: Int): BigDecimal = {
		val pairInSameHotelAndNight = (BigDecimal(args.probabilityOfSleepingInHotel) pow 2) / args.numOfHotels
		val propNrOf = pairInSameHotelAndNight pow nrOfNights
		val nrOfPossiblePersonPairs = math.pow(args.numOfPersons, 2) / 2
		val nrOfPossibleNightsPairs = math.pow(args.numOfDays, 2) / 2
		propNrOf * nrOfPossibleNightsPairs * nrOfPossiblePersonPairs
	}
}
