package cs.put.pmds.labs.lab1


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