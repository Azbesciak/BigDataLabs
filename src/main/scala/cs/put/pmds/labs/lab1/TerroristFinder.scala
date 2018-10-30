package cs.put.pmds.labs.lab1


class TerroristFinder(private val nights: Array[NightInTheHotel]) {
	private def isPairInTheSameNightAndHotel(n1: NightInTheHotel, n2: NightInTheHotel) =
		n1.person != n2.person && n1.night == n2.night

	def getPairNights() = {
		val tupleToNights = nights.par.zipWithIndex
		 .flatMap { case (n1, i) =>
			 nights.drop(i + 1)
				.filter(n2 => isPairInTheSameNightAndHotel(n1, n2))
				.map(n2 => (n1, n2))
		 }
		 .groupBy(pair)
		 .mapValues(distinctNights)
		val size = tupleToNights.toMap
		tupleToNights
		 .filter(p => p._2.length > 1)
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
