package cs.put.pmds.labs.lab1



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

case class TerroristsPairs(
	t1: Person,
	t2: Person,
	meetings: Array[Night]
) {
	override def toString: String = s"TerroristPairs($t1, $t2, ${meetings.mkString("[", ",", "]")}"
}
