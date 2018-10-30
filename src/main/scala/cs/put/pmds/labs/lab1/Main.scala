package cs.put.pmds.labs.lab1

object Main extends App {
	private val tries = 10
	private val arg = ProvidedArgsSupplier.produce()
	private val statisticMaker = new TerroristsStatisticMaker()
	for (_ <- 0 until 10) {
		val finder = new TerroristFinder(new NightsCreator(arg).nights)
		val terrorists = finder.getPairNights()
		statisticMaker.add(terrorists)
	}

	HistogramMaker.make(statisticMaker.getStats(), arg)
}
