package cs.put.pmds.labs.lab1

import plotly.Plotly._
import plotly._

object HistogramMaker {
	def make(stats: TerroristsStats, arg: ProgramArgs): Unit = {
		val (x, y) = stats.terroristsMeetingsCount.toSeq.unzip
		val (xt, yt) = (x.min.intValue() to x.max.intValue())
		 .map(v => (v, TheoreticStatisticMaker.getFor(arg, v).toDouble)).unzip
		Bar(x, y).plot(
			title = "Pairs meetings to frequency",
			height = 950,
			width = 1900
		)

		Bar(xt, yt).plot(
			title = "Theoretic meetings",
			height = 950,
			width = 1900
		)
	}
}