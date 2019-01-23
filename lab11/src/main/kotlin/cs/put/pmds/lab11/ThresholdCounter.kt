package cs.put.pmds.lab11

import cs.put.pmds.lab10.compare.ErrorCounter
import cs.put.pmds.lab10.compare.UserCompare
import java.math.BigDecimal
import java.math.RoundingMode
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong

class ThresholdCounter(
        private val n: Int,
        private val bucketSize: Int,
        private val threshold: Double
) : ErrorCounter<ThresholdResult> {
    private val original = AtomicLong(0)
    private val lsh = AtomicLong(0)
    private val allLsh = AtomicLong(0)
    private val users = AtomicLong(0)
    private val bellowThreshold = CopyOnWriteArrayList<Double>()

    override fun countError(u1: UserCompare, u2: UserCompare) {
        val u1Above = u1.aboveTh()
        val u2Above = u2.aboveTh()
        val bellowTh = u2.coeff.map { it.value }.filter { it < threshold }
        bellowThreshold.addAll(bellowTh)
        allLsh.addAndGet(u2.coeff.size().toLong())
        original.addAndGet(u1Above.size.toLong())
        lsh.addAndGet(u2Above.size.toLong())
        users.incrementAndGet()
    }

    private fun UserCompare.aboveTh(): List<Double> {
        val res = mutableListOf<Double>()
        coeff.forEach {
            if (it.value >= threshold)
                res += it.value
        }
        return res
    }

    override val result: ThresholdResult
        get() = ThresholdResult(
                n, bucketSize,
                threshold.toBigDecimal().round(),
                BigDecimal.ONE - (lsh.get().toBigDecimal() / original.get().toBigDecimal()).round(),
                (bellowThreshold.size.toBigDecimal() / users.get().toBigDecimal()).round(),
                getAverageBellowThreshold()
        )

    private fun getAverageBellowThreshold() =
            if (bellowThreshold.isEmpty())
                BigDecimal.ZERO
            else
                bellowThreshold.map { threshold - it }.average().toBigDecimal().round()

    private fun BigDecimal.round() = setScale(2, RoundingMode.HALF_UP)

}

data class ThresholdResult(
        val n: Int,
        val bucketSize: Int,
        val threshold: BigDecimal,
        val missingPercentage: BigDecimal,
        val bellowThresholdAvgCount: BigDecimal,
        val stdBellowThreshold: BigDecimal
)