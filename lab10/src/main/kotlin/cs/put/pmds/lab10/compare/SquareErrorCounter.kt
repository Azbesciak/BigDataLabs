package cs.put.pmds.lab10.compare

import java.math.BigDecimal
import java.math.RoundingMode
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.pow
import kotlin.math.sqrt

class SquareErrorCounter : ErrorCounter<Result> {
    private val u1More = AtomicInteger()
    private val u2More = AtomicInteger()
    private val u2SimMore = AtomicInteger()
    private val u1SimMore = AtomicInteger()
    private val errors = CopyOnWriteArrayList<Double>()

    override val result
        get() = Result(
                sqrt(errors.average()).toBigDecimal().setScale(4, RoundingMode.HALF_UP),
                u1More.get(),
                u2More.get(),
                u1SimMore.get(),
                u2SimMore.get()
        )

    override fun countError(u1: UserCompare, u2: UserCompare) {
        require(u1.id == u2.id) { "users ids are not equal (${u1.id} vs ${u2.id})" }
        if (u1.coeff.size() > u2.coeff.size()) {
            u1More.incrementAndGet()
        } else if (u2.coeff.size() > u1.coeff.size()) {
            u2More.incrementAndGet()
        }
        val localErrors = u1.coeff.map { c1 ->
            val c2 = u2.coeff.getOrDefault(c1.key, 0.0)
            if (c1.value > c2) u1SimMore.incrementAndGet()
            else if (c2 > c1.value) u2SimMore.incrementAndGet()
            (c1.value - c2).pow(2)
        }
        errors.addAll(localErrors)
    }
}

data class Result(
        val squareAverageError: BigDecimal,
        val hashLongerList: Int,
        val jacLongerList: Int,
        val hashSimMore: Int,
        val jacSimMore: Int
)