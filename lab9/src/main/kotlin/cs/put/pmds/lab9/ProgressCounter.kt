package cs.put.pmds.lab9

import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min

class ProgressCounter(val total: Long) {
    private val counter = AtomicInteger(0)
    private val step = total / min(total, 1000)
    private val progress = AtomicInteger(0)
    fun increment() {
        if (counter.incrementAndGet() % step == 0L)
            print("progress: ${progress.incrementAndGet() * step / total.toDouble() * 100}%\r")
    }
}