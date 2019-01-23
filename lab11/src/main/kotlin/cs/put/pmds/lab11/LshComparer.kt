package cs.put.pmds.lab11

import cs.put.pmds.lab10.compare.Comparer

object LshComparer {
    @JvmStatic
    fun main(args: Array<String>) {
        Comparer(args) { _, f2 ->
            val data = f2.split("[_\\-]".toRegex())
            val n = data[1].toInt()
            val bucketSize = data[2].toInt()
            val th = data[4].toDouble()
            ThresholdCounter(n, bucketSize, th)
        }.compareResults()
    }
}
