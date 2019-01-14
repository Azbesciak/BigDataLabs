package cs.put.pmds.lab10

import java.util.Random
import kotlin.math.min
import kotlin.math.roundToInt
import kotlin.math.sqrt

data class HashFunction(val a: Long, val b: Long) {
    companion object {
        private const val LARGE_PRIME = Int.MAX_VALUE
        infix fun create(r: Random) = HashFunction(nextVal(r), nextVal(r))
        private fun nextVal(r: Random) = (r.nextInt(LARGE_PRIME - 1) + 1).toLong()
    }

    operator fun get(x: Int) = ((a * x.toLong() + b) % LARGE_PRIME).toInt()
}

class MinHash(val n: Int, dictSize: Int, r: Random = Random()) {
    init {
        require(n > 0) { "Signature size should be positive" }
        require(dictSize > 0) { "Dictionary size (or vector size) should be positive" }

        // In function h(i, x) the largest value could be
        // dictSize * dictSize + dictSize
        // throw an error if dictSize * dictSize + dictSize > Long.MAX_VALUE
        require(dictSize <= (Long.MAX_VALUE - dictSize) / dictSize) {
            "Dictionary size (or vector size) is too big and will cause a multiplication overflow"
        }
    }

    private val hashFunctions = (0 until n).map { HashFunction create r }

    fun signature(values: IntArray): IntArray {
        val sig = IntArray(n).apply { fill(Int.MAX_VALUE) }
        values.forEach { value ->
            sig.forEachIndexed { si, sval ->
                sig[si] = min(sval, hashFunctions[si][value])
            }
        }
        return sig
    }

    /**
     * Computes an estimation of Jaccard similarity (the number of elements in
     * common) between two sets, using the MinHash signatures of these two sets.
     *
     * @param sig1 MinHash signature of set1
     * @param sig2 MinHash signature of set2 (produced using the same
     * hashFunctions)
     * @return the estimated similarity
     */
    fun similarity(sig1: IntArray, sig2: IntArray): Double {
        require(sig1.size == sig2.size) { "Size of signatures should be the same" }
        var sim = 0.0
        for (i in sig1.indices) {
            if (sig1[i] == sig2[i]) {
                sim += 1.0
            }
        }

        return sim / sig1.size
    }

    /**
     * Computes the expected error of similarity computed using signatures.
     *
     * @return the expected error
     */
    fun error() = 1.0 / sqrt(n.toDouble())

    companion object {

        /**
         * Computes the size of the signature required to achieve a given error in
         * similarity estimation. (1 / error^2)
         *
         * @param error
         * @return size of the signature
         */
        fun size(error: Double): Int {
            require(error in 0.0..1.0) { "error should be in [0 .. 1]" }
            return (1 / (error * error)).roundToInt()
        }
    }
}
