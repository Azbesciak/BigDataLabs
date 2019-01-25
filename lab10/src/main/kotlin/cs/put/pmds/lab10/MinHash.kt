package cs.put.pmds.lab10

import java.util.Random
import kotlin.math.min

data class HashFunction(val a: Long, val b: Long, val mod: Int = LARGE_PRIME) {
    companion object {
        private const val LARGE_PRIME = 1299827
        infix fun create(r: Random) = HashFunction(nextVal(r), nextVal(r))
        private fun nextVal(r: Random) = (r.nextInt(LARGE_PRIME - 1) + 1).toLong()
    }

    operator fun get(x: Int) = ((a * x.toLong() + b) % mod).toInt()
}

class MinHash(private val hashFunctions: List<HashFunction>) {
    init {
        require(hashFunctions.isNotEmpty()) {"expected at least one hash function"}
    }
    constructor(n: Int, random: Random = Random()): this((0 until n).map { HashFunction create random }) {
        require(n > 0) { "Signature size should be positive" }
    }

    fun signature(values: IntArray): IntArray {
        val sig = IntArray(hashFunctions.size) { Int.MAX_VALUE }
        values.forEach { value ->
            sig.forEachIndexed { si, sval ->
                sig[si] = min(sval, hashFunctions[si][value])
            }
        }
        return sig
    }

    companion object {
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
    }
}
