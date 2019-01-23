package cs.put.pmds.lab11

import com.carrotsearch.hppc.IntHashSet
import cs.put.pmds.lab10.MinHash
import cs.put.pmds.lab10.generateMinHashes
import cs.put.pmds.lab9.*
import java.io.File
import java.math.BigDecimal
import java.math.RoundingMode
import java.util.stream.Stream
import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.min
import kotlin.math.pow
import kotlin.streams.asStream
import kotlin.system.measureTimeMillis


fun main(args: Array<String>) {
    require(args.size == 2) { "usage: <input path> <output path>" }
    val (input, output) = args
    val file = File(input).also {
        require(it.exists()) { "file does not exist" }
    }

    val users = fetchUsersFromFile(file)
    compute(users) { (total, n, bucketSize, hash, th), usersMinHashes ->
        val thScaled = BigDecimal(th).setScale(2, RoundingMode.HALF_UP)
        println("compute lsh for total:$total n:$n rowsPerBucket:$bucketSize hash:$hash, threshold: $thScaled")
        val lshTime = measureTimeMillis {
            val lsh = LSH(n, bucketSize, hash)
            val neighbours = computeByLshNeighbours(lsh, users, usersMinHashes, total)
            val oName = formatName(output, "lsh_${n}_${bucketSize}_${hash}_$thScaled", total)
            writeToFile(oName, neighbours)
        }
        println("lsh $total $n $bucketSize TH:$thScaled time:$lshTime")
    }
}

private fun computeByLshNeighbours(lsh: LSH, users: List<User>, usersMinHashes: List<User>, total: Int): Stream<Pair<Int, List<Pair<Int, Double>>>> {
    val res = measure("lsh compute($total)") { lsh.compute(usersMinHashes) }
    val progressCounter = ProgressCounter(total.toLong())
    return res
            .map { it.key to it.value }
            .sortedBy { it.first }
            .take(total)
            .asSequence()
            .asStream()
            .parallel()
            .peek { progressCounter.increment() }
            .map { it.first to getUserNeighbours(users, it) }
}

private inline fun compute(users: List<User>, f: (LSHTry, List<User>) -> Unit) {
    return listOf(100, 10000).forEach { total ->
        listOf(200, 300).forEach { n ->
            val minHash = MinHash(n)
            val usersMinHashes = users.generateMinHashes(minHash)
            listOf(0.05,0.15)
                    .map { th -> computeBucketSize(n, th) }
                    .filter { it > 2 }
                    .distinct()
                    .forEach { bucket ->
                        val realThreshold = computeThreshold(n, bucket)
                        listOf(31).forEach { hash ->
                            f(LSHTry(total, n, bucket, hash, realThreshold), usersMinHashes)
                        }
                    }
        }
    }
}

fun computeThreshold(n: Int, rowsPerBucket: Int) =
        (rowsPerBucket.toDouble() / n).pow(1 / rowsPerBucket.toDouble())

data class LSHTry(
        val total: Int,
        val n: Int,
        val rowsPerBucket: Int,
        val hashInd: Int,
        val threshold: Double
)

fun computeBucketSize(n: Int, threshold: Double): Int {
    val proposed = min(ceil(ln(1.0 / n) / ln(threshold)).toInt(), n)
    return findClosestDivider(n, proposed)
}

private fun findClosestDivider(n: Int, current: Int) =
        generateSequence(0) { it + 1 }
                .flatMap { listOf(it, -it).asSequence() }
                .map { current + it }
                .filter { it <= n && n > 0 }
                .first { n % it == 0 }

private fun getUserNeighbours(users: List<User>, it: Pair<Int, IntHashSet>): List<Pair<Int, Double>> {
    val user = users[it.first - 1]
    return it.second.map {
        val other = users[it.value - 1]
        other.id to jaccardCoef(user.favourites, other.favourites)
    }.sortedByDescending { it.second }
            .filter { it.second > 0.0 }
            .take(100)
}

