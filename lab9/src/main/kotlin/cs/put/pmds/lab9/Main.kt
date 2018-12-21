package cs.put.pmds.lab9

import com.carrotsearch.hppc.IntHashSet
import java.io.BufferedWriter
import java.io.File
import java.math.RoundingMode
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Stream
import kotlin.streams.toList
import kotlin.system.measureTimeMillis

fun main(args: Array<String>) {
    require(args.size == 2) { "usage: <input path> <output path>" }
    val (input, output) = args
    val file = File(input).also {
        require(it.exists()) { "file does not exist" }
    }
    val mem = Runtime.getRuntime().freeMemory()
    val measuredTime = measureTimeMillis {
        val userSongs = file.useLines { lines ->
            lines.drop(1)
                    .map(::mapLine)
                    .filter { it[0] != it[1] }
                    .groupBy { it.first() }
                    .mapValues {
                        val s = IntHashSet(it.value.size)
                        it.value.forEach { s.add(it[1]) }
                        s.toArray().apply { sort() }
                    }.toList()

        }
        println("used mem: ${mem - Runtime.getRuntime().freeMemory()} songs: ${userSongs.size}")
        val counter = AtomicInteger(0)
        val total = 10000L//userSongs.size.toLong()
        val step = total / 1000
        val progress = AtomicInteger(0)

        val neighbors = userSongs
                .parallelStream()
                .limit(total)
                .peek {
                    if (counter.incrementAndGet() % step == 0L) print("progress: ${progress.incrementAndGet() / 10.0}%\r")
                }
                .map { user -> user.first to findClosestNeighbours(userSongs, user) }

        File(output)
                .also { it.parentFile.mkdirs() }
                .outputStream()
                .bufferedWriter()
                .use { o -> o writeUserNeighbours neighbors }
    }

    println("total time: $measuredTime")

}

private infix fun BufferedWriter.writeUserNeighbours(neighbors: Stream<Pair<Int, List<Pair<Int, Double>>>>) {
    neighbors.forEach { (user, neigh) ->
        synchronized(this) {
            write("User = $user\n")
            neigh.forEach { (u, jac) ->
                write("\t$u ${jac.toBigDecimal().setScale(4, RoundingMode.HALF_UP)}\n")
            }
            newLine()
        }
    }
}

private fun findClosestNeighbours(userSongs: List<Pair<Int, IntArray>>, user: Pair<Int, IntArray>) =
        userSongs
                .stream()
                .map { other ->
                    if (user.first == other.first) other.first to 0.0
                    else other.first to jaccardCoef(user.second, other.second)
                }.filter { it.second > 0 }
                .limit(100)
                .toList()

private fun jaccardCoef(user: IntArray, other: IntArray): Double {
    if (user.first() > other.last() || user.last() < other.first()) return 0.0
    var common = 0
    var onlyUser = 0.0
    val userIterator = user.iterator()
    val otherIterator = other.iterator()
    var currentUser = userIterator.next()
    var currentOther = otherIterator.next()
    while (userIterator.hasNext() && otherIterator.hasNext()) {
        when {
            currentUser == currentOther -> {
                common++
                currentUser = userIterator.next()
                currentOther = otherIterator.next()
            }
            currentUser > currentOther -> currentOther = otherIterator.next()
            currentUser < currentOther -> {
                onlyUser++
                currentUser = userIterator.next()
            }
        }
    }
    return common / (other.size + onlyUser)
}

private fun mapLine(it: String) = it.split(",").map { it.trim().toInt() }