package cs.put.pmds.lab9

import com.carrotsearch.hppc.IntHashSet
import java.io.BufferedWriter
import java.io.File
import java.math.RoundingMode
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Stream
import kotlin.math.min
import kotlin.streams.toList


fun countAndWriteCoefficient(total: Long, userSongs: List<User>, output: String, compute: (User, User) -> Double) {
    val counter = AtomicInteger(0)
    val step = total / min(total, 1000)
    val progress = AtomicInteger(0)
    val neighbors = userSongs
            .parallelStream()
            .limit(total)
            .peek {
                if (counter.incrementAndGet() % step == 0L)
                    print("progress: ${progress.incrementAndGet() * step / total.toDouble() * 100}%\r")
            }
            .map { user -> user.id to findClosestNeighbours(userSongs, user, compute) }

    File(output)
            .also { it.parentFile.mkdirs() }
            .outputStream()
            .bufferedWriter()
            .use { o -> o writeUserNeighbours neighbors }
}

fun jaccardCoef(user: IntArray, other: IntArray): Double {
    if (user.first() > other.last() || user.last() < other.first()) return 0.0
    var common = 0
    val userIterator = user.iterator()
    val otherIterator = other.iterator()
    var currentUser = userIterator.next()
    var currentOther = otherIterator.next()
    loop@ while (true) {
        when {
            currentUser == currentOther -> {
                common++
                if (!userIterator.hasNext() || !otherIterator.hasNext()) break@loop
                currentUser = userIterator.next()
                currentOther = otherIterator.next()
            }
            currentUser > currentOther -> {
                if (!otherIterator.hasNext()) break@loop
                currentOther = otherIterator.next()
            }
            currentUser < currentOther -> {
                if (!userIterator.hasNext()) break@loop
                currentUser = userIterator.next()
            }
        }
    }
    return common.toDouble() / (other.size + user.size - common)
}

private fun findClosestNeighbours(userSongs: List<User>, user: User, compute: (User, User) -> Double) =
        userSongs
                .stream()
                .map { other ->
                    other.id to if (user.id == other.id) 1.0 else compute(user, other)
                }
                .filter { it.second > 0 }
                .sorted { o1, o2 ->
                    val dif = o1.second - o2.second
                    when {
                        dif < 0 -> 1
                        dif > 0 -> -1
                        else -> 0
                    }
                }
                .limit(100)
                .toList()

data class User(val id: Int, val favourites: IntArray) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as User

        if (id != other.id) return false
        if (!favourites.contentEquals(other.favourites)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id
        result = 31 * result + favourites.contentHashCode()
        return result
    }
}

private infix fun BufferedWriter.writeUserNeighbours(neighbors: Stream<Pair<Int, List<Pair<Int, Double>>>>) {
    neighbors.forEach { (user, neigh) ->
        synchronized(this) {
            write("User = $user\n")
            neigh.forEach { (u, jac) ->
                write("\t$u ${jac.toBigDecimal().setScale(5, RoundingMode.HALF_UP)}\n")
            }
            newLine()
        }
    }
}

private fun mapLine(it: String) = it.split(",").map { it.trim().toInt() }

fun fetchUsers(file: File) = file.useLines { lines ->
    lines.drop(1)
            .map(::mapLine)
            .filter { it[0] != it[1] }
            .groupBy { it.first() }
            .map { (id, values) ->
                val s = IntHashSet(values.size)
                values.forEach { s.add(it[1]) }
                User(id, s.toArray().apply { sort() })
            }.toList()
}
