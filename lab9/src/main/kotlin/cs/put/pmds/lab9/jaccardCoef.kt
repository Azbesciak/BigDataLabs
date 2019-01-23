package cs.put.pmds.lab9

import com.carrotsearch.hppc.IntArrayList
import com.carrotsearch.hppc.IntHashSet
import com.carrotsearch.hppc.IntObjectHashMap
import java.io.BufferedWriter
import java.io.File
import java.math.RoundingMode
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Stream
import kotlin.math.min
import kotlin.streams.toList
import kotlin.system.measureTimeMillis


fun countAndWriteCoefficient(total: Long, users: List<User>, output: String,
                             userNeighbours: (User) -> List<User>,
                             compute: (User, User) -> Double) {
    val counter = AtomicInteger(0)
    val step = total / min(total, 1000)
    val progress = AtomicInteger(0)
    val neighbors = users
            .parallelStream()
            .limit(total)
            .peek {
                if (counter.incrementAndGet() % step == 0L)
                    print("progress: ${progress.incrementAndGet() * step / total.toDouble() * 100}%\r")
            }
            .map { user -> user.id to findClosestNeighbours(userNeighbours(user), user, compute) }

    writeToFile(output, neighbors)
}

fun writeToFile(output: String, neighbors: Stream<Pair<Int, List<Pair<Int, Double>>>>) {
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

fun User.getNeighbours(songs: IntObjectHashMap<IntArrayList>, users: List<User>): List<User> {
    val neighbours = IntHashSet(favourites.size)
    favourites.forEach { song ->
        neighbours.addAll(songs[song])
    }
    return neighbours.map { users[it.value - 1] }
}

private fun findClosestNeighbours(userSongs: List<User>, user: User, compute: (User, User) -> Double) =
        userSongs
                .stream()
                .map { other ->
                    other.id to if (user.id == other.id) 1.0 else compute(user, other)
                }
                .filter { it.second > 0 }
                .toList()
                .sortedWith(compareBy({ -it.second }, { it.first }))
                .take(100)

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

data class Users(
        val users: List<User>,
        val songsListeners: IntObjectHashMap<IntArrayList>
)

fun fetchUsers(file: File): Users {
    val (users, usersTime) = fetchUsersFromFile(file)
    println("fetch time: $usersTime")
    val (songs, songsTime) = getSongsUsers(users)
    println("time of mapping: $songsTime")
    return Users(users, songs)
}

fun fetchUsersFromFile(file: File): Pair<List<User>, Long> {
    val start = System.currentTimeMillis()
    return file.useLines { lines ->
        lines.drop(1)
                .map(::mapLine)
                .filter { it[0] != it[1] }
                .groupBy { it.first() }
                .map { (id, values) ->
                    val s = IntHashSet(values.size)
                    values.forEach { s.add(it[1]) }
                    User(id, s.toArray().apply { sort() })
                }.toList()
    }.run { this to (System.currentTimeMillis() - start) }
}


private fun getSongsUsers(users: List<User>): Pair<IntObjectHashMap<IntArrayList>, Long> {
    val songs = IntObjectHashMap<IntArrayList>(2_000_000)
    val time = measureTimeMillis {
        users.forEach {
            it.favourites.forEach { f -> songs.computeIfAbsent(f) { IntArrayList() }.add(it.id) }
        }
    }
    return songs to time
}

inline fun <reified T> IntObjectHashMap<T>.computeIfAbsent(id: Int, f: IntObjectHashMap<T>.() -> T) =
        get(id) ?: run {
            val newRes = f()
            put(id, newRes)
            newRes
        }
