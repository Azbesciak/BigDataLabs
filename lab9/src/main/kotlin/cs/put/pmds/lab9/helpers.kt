package cs.put.pmds.lab9

import com.carrotsearch.hppc.IntArrayList
import com.carrotsearch.hppc.IntHashSet
import com.carrotsearch.hppc.IntObjectHashMap
import java.io.BufferedWriter
import java.io.File
import java.math.RoundingMode
import java.util.stream.Stream


inline fun <reified T> measure(measured: String, f: () -> T): T {
    val start = System.currentTimeMillis()
    val res = f()
    println("$measured: ${System.currentTimeMillis() - start}")
    return res
}


fun writeToFile(output: String, neighbors: Stream<Pair<Int, List<Pair<Int, Double>>>>) {
    File(output)
            .also { it.parentFile.mkdirs() }
            .outputStream()
            .bufferedWriter()
            .use { o -> o writeUserNeighbours neighbors }
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

data class Users(
        val users: List<User>,
        val songsListeners: IntObjectHashMap<IntArrayList>
)

fun fetchUsers(file: File): Users {
    val users = fetchUsersFromFile(file)
    val songs = measure("time of mapping") { getSongsUsers(users) }
    return Users(users, songs)
}

fun formatName(base: String, algo: String, total: Int) = File(base).run {
    "${parentFile.path}/$algo-$total-$name"
}

fun fetchUsersFromFile(file: File) = measure("fetchTime") {
    file.useLines { lines ->
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
}

private fun getSongsUsers(users: List<User>): IntObjectHashMap<IntArrayList> {
    val songs = IntObjectHashMap<IntArrayList>(2_000_000)
    users.forEach {
        it.favourites.forEach { f -> songs.computeIfAbsent(f) { IntArrayList() }.add(it.id) }
    }
    return songs
}

inline fun <reified T> IntObjectHashMap<T>.computeIfAbsent(id: Int, f: IntObjectHashMap<T>.() -> T) =
        get(id) ?: run {
            val newRes = f()
            put(id, newRes)
            newRes
        }

private fun mapLine(it: String) = it.split(",").map { it.trim().toInt() }
