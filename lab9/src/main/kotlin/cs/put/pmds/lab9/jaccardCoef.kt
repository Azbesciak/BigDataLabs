package cs.put.pmds.lab9

import com.carrotsearch.hppc.IntArrayList
import com.carrotsearch.hppc.IntHashSet
import com.carrotsearch.hppc.IntObjectHashMap
import kotlin.streams.toList


fun countAndWriteCoefficient(
        total: Long, users: List<User>, output: String,
        userNeighbours: (User) -> List<User>,
        compute: (User, User) -> Double
) {
    val progress = ProgressCounter(total)
    val neighbors = users
            .parallelStream()
            .limit(total)
            .peek { progress.increment() }
            .map { user -> user.id to findClosestNeighbours(userNeighbours(user), user, compute) }

    writeToFile(output, neighbors)
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