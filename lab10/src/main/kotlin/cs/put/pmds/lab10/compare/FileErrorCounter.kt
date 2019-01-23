package cs.put.pmds.lab10.compare

import com.carrotsearch.hppc.IntDoubleHashMap
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

class FileErrorCounter<T>(private val counter: (String, String) -> ErrorCounter<T>) {
    fun getStatsFromEach(in1: String, in2: String): T {
        require(File(in1).exists()) { "file 1 not exists" }
        require(File(in2).exists()) { "file 2 not exists" }
        val sorted1 = fetchUsers(in1)
        val sorted2 = fetchUsers(in2)
        require(sorted1.size == sorted2.size) { "file sizes are not equal" }
        val errorCounter = counter(in1, in2)

        sorted1.zip(sorted2)
                .parallelStream()
                .forEach { (u1, u2) ->
                    errorCounter.countError(u1, u2)
                }
        return errorCounter.result
    }

    private fun fetchUsers(in1: String): List<UserCompare> {
        val creator = UserCreator()
        Files.lines(Paths.get(in1)).forEach {
            val line = it.trim()
            if (line.startsWith("User")) {
                val userId = line.dropWhile { !it.isDigit() }.toInt()
                creator.createUser(userId)
            } else if (line.isNotEmpty()) {
                val (uid, value) = line.split("\\s+".toRegex())
                creator.addValue(uid.toInt(), value.toDouble())
            }
        }
        creator.finishUser()
        return creator.users.sortedBy { it.id }
    }
}

data class UserCompare(
        val id: Int,
        val coeff: IntDoubleHashMap = IntDoubleHashMap()
)

class UserCreator {
    val users = mutableListOf<UserCompare>()
    private var currentUser: UserCompare? = null
    fun createUser(id: Int) {
        finishUser()
        currentUser = UserCompare(id)
    }

    fun addValue(id: Int, value: Double) {
        requireNotNull(currentUser) { "current user is null" }.coeff.put(id, value)
    }

    fun finishUser() {
        currentUser?.also {
            users += it
        }
        currentUser = null
    }
}
