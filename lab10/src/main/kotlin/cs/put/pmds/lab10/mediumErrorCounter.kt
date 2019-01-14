package cs.put.pmds.lab10

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.math.abs

fun main(args: Array<String>) {
    require(args.size == 2) { "require 2 args: <file 1> <file 2>, got ${args.toList()}" }
    val (in1, in2) = args
    require(File(in1).exists()) { "file 1 not exists" }
    require(File(in2).exists()) { "file 2 not exists" }
    val sorted1 = fetchUsers(in1)
    val sorted2 = fetchUsers(in2)
    require(sorted1.size == sorted2.size) { "file sizes are not equal" }
    val stats = sorted1.zip(sorted2)
            .parallelStream()
            .flatMap { (u1, u2) ->
                require(u1.id == u2.id) { "users ids are not equal (${u1.id} vs ${u2.id})" }
                u1.coeff.zip(u2.coeff).stream()
            }
            .mapToDouble { (c1, c2) -> abs(c1.value - c2.value) }
            .summaryStatistics()

    println(stats)
}

private fun fetchUsers(in1: String): List<UserCompare> {
    val creator = UserCreator()
    Files.lines(Paths.get(in1)).forEach {
        val line = it.trim()
        if (line.startsWith("User")) {
            val userId = line.dropWhile { !it.isDigit() }.toInt()
            creator.createUser(userId)
        } else if (line.isNotEmpty()){
            val (uid, value) = line.split("\\s+".toRegex())
            creator.addValue(uid.toInt(), value.toDouble())
        }
    }
    creator.finishUser()
    return creator.users.sortedBy { it.id }
}

data class UserCompare(
        val id: Int,
        val coeff: MutableList<Coeff> = mutableListOf()
)

data class Coeff(val userId: Int, val value: Double)

class UserCreator {
    val users = mutableListOf<UserCompare>()
    private var currentUser: UserCompare? = null
    fun createUser(id: Int) {
        finishUser()
        currentUser = UserCompare(id)
    }

    fun addValue(id: Int, value: Double) {
        requireNotNull(currentUser) { "current user is null" }.coeff += Coeff(id, value)
    }

    fun finishUser() {
        currentUser?.also {
            it.coeff.sortWith(compareBy({ it.value }, { it.userId }))
            users += it
        }
        currentUser = null
    }
}

