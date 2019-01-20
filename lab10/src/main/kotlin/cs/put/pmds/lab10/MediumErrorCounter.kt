package cs.put.pmds.lab10

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.abs
import kotlin.math.pow
import kotlin.math.sqrt

object MediumErrorCounter {
    @JvmStatic
    fun main(args: Array<String>) {
        require(args.size == 2) { "require 2 args: <file 1> <file 2> <output>, got ${args.toList()}" }
        val (in1, in2) = args
        val stats = getStatsFromEach(in1, in2)
        println(stats)
    }

    fun getStatsFromEach(in1: String, in2: String): Result {
        require(File(in1).exists()) { "file 1 not exists" }
        require(File(in2).exists()) { "file 2 not exists" }
        val sorted1 = fetchUsers(in1)
        val sorted2 = fetchUsers(in2)
        require(sorted1.size == sorted2.size) { "file sizes are not equal" }
        val u1More = AtomicInteger()
        val u2More = AtomicInteger()
        val u2SimMore = AtomicInteger()
        val u1SimMore = AtomicInteger()

        val squareError = sorted1.zip(sorted2)
                .parallelStream()
                .flatMap { (u1, u2) ->
                    require(u1.id == u2.id) { "users ids are not equal (${u1.id} vs ${u2.id})" }
                    if (u1.coeff.size > u2.coeff.size) {
                        u1More.incrementAndGet()
                    } else if (u2.coeff.size > u1.coeff.size) {
                        u2More.incrementAndGet()
                    }
                    u1.coeff.zip(u2.coeff).stream()
                }
                .mapToDouble { (c1, c2) ->
                    if (c1.value > c2.value) u1SimMore.incrementAndGet()
                    else if (c2.value > c1.value) u2SimMore.incrementAndGet()
                    (c1.value - c2.value).pow(2)
                }
                .average()
                .let { sqrt(it.asDouble) }
        return Result(
                squareError,
                u1More.get(),
                u2More.get(),
                u1SimMore.get(),
                u2SimMore.get()
        )
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

data class Result(
        val squareAverageError: Double,
        val hashLongerList: Int,
        val jacLongerList: Int,
        val hashSimMore: Int,
        val jacSimMore: Int
)