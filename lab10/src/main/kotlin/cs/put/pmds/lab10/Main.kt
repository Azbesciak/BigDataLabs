package cs.put.pmds.lab10

import cs.put.pmds.lab9.countAndWriteCoefficient
import cs.put.pmds.lab9.fetchUsers
import cs.put.pmds.lab9.jaccardCoef
import java.io.File
import kotlin.streams.toList
import kotlin.system.measureTimeMillis

fun main(args: Array<String>) {
    require(args.size == 2) { "usage: <input path> <output path>" }
    val (input, output) = args
    val file = File(input).also {
        require(it.exists()) { "file does not exist" }
    }
    val jaccardOut = "$output.jac"
    val minHashOut = "$output.hash"
    val mem = Runtime.getRuntime().freeMemory()
    val measuredTime = measureTimeMillis {
        val users = fetchUsers(file)
        println("used mem: ${mem - Runtime.getRuntime().freeMemory()} songs: ${users.size}")
        val total = 100L//userSongs.size.toLong()
        val jaccardTime = measureTimeMillis {
            countAndWriteCoefficient(total, users, jaccardOut) { u1, u2 ->
                jaccardCoef(u1.favourites, u2.favourites)
            }
        }
        println("JaccardTime: $jaccardTime")
        val minHash = MinHash(100, 100)
        val minHashTime = measureTimeMillis {
        val usersSigs = users.parallelStream()
                .map { it.copy(favourites = minHash.signature(it.favourites)) }
                .toList()
            countAndWriteCoefficient(total, usersSigs, minHashOut) { u1, u2 ->
                minHash.similarity(u1.favourites, u2.favourites)
            }
        }
        println("minHash time: $minHashTime")
    }
    println("total time: $measuredTime")
}