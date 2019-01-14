package cs.put.pmds.lab9

import java.io.File
import kotlin.system.measureTimeMillis

fun main(args: Array<String>) {
    require(args.size == 2) { "usage: <input path> <output path>" }
    val (input, output) = args
    val file = File(input).also {
        require(it.exists()) { "file does not exist" }
    }
    val mem = Runtime.getRuntime().freeMemory()
    val measuredTime = measureTimeMillis {
        val userSongs = fetchUsers(file)
        println("used mem: ${mem - Runtime.getRuntime().freeMemory()} songs: ${userSongs.size}")
        val total = 100L//userSongs.size.toLong()
        countAndWriteCoefficient(total, userSongs, output) { u1, u2 ->
            jaccardCoef(u1.favourites, u2.favourites)
        }
    }
    println("total time: $measuredTime")
}