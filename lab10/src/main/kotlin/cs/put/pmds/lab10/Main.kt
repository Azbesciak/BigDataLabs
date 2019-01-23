package cs.put.pmds.lab10

import cs.put.pmds.lab9.*
import java.io.File
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
        val (users, songsUsers) = fetchUsers(file)
        println("used mem: ${mem - Runtime.getRuntime().freeMemory()} songs: ${users.size}")
        listOf(100, 200, 300, 400, 500).forEach { total ->
            //        listOf(100).forEach { total ->
            val jaccardTime = measureTimeMillis {
                val outName = formatName(output, "jac", total)
                countAndWriteCoefficient(total.toLong(), users, outName,
                        { u -> u.getNeighbours(songsUsers, users) }) { u1, u2 ->
                    jaccardCoef(u1.favourites, u2.favourites)
                }
            }
            println("JaccardTime for total $total: $jaccardTime")
            listOf(100, 150, 200, 250).forEach { n ->
                //            listOf(100).forEach { n ->
                println("TOTAL: $total n: $n")
                val minHash = MinHash(n)
                val usersSigs = users.generateMinHashes(minHash)
                val minHashTime = measureTimeMillis {
                    val outName = formatName(output, "has_$n", total)
                    countAndWriteCoefficient(total.toLong(), usersSigs, outName, { usersSigs }) { u1, u2 ->
                        MinHash.similarity(u1.favourites, u2.favourites)
                    }
                }
                println("minHash time: $minHashTime")
            }
        }
    }
    println("total time: $measuredTime")
}

fun List<User>.generateMinHashes(minHash: MinHash): List<User> {
    val start = System.currentTimeMillis()
    return parallelStream()
            .map { it.copy(favourites = minHash.signature(it.favourites)) }
            .toList().also {
                println("generated minHashes for ${System.currentTimeMillis() - start}")
            }
}