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
                val minHashTime = measureTimeMillis {
                    val usersSigs = users.generateMinHashes(minHash)
                    val outName = formatName(output, "has", total, n)
                    countAndWriteCoefficient(total.toLong(), usersSigs, outName, { usersSigs }) { u1, u2 ->
                        minHash.similarity(u1.favourites, u2.favourites)
                    }
                }
                println("minHash time: $minHashTime")
            }
        }
    }
    println("total time: $measuredTime")
}

private fun formatName(base: String, algo: String, total: Int, n: Int? = null) = File(base).run {
    "${parentFile.path}/$algo${n ?: ""}-$total-$name"
}

fun List<User>.generateMinHashes(minHash: MinHash) = parallelStream()
        .map { it.copy(favourites = minHash.signature(it.favourites)) }
        .toList()