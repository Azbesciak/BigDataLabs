package cs.put.pmds.lab10

import cs.put.pmds.lab9.User
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
    val mem = Runtime.getRuntime().freeMemory()
    val measuredTime = measureTimeMillis {
        val users = fetchUsers(file)
        println("used mem: ${mem - Runtime.getRuntime().freeMemory()} songs: ${users.size}")
//        listOf(100,200,300,400,500).forEach { total ->
        listOf(100).forEach { total ->
            val jaccardTime = measureTimeMillis {
                countAndWriteCoefficient(total.toLong(), users, formatName(output, "jac", total)) { u1, u2 ->
                    jaccardCoef(u1.favourites, u2.favourites)
                }
            }
            println("JaccardTime for total $total: $jaccardTime")
//            listOf(100,150,200,250).forEach { n ->
            listOf(100).forEach { n ->
                println("TOTAL: $total n: $n")
                val minHash = MinHash(n)
                val minHashTime = measureTimeMillis {
                val usersSigs = users.generateMinHashes(minHash)
                    countAndWriteCoefficient(total.toLong(), usersSigs, formatName(output, "has", total, n)) { u1, u2 ->
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