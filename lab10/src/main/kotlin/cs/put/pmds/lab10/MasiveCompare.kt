package cs.put.pmds.lab10

import java.io.File
import kotlin.streams.toList

object MasiveCompare {
    @JvmStatic
    fun main(args: Array<String>) {
        require(args.size == 2) { "require input dir path and output path" }
        val (input, output) = args
        val inputDir = File(input).apply {
            require(exists()) { "input dir does not exists" }
            require(isDirectory) { "input dir is not a directory" }
        }
        val toCompare = inputDir.listFiles()
                .groupBy { it.nameWithoutExtension.split("-").first() }
                .mapValues { it.value.sortedBy { it.name } }
                .toList()
                .sortedBy { it.first }

        require(toCompare.size == 2) { "require 2 groups, got ${toCompare.map { it.first }}" }
        val (f, s) = toCompare
        require(f.second.size == s.second.size) { "require equal number of lists in each group" }
        val results = f.second.zip(s.second)
                .parallelStream()
                .map { (file1, file2) ->
                    val stats = MediumErrorCounter.getStatsFromEach(file1.absolutePath, file2.absolutePath)
                    val (total, n) = file1.nameWithoutExtension.split("-").drop(1)
                    "total: $total n: $n - $stats"
                }.toList()
        File(output).apply { parentFile.mkdirs() }
                .bufferedWriter().use { o ->
                    results.forEach {
                        o.write(it)
                        o.newLine()
                    }
                    o.flush()
                }
    }
}