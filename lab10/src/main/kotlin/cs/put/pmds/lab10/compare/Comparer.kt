package cs.put.pmds.lab10.compare

import java.io.File
import kotlin.streams.toList

class Comparer<T>(
        private val args: Array<String>,
        private val counter: (String, String) -> ErrorCounter<T>
) {
    fun compareResults() {
        require(args.size == 2) { "require input dir path and output path" }
        val (input, output) = args
        val inputDir = File(input).apply {
            require(exists()) { "input dir does not exists" }
            require(isDirectory) { "input dir is not a directory" }
        }
        val results = inputDir.listFiles()
                .groupBy { splitName(it)[1] }
                .mapValues { createInnerFilesCompare(it) }
                .toList()
                .sortedBy { it.first }
                .parallelStream()
                .map { it.second.compare().toList() }
                .toList()
                .flatten()

        File(output).apply { parentFile.mkdirs() }
                .bufferedWriter().use { o ->
                    results.forEach {
                        o.write(it)
                        o.newLine()
                    }
                    o.flush()
                }
    }

    private fun createInnerFilesCompare(it: Map.Entry<String, List<File>>): FilesCompare<T> {
        val (hash, org) = it.value
                .groupBy { it.nameWithoutExtension.takeWhile { it.isLetter() } }
                .toList().sortedBy { it.first }
        return FilesCompare(org.second, hash.second, counter)
    }

    private fun splitName(it: File) = it.nameWithoutExtension.split("-")
}