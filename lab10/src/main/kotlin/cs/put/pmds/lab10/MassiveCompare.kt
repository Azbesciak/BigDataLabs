package cs.put.pmds.lab10

import java.io.File
import java.util.stream.Stream
import kotlin.streams.toList

data class FilesCompare(
        val original: List<File>,
        val another: List<File>
) {
    fun compare(): Stream<String> {
        validateSizes()
        val orgToComp = getOriginalAlignedList()
        return orgToComp.zip(another)
                .stream()
                .map { (file1, file2) ->
                    val stats = MediumErrorCounter.getStatsFromEach(file1.absolutePath, file2.absolutePath)
                    val (total, n) = file1.nameWithoutExtension.split("-").drop(1)
                    "total: $total n: $n - $stats"
                }
    }

    private fun getOriginalAlignedList() = if (original.size < another.size)
        another.indices.map { i -> original[i % original.size] }
    else original

    private fun validateSizes() {
        require(original.size <= another.size) { "copy size must be at least of size of original" }
        require(another.size % another.size == 0) { "group sizes are invalid" }
    }
}

object MassiveCompare {
    @JvmStatic
    fun main(args: Array<String>) {
        require(args.size == 2) { "require input dir path and output path" }
        val (input, output) = args
        val inputDir = File(input).apply {
            require(exists()) { "input dir does not exists" }
            require(isDirectory) { "input dir is not a directory" }
        }
        val results = inputDir.listFiles()
                .groupBy { splitName(it)[1] }
                .mapValues {
                    val (hash, org) = it.value
                            .groupBy { it.nameWithoutExtension.takeWhile { it.isLetter() } }
                            .toList().sortedBy { it.first }
                    FilesCompare(org.second, hash.second)
                }
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

    private fun splitName(it: File) = it.nameWithoutExtension.split("-")
}