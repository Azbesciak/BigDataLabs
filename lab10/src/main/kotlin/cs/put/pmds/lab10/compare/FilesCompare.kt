package cs.put.pmds.lab10.compare

import java.io.File
import java.util.stream.Stream

data class FilesCompare<T>(
        val original: List<File>,
        val another: List<File>,
        val counter: (String, String) -> ErrorCounter<T>
) {
    fun compare(): Stream<String> {
        validateSizes()
        val orgToComp = getOriginalAlignedList()
        return orgToComp.zip(another)
                .stream()
                .map { (file1, file2) ->
                    val stats = FileErrorCounter(counter).getStatsFromEach(file1.absolutePath, file2.absolutePath)
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