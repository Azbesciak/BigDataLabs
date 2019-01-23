package cs.put.pmds.lab10.compare

object MassiveCompare {
    @JvmStatic
    fun main(args: Array<String>) {
        Comparer(args) { _, _ -> SquareErrorCounter() }.compareResults()
    }
}