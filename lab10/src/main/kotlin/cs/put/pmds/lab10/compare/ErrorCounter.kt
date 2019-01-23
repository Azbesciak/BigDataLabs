package cs.put.pmds.lab10.compare

interface ErrorCounter<T> {
    fun countError(u1: UserCompare, u2: UserCompare)
    val result: T
}