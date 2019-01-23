package cs.put.pmds.lab11

import com.carrotsearch.hppc.IntArrayList
import com.carrotsearch.hppc.IntHashSet
import com.carrotsearch.hppc.IntObjectHashMap
import cs.put.pmds.lab9.User
import cs.put.pmds.lab9.computeIfAbsent
import kotlin.streams.toList

class LSH(private val n: Int, private val rowsPerBand: Int, private val someHashInd: Int) {
    init {
        require(n % rowsPerBand == 0) { "n % rowsPerBand must be 0" }
    }

    fun compute(users: List<User>): IntObjectHashMap<IntHashSet> {
        val bucketsNum = n / rowsPerBand
        val buckets = createBuckets(bucketsNum, users)
        val usersSim = initializeSimilarity(users)
        fillUsersSimilarity(buckets, usersSim)
        return usersSim
    }

    private fun fillUsersSimilarity(buckets: List<IntObjectHashMap<IntArrayList>>, usersSim: IntObjectHashMap<IntHashSet>) {
        buckets.forEach { bucket ->
            bucket.forEach { hash ->
//                if (hash.value.size() > 1)
                hash.value.forEach { uid ->
                    usersSim.computeIfAbsent(uid.value) { IntHashSet(10) }.addAll(hash.value)
                }
            }
        }
    }

    private fun initializeSimilarity(users: List<User>) =
            IntObjectHashMap<IntHashSet>(users.size / 10)

    private fun createBuckets(bucketsNum: Int, users: List<User>) =
            (0 until bucketsNum).toList().parallelStream().map { i ->
                val bucket = IntObjectHashMap<IntArrayList>(users.size)
                users.forEach { (id, songs) ->
                    val start = i * rowsPerBand
                    val end = start + rowsPerBand
                    val hash = (start until end).fold(0) { acc, i -> acc * someHashInd + songs[i] }
                    bucket.computeIfAbsent(hash) { IntArrayList() }.add(id)
                }
                bucket
            }.toList()
}