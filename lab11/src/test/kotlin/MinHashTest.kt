package cs.put.pmds.lab10

import cs.put.pmds.lab11.LSH
import cs.put.pmds.lab9.User
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

class MinHashTest {

    companion object {
        private val exerciseBeforeExam = TestData(
                listOf(
                        HashFunction(1, 1, 5),
                        HashFunction(3, 1, 5),
                        HashFunction(2, 4, 5),
                        HashFunction(2, 1, 5)
                ),
                listOf(
                        User(1, intArrayOf(0, 3)),
                        User(2, intArrayOf(2)),
                        User(3, intArrayOf(1, 3, 4)),
                        User(4, intArrayOf(0, 2, 3))
                ),
                listOf(
                        User(1, intArrayOf(1, 0, 0, 1)),
                        User(2, intArrayOf(3, 2, 3, 0)),
                        User(3, intArrayOf(0, 0, 0, 2)),
                        User(4, intArrayOf(1, 0, 0, 0))
                ),
                listOf(
                        listOf(1.0, 0.0, 0.5, 0.75),
                        listOf(0.0, 1.0, 0.0, 0.25),
                        listOf(0.5, 0.0, 1.0, 0.5),
                        listOf(0.75, 0.25, 0.5, 1.0)
                ),
                mapOf(
                        1 to setOf(1, 3, 4),
                        2 to setOf(2, 4),
                        3 to setOf(1, 3, 4),
                        4 to setOf(1, 2, 3, 4)
                )
        )

        private val examTask = TestData(
                listOf(
                        HashFunction(1, 1, 5),
                        HashFunction(2, 3, 5),
                        HashFunction(4, 3, 5),
                        HashFunction(3, 1, 5)
                ),
                listOf(
                        User(1, intArrayOf(0, 3, 4)),
                        User(2, intArrayOf(1, 2)),
                        User(3, intArrayOf(1, 3)),
                        User(4, intArrayOf(0,2,3))
                ),
                listOf(
                        User(1, intArrayOf(0, 1, 0, 0)),
                        User(2, intArrayOf(2, 0, 1, 2)),
                        User(3, intArrayOf(2, 0, 0, 0)),
                        User(4, intArrayOf(1, 2, 0, 0))
                ),
                listOf(
                        listOf(1.0, 0.0, 0.5, 0.5),
                        listOf(0.0, 1.0, 0.5, 0.0),
                        listOf(0.5, 0.5, 1.0, 0.5),
                        listOf(0.5, 0.0, 0.5, 1.0)
                ),
                mapOf(
                        1 to setOf(1, 3, 4),
                        2 to setOf(2, 3),
                        3 to setOf(1, 2, 3, 4),
                        4 to setOf(1, 3, 4)
                )
        )

        @JvmStatic
        fun users() = listOf(
                Arguments.of(exerciseBeforeExam),
                Arguments.of(examTask)
        )
    }

    @ParameterizedTest
    @MethodSource("users")
    fun checkValidHashing(testData: TestData) {
        val minHash = MinHash(testData.hashFunctions)
        val minHashes = testData.users.generateMinHashes(minHash)
        validateMinHashes(testData.expectedSignatures, minHashes)
        validateUsersSimilarity(testData, testData.expectedSignatures)
        validateLsh(testData, minHashes)
    }

    private fun validateMinHashes(expectedMinHashes: List<User>, minHashes: List<User>) {
        val minHashAssertions = expectedMinHashes.zip(minHashes)
                .map { (exp, act) ->
                    { assertEquals(exp, act) { "dif for user ${exp.id}" } }
                }.toTypedArray()
        assertAll(*minHashAssertions)
    }

    private fun validateLsh(testData: TestData, minHashes: List<User>) {
        val lsh = LSH(testData.hashFunctions.size, 1, 10)
        val lshNeighbours = lsh.compute(minHashes)
                .map { it.key to it.value.map { it.value }.toSet() }
                .toMap()
        assertEquals(testData.lshNeighbours, lshNeighbours)
    }

    private fun validateUsersSimilarity(testData: TestData, expectedMinHashes: List<User>) {
        val simAssertions = testData.similarity.mapIndexed { x, line ->
            line.mapIndexed { y, expected ->
                val actual = MinHash.similarity(expectedMinHashes[x].favourites, expectedMinHashes[y].favourites)
                val res = { assertEquals(expected, actual, 1e-2) { "invalid values at $x $y: $expected vs $actual" } }
                res
            }
        }.flatten().toTypedArray()
        assertAll(*simAssertions)
    }

    data class TestData(
            val hashFunctions: List<HashFunction>,
            val users: List<User>,
            val expectedSignatures: List<User>,
            val similarity: List<List<Double>>,
            val lshNeighbours: Map<Int, Set<Int>>
    )
}
