import cs.put.pmds.lab9.jaccardCoef
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.of
import org.junit.jupiter.params.provider.MethodSource

class JaccardTest {

    companion object {
        @JvmStatic
        fun params() = listOf(
                of(intArrayOf(1), intArrayOf(1), 1.0),
                of(intArrayOf(1, 2, 3), intArrayOf(1, 2, 3), 1.0),
                of(intArrayOf(1, 2, 3, 4, 5), intArrayOf(4, 5, 6, 7), 2.0 / 7),
                of(intArrayOf(1, 2, 3, 4, 5), intArrayOf(6, 7, 8, 9, 10), 0.0),
                of(intArrayOf(279991, 378165, 539630, 932750), intArrayOf(104016, 378165), 0.2),
                of(intArrayOf(1,3,5,7,9,10,11,13,15,17), intArrayOf(0,1,2,4,6,8,9,10,13,14,15,18,19), 5.0/18)
        )
    }


    @ParameterizedTest
    @MethodSource("params")
    fun check(user: IntArray, other: IntArray, expected: Double) {
        val actual = jaccardCoef(user, other)
        Assertions.assertEquals(expected, actual, 1e-4)
    }

}