package cs.put.pmds.lab9

data class User(val id: Int, val favourites: IntArray) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as User

        if (id != other.id) return false
        if (!favourites.contentEquals(other.favourites)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id
        result = 31 * result + favourites.contentHashCode()
        return result
    }
}