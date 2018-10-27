package cs.put.pmds.labs.lab3

import com.carrotsearch.hppc.ObjectObjectHashMap
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.system.measureTimeMillis

object Sample {
    private const val TRACK_LISTENINGS_TABLE = "tracks_listenings"
    private const val SEPARATOR = "<SEP>"

    @JvmStatic
    fun main(args: Array<String>) {
        require(args.size == 3) { "usage args: <path to db/':memory:'> <tracks_path> <triplets path>" }
        withDriver(args[0]) {
            initialize()
            initData(args)

            execute("Select count(*) from $TRACK_LISTENINGS_TABLE") {
                println("tracks: ${getInt(1)}")
            }
        }
    }

    private fun Connection.initData(args: Array<String>) {
        val time = measureTimeMillis {
            val songs = readTracks(args)
            insertListenings(args, songs)
        }
        println("insert time: ${time / 1000}s")
    }

    private fun Connection.insertListenings(args: Array<String>, songs: ObjectObjectHashMap<String, List<String>>) {
        beginRequest()
        prepareStatement("""
                    INSERT INTO $TRACK_LISTENINGS_TABLE
                    (track_id,artist,title,user_id,song_id,listening_date)
                    VALUES (?,?,?,?,?,?)
                """).use { statement ->
            args[2].lines {
                val values = it.split(SEPARATOR)
                val insertArgs = songs[values[1]] + values
                statement.executeInsert(insertArgs)
            }
        }
        commit()
    }

    private fun readTracks(args: Array<String>): ObjectObjectHashMap<String, List<String>> {
        val songs = ObjectObjectHashMap<String, List<String>>(100_000)
        args[1].lines {
            val (trackId, songId, artist, title) = it.split(SEPARATOR)
            songs.put(songId,listOf(trackId, artist, title))
        }
        return songs
    }

    private inline fun withDriver(driver: String, block: Connection.() -> Unit) =
            DriverManager.getConnection("jdbc:sqlite:$driver").use { it.block() }

    private inline fun Connection.execute(select: String, consumer: ResultSet.() -> Unit) {
        prepareStatement(select).executeQuery().use {
            while (it.next())
                it.consumer()
        }
    }

    private inline fun String.lines(consumer: (String) -> Unit) = File(this)
            .inputStream()
            .reader()
            .useLines { it.forEach(consumer) }

    private fun PreparedStatement.executeInsert(it: List<String>) =
            it.forEachIndexed { i, value ->
                setString(i + 1, value)
            }.run { execute() }

    private fun Connection.initialize() {
        dropTables(TRACK_LISTENINGS_TABLE)
        executeUpdate("""
        CREATE TABLE $TRACK_LISTENINGS_TABLE (
            track_id varchar(18) NOT NULL,
            song_id varchar(18) NOT NULL,
            artist varchar(256) DEFAULT NULL,
            title varchar(256) DEFAULT NULL,
            user_id varchar(40) NOT NULL,
            listening_date DATETIME NOT NULL
        );"""
        )
        autoCommit = false
    }

    private fun Connection.dropTables(vararg tables: String) = tables.forEach {
        executeUpdate("DROP TABLE IF EXISTS $it;")
    }

    private fun Connection.executeUpdate(sql: String) = prepareStatement(sql).executeUpdate()
}