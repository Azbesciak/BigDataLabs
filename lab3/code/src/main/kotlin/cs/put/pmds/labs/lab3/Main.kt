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
    private val DEBUG = System.getProperty("debug") == "true"
    @JvmStatic
    fun main(args: Array<String>) {
        require(args.size == 3) { "usage args: <path to db/':memory:'> <tracks_path> <triplets path>" }
        withDriver(args[0]) {
            initialize()
            initData(args)
            mineData()
        }
    }

    private fun Connection.initData(args: Array<String>) {
        val time = measureTimeMillis {
            val songs = readTracks(args)
            insertListenings(args, songs)
        }
        debug("insert", time)
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
            songs.put(songId, listOf(trackId, artist, title))
        }
        return songs
    }

    private inline fun withDriver(driver: String, block: Connection.() -> Unit) =
            DriverManager.getConnection("jdbc:sqlite:$driver").use { it.block() }

    private inline fun Connection.execute(select: String, consumer: ResultSet.() -> String) =
            prepareStatement(select).executeQuery().use {
                val results = mutableListOf<String>()
                while (it.next())
                    results += it.consumer()
                results
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

    private fun Connection.mineData() {
        val indexTime = measureTimeMillis { createIndexes() }
        debug("index", indexTime)

        val time = measureTimeMillis {
            tracksRanking().forEach { println(it) }
            usersRanking().forEach { println(it) }
            artistsRanking().forEach { println(it) }
            monthlyListenings().forEach { println(it) }
            queenMostPopularSongListeners().forEach { println(it) }
        }
        debug("query", time)
    }

    private fun Connection.createIndexes() {
        createStatement()
                .execute("create index songs on $TRACK_LISTENINGS_TABLE(song_id);")
                .also { commit() }
        createStatement()
                .execute("create index artists on $TRACK_LISTENINGS_TABLE(artist);")
                .also { commit() }
    }

    private fun Connection.tracksRanking() =
            execute("""
        select title, artist, count(*) as listenings_cout
        from $TRACK_LISTENINGS_TABLE
              group by song_id
              order by count(*) desc
              limit 10;
        """) { serialize(3) }

    private fun Connection.usersRanking() =
            execute("""
        select user_id, count(distinct song_id) as listenings_count
        from $TRACK_LISTENINGS_TABLE
        group by user_id
        order by count(distinct song_id) desc
        limit 5;
        """) { serialize(2) }

    private fun Connection.artistsRanking() = execute("""
        select artist, count(*)
        from $TRACK_LISTENINGS_TABLE
        group by artist
        order by count(*) desc
        limit 3;
    """) {
        serialize(2)
    }

    private fun Connection.monthlyListenings() =
            execute("""
        select month, count(*)
        from (SELECT strftime('%m', datetime(listening_date, 'unixepoch')) as month FROM $TRACK_LISTENINGS_TABLE)
        group by month
        order by month;
        """) {
                serialize(2)
            }

    private fun Connection.queenMostPopularSongListeners() =
            execute("""
            select distinct user_id
            from tracks_listenings l
            where l.song_id in (select q.song_id
                                from tracks_listenings q
                                where artist = 'Queen'
                                group by q.song_id
                                order by count(*) desc
                                limit 3)
            order by user_id
            limit 10;
        """) {
                serialize(1)
            }

    private fun ResultSet.serialize(fields: Int) = (1..fields).joinToString(" ") { getString(it) }
    private fun debug(value: String, time: Long) {
        if (DEBUG)
            println("time of $value: ${time / 1000}s")
    }
}