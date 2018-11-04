package cs.put.pmds.labs.lab3

import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.runBlocking
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.system.measureTimeMillis

object Main {
    private const val TRACKS_TABLE = "tracks"
    private const val LISTENINGS_TABLE = "listenings"
    private const val SONGS_LISTENINGS_COUNT_TABLE = "songs_listenings_numbers"
    private const val MONTHLY_LISTENINGS_TABLE = "monthly_listenings"
    private const val SEPARATOR = "<SEP>"

    private val debug = System.getProperty("debug") == "true"

    @JvmStatic
    fun main(args: Array<String>) {
        require(args.size == 3) {
            "usage args: <path to db/':memory:'> <tracks_path> <triplets path>\n got: ${args.toList()}" }
        measureTime("total") {
            withDriver(args[0]) {
                initialize()
                initData(args)
                createIndexes()
                createSongsListeningsTable()
                createMonthlyListeningsTable()
                mineData()
            }
        }
    }

    private fun Connection.initData(args: Array<String>) {
        measureTime("init") {
            insertFromFile(args[1], "INSERT INTO $TRACKS_TABLE VALUES (?,?,?,?)")
            insertFromFile(args[2], "INSERT INTO $LISTENINGS_TABLE VALUES (?,?,?)")
        }
    }

    private inline fun withDriver(driver: String, block: Connection.() -> Unit) =
            DriverManager.getConnection("jdbc:sqlite:$driver").use { con ->
                con.block()
            }

    private inline fun <T> Connection.execute(select: String, consumer: ResultSet.() -> T) =
            prepareStatement(select).executeQuery().use {
                val values = mutableListOf<T>()
                while (it.next()) {
                    values += it.consumer()
                }
                values
            }

    private fun Connection.insertFromFile(filePath: String, sql: String): Int {
        var counter = 0
        prepareStatement(sql)
                .use { statement ->
                    beginRequest()
                    filePath.lines {
                        val values = it.split(SEPARATOR)
                        statement.executeInsert(values)
                        counter++
                    }
                }
        commit()
        return counter
    }

    private inline fun String.lines(consumer: (String) -> Unit) = File(this)
            .inputStream()
            .reader()
            .useLines { seq ->
                seq.forEach(consumer)
            }

    private fun PreparedStatement.executeInsert(it: List<String>) =
            it.forEachIndexed { i, value ->
                setString(i + 1, value)
            }.run { execute() }

    private fun Connection.initialize() {
        dropTable(TRACKS_TABLE)
        dropTable(LISTENINGS_TABLE)
        executeUpdate("""
        CREATE TABLE $TRACKS_TABLE (
            track_id varchar(18) NOT NULL,
            song_id varchar(18) NOT NULL,
            artist varchar(256) DEFAULT NULL,
            title varchar(256) DEFAULT NULL,
            PRIMARY KEY (track_id)
        );
    """)
        executeUpdate("""
        CREATE TABLE $LISTENINGS_TABLE (
            user_id varchar(40) NOT NULL,
            song_id varchar(18) NOT NULL REFERENCES $TRACKS_TABLE(song_id),
            listening_date DATETIME NOT NULL
        );"""
        )
        autoCommit = false
    }

    private fun Connection.createSongsListeningsTable() {
        createTable(SONGS_LISTENINGS_COUNT_TABLE, """
                create table $SONGS_LISTENINGS_COUNT_TABLE as
                select t.title, t.artist, t.song_id, count(*) as listenings_count
                from $TRACKS_TABLE t
                     join $LISTENINGS_TABLE l on t.song_id = l.song_id
                group by l.song_id;
            """)
    }

    private fun Connection.createMonthlyListeningsTable() {
        createTable(MONTHLY_LISTENINGS_TABLE, """
                create table $MONTHLY_LISTENINGS_TABLE as
                select month, count(*) as listenings
                from (SELECT strftime('%m', datetime(listening_date, 'unixepoch')) as month FROM $LISTENINGS_TABLE)
                group by month
                order by month;
            """)
    }

    private fun Connection.createTable(name: String, statement: String) {
        measureTime("create table: $name") {
            beginRequest()
            dropTable(name)
            executeUpdate(statement)
            commit()
        }
    }

    private fun Connection.executeUpdate(sql: String) = prepareStatement(sql).executeUpdate()

    private fun Connection.mineData() {
        measureTime("query") {
            runBlocking {
                arrayOf(
                        async { tracksRanking() },
                        async { usersRanking() },
                        async { artistsRanking() },
                        async { monthlyListenings() },
                        async { queenMostPopularSongListeners() }
                ).map { it.await().forEach { println(it) } }
            }
        }
    }

    private fun Connection.createIndexes() {
        measureTime("indexes") {
            createStatement()
                    .execute("create index songs on $LISTENINGS_TABLE(song_id);")
            createStatement()
                    .execute("create index artists on $TRACKS_TABLE(song_id);")
            commit()
        }
    }

    private fun Connection.tracksRanking() =
            execute("""
                select title, artist, listenings_count
                from $SONGS_LISTENINGS_COUNT_TABLE
                order by listenings_count desc
                limit 10;
            """) { serialize(3) }

    private fun Connection.usersRanking() =
            execute("""
                select user_id, count(distinct song_id) as listenings_count
                from $LISTENINGS_TABLE
                group by user_id
                order by count(distinct song_id) desc
                limit 10;
            """) { serialize(2) }

    private fun Connection.artistsRanking() =
            execute("""
                select artist, sum(listenings_count) as songs_listenings_count
                from $SONGS_LISTENINGS_COUNT_TABLE
                group by artist
                order by sum(listenings_count) desc limit 1;
            """) { serialize(2) }

    private fun Connection.monthlyListenings() =
            execute("select * from $MONTHLY_LISTENINGS_TABLE;") { serialize(2) }

    private fun Connection.queenMostPopularSongListeners() =
            execute("""
                select user_id
                from (select distinct user_id, l.song_id
                      from $LISTENINGS_TABLE l
                      where l.song_id in (select song_id
                                          from $SONGS_LISTENINGS_COUNT_TABLE
                                          where artist = 'Queen'
                                          order by listenings_count desc
                                          limit 3))
                group by user_id
                having count(*) = 3
                order by user_id
                limit 10;
            """) { serialize(1) }

    private fun ResultSet.serialize(fields: Int) = (1..fields).joinToString(" ") { getString(it) }

    private inline fun measureTime(name: String, block: () -> Unit) {
        val time = measureTimeMillis(block)
        if (debug)
            println("time of $name: ${time / 1000.0}s")
    }

    private fun Connection.dropTable(name: String) =
            executeUpdate("DROP TABLE IF EXISTS $name;")
}