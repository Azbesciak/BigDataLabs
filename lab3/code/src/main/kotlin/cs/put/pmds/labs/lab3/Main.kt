package cs.put.pmds.labs.lab3

import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.runBlocking
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.system.measureTimeMillis

object Sample {
    private const val TRACKS_TABLE = "tracks"
    private const val LISTENINGS_TABLE = "listenings"
    private const val SEPARATOR = "<SEP>"

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
            insertFromFile(args[1], "INSERT INTO $TRACKS_TABLE VALUES (?,?,?,?)")
            insertFromFile(args[2], "INSERT INTO $LISTENINGS_TABLE VALUES (?,?,?)")
        }
        println("insert time: ${time / 1000}s")
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
        executeUpdate("DROP TABLE IF EXISTS $TRACKS_TABLE;")
        executeUpdate("DROP TABLE IF EXISTS $LISTENINGS_TABLE;")
        executeUpdate("""
        CREATE TABLE $TRACKS_TABLE (
            track_id varchar(18) NOT NULL,
            song_id varchar(18) NOT NULL,
            artist varchar(256) DEFAULT NULL,
            title varchar(256) DEFAULT NULL,
            PRIMARY KEY (track_id)
        );
    """)
        executeUpdate(
                """
                    CREATE TABLE $LISTENINGS_TABLE (
            user_id varchar(40) NOT NULL,
            song_id varchar(18) NOT NULL REFERENCES $TRACKS_TABLE(song_id),
            listening_date DATETIME NOT NULL
        );"""
        )
        autoCommit = false
    }

    private fun Connection.executeUpdate(sql: String) = prepareStatement(sql).executeUpdate()

    private fun Connection.mineData() {
        val time = measureTimeMillis {
            createSongsIndex()
            runBlocking {
                arrayOf(
                        async { tracksRanking() },
                        async { usersRanking() },
                        async { artistsRanking() },
                        async { monthlyListenings() },
                        async { queenMostPopularSongListeners() }
                ).map { println(it.await()) }
            }
        }
        println("total query time: $time")
    }

    private fun Connection.createSongsIndex() = createStatement()
            .execute("create index songs on $LISTENINGS_TABLE(song_id);")

    private fun Connection.tracksRanking() =
            execute("""
            select t.title, t.artist, r.listenings_cout as listenings_count
            from (select count(*) as listenings_cout, song_id
                  from listenings
                  group by song_id
                  order by count(*) desc
                  limit 10) r
                   join tracks t on r.song_id = t.song_id
            order by listenings_count desc;
        """) { serialize(3) }

    private fun Connection.usersRanking() =
            execute("""
        select user_id, count(distinct song_id) as listenings_count
        from listenings
        group by user_id
        order by count(distinct song_id) desc
        limit 5;
        """) { serialize(2) }

    private fun Connection.artistsRanking() = execute("""
    select t.artist, count(*)
    from tracks t
           join listenings l on l.song_id = t.song_id
    group by t.artist
    order by count(*) desc
    limit 3;
    """) {
        serialize(2)
    }

    private fun Connection.monthlyListenings() =
            execute("""
        select month, count(*)
        from (SELECT strftime('%m', datetime(listening_date, 'unixepoch')) as month FROM listenings)
        group by month
        order by month;
        """) {
                serialize(2)
            }

    private fun Connection.queenMostPopularSongListeners() =
            execute("""
           select distinct user_id from listenings l
        where l.song_id in (
          select q.song_id as sid
          from (select song_id from tracks where artist = 'Queen') q
                 join listenings l on l.song_id = q.song_id
          group by q.song_id
          order by count(*) desc
          limit 3
            )
        order by user_id
        limit 10
        """) {
                serialize(1)
            }

    private fun ResultSet.serialize(fields: Int) = (1..fields).joinToString(" ") { getString(it) }

}