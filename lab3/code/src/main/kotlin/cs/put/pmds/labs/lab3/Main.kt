package cs.put.pmds.labs.lab3

import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.system.measureTimeMillis

object Sample {
    private const val TRACKS_TABLE = "tracks"
    private const val LISTENINGS_TABLE = "listenings"
    private const val TRACK_LISTENINGS_TABLE = "tracks_listenings"

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
            beginRequest()
            insertFromFile(args[1], "INSERT INTO $TRACKS_TABLE VALUES (?,?,?,?)")
            insertFromFile(args[2], "INSERT INTO $LISTENINGS_TABLE VALUES (?,?,?)")
            execute("""
                create table $TRACK_LISTENINGS_TABLE as
                    select t.track_id, t.song_id, t.title, t.artist, l.user_id, l.listening_date from
                    $LISTENINGS_TABLE l left join $TRACKS_TABLE t on l.song_id = t.song_id;
            """
            )
            dropTables(LISTENINGS_TABLE, TRACKS_TABLE)
            commit()
        }
        println("insert time: ${time / 1000}s")
    }

    private fun Connection.execute(vararg commands: String) {
        commands.forEach {
            createStatement().execute(it)
        }
    }

    private inline fun withDriver(driver: String, block: Connection.() -> Unit) =
            DriverManager.getConnection("jdbc:sqlite:$driver").use { con ->
                con.block()
            }

    private inline fun Connection.execute(select: String, consumer: ResultSet.() -> Unit) {
        prepareStatement(select).executeQuery().use {
            while (it.next()) {
                it.consumer()
            }
        }
    }

    private fun Connection.insertFromFile(filePath: String, sql: String): Int {
        var counter = 0
        prepareStatement(sql)
                .use { statement ->
                    filePath.lines {
                        val values = it.split("<SEP>")
                        statement.executeInsert(values)
                        counter++
                    }
                }
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
        dropTables(TRACKS_TABLE, LISTENINGS_TABLE, TRACK_LISTENINGS_TABLE)
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
            song_id varchar(18) NOT NULL REFERENCES tracks(song_id),
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