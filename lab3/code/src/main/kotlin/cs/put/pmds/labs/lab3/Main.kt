package cs.put.pmds.labs.lab3

import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.system.measureTimeMillis

object Sample {
    @JvmStatic
    fun main(args: Array<String>) {
        require(args.size == 3) { "usage args: <path to db/':memory:'> <tracks_path> <triplets path>" }
        withDriver(args[0]) {
            initialize()
            initData(args)

            execute("Select count(*) from tracks") {
                println("tracks: ${getInt(1)}")
            }
            execute("Select count(*) from listenings") {
                println("listenings: ${getInt(1)}")
            }
        }
    }

    private fun Connection.initData(args: Array<String>) {
        val time = measureTimeMillis {
            insertFromFile(args[1], "INSERT INTO tracks VALUES (?,?,?,?)")
            insertFromFile(args[2], "INSERT INTO listenings VALUES (?,?,?)")
        }
        println("insert time: ${time / 1000}s")
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
                    beginRequest()
                    filePath.lines {
                        val values = it.split("<SEP>")
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
        executeUpdate("DROP TABLE IF EXISTS tracks;")
        executeUpdate("DROP TABLE IF EXISTS listenings;")
        executeUpdate("""
        CREATE TABLE tracks (
            track_id varchar(18) NOT NULL,
            song_id varchar(18) NOT NULL,
            artist varchar(256) DEFAULT NULL,
            title varchar(256) DEFAULT NULL,
            PRIMARY KEY (track_id)
        );
    """)
        executeUpdate(
                """
                    CREATE TABLE listenings (
            user_id varchar(40) NOT NULL,
            song_id varchar(18) NOT NULL REFERENCES tracks(song_id),
            listening_date DATETIME NOT NULL
        );"""
        )
        autoCommit = false
    }

    private fun Connection.executeUpdate(sql: String) = prepareStatement(sql).executeUpdate()
}