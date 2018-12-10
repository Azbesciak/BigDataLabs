package cs.put.pmds.lab7

import java.io.{File, FileOutputStream}
import java.time._

object SongsQuerier extends App {

	case class Track(artist: String, name: String)

	case class Listening(user: String, month: Int)

	case class FullListening(user: String, songName: String, songId: String, artist: String, month: Int)

	require(args.length == 3, "args: <path to unique tracks> <path to listenings> <result path>")
	private val outputFile = new File(args(2))
	outputFile.getParentFile.mkdirs()
	outputFile.createNewFile()
	val fs = new FileOutputStream(outputFile)
	private val sc = Utils.getContext("songs")
	val songs = sc.textFile(args(0), 8)
	 .map(t => {
		 val trackLine = split(t).drop(1).padTo(3, "")
		 (trackLine(0), Track(trackLine(1), trackLine(2)))
	 })
	 .reduceByKey { case (f, _) => f }

	val listenings = sc.textFile(args(1), 8)
	 .map(t => {
		 val line = split(t)
		 (line(1), Listening(line(0), getMonth(line)))
	 }).join(songs)
	 .map { case (id, (l, t)) => (id, FullListening(l.user, t.name, id, t.artist, l.month)) }
	 .cache()

	private def getMonth(line: Array[String]) = {
		LocalDateTime.ofInstant(Instant.ofEpochMilli(line(2).toLong * 1000L), ZoneId.of("UTC+1")).getMonth.getValue
	}

	val sortedListeningsBySongId = listenings
	 .groupByKey()
	 .sortBy(-_._2.size)
	 .cache()

	sortedListeningsBySongId
	 .map(v => (v._2.head, v._2.size))
	 .take(10)
	 .foreach(l => fs.write(s"${l._1.songName} ${l._1.artist} ${l._2}\n".getBytes))

	listenings.groupBy(_._2.user)
	 .mapValues(v => v.groupBy(_._2.songId).size)
	 .sortBy(-_._2)
	 .take(10)
	 .foreach(l => fs.write(s"${l._1} ${l._2}\n".getBytes()))

	sortedListeningsBySongId
	 .map { case (_, l) => (l.head.artist, l.size) }
	 .reduceByKey(_ + _)
	 .sortBy(-_._2)
	 .take(1)
	 .foreach(l => fs.write(s"${l._1} ${l._2}\n".getBytes))

	listenings.groupBy(_._2.month)
	 .mapValues(_.size)
	 .sortBy(_._1)
	 .collect()
	 .foreach(l => fs.write(s"${l._1} ${l._2}\n".getBytes()))

	listenings.filter(_._2.artist == "Queen")
	 .groupByKey()
	 .sortBy(-_._2.size)
	 .take(3)
	 .map(_._2.map(_.user).toSet).reduce(_.intersect(_))
	 .toArray.sorted
	 .take(10)
	 .foreach(l => fs.write(s"$l\n".getBytes()))

	fs.flush()
	fs.close()

	def split(text: String) = text.split("<SEP>")
}
