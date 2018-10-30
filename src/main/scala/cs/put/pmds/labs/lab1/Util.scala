package cs.put.pmds.labs.lab1


object Util {

	implicit class Col[T](obj: Array[T]) {
		def cross(): Array[(T, T)] = {
			obj
			 .flatMap(o1 => obj.map(o2 => (o1, o2)))
			 .filter(p => p._1 != p._2)
		}
	}

}