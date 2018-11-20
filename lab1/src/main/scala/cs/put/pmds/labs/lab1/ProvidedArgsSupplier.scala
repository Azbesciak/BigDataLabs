package cs.put.pmds.labs.lab1


object ProvidedArgsSupplier extends ProblemArgsSupplier {
	override def produce() = ProgramArgs(10000, 0.1, 100, 100)
}