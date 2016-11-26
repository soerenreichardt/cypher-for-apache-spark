package org.opencypher.spark.benchmark

object BenchmarkSeries {
  def run[G](benchmarkAndGraph: BenchmarkAndGraph[G], nbrTimes: Int = 6, warmupTimes: Int = 3): BenchmarkResult = {
    warmup(benchmarkAndGraph, warmupTimes)

    measure(benchmarkAndGraph, nbrTimes)
  }

  private def runAndTime(i: Int, f: => Outcome): (Long, Outcome) = {
    println(s"Timing -- Run $i")
    val start = System.currentTimeMillis()
    val outcome = f
    val time = System.currentTimeMillis() - start
    println(s"Done -- $time ms")
    time -> outcome
  }

  private def warmup[G](benchmarkAndGraph: BenchmarkAndGraph[G], nbrTimes: Int) = {
    benchmarkAndGraph.use { (benchmark, graph) =>
      println("Begin warmup")
      (0 until nbrTimes).foreach { i =>
        runAndTime(i, benchmark.run(graph))
      }
    }
  }

  private def measure[G](benchmarkAndGraph: BenchmarkAndGraph[G], nbrTimes: Int) = {
    benchmarkAndGraph.use { (benchmark, graph) =>
      val outcome = benchmark.run(graph)
      val plan = outcome.plan
      val count = outcome.computeCount
      val checksum = outcome.computeChecksum

      println("Begin measurements")
      val outcomes = (0 until nbrTimes).map { i =>
        runAndTime(i, benchmark.run(graph))
      }
      BenchmarkResult(benchmark.name, outcomes.map(_._1), plan, count, checksum)
    }
  }
}
