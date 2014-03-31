package scalaz.stream

import org.scalacheck.Prop._
import org.scalacheck.Properties
import java.util.concurrent.{TimeUnit, CountDownLatch}
import scalaz.concurrent.Task
import scalaz.stream.{Process => P}

object BlahSpec extends Properties("NAME_PENDING") {

  def emitOne[A](a: A, sink: Sink[Task, A]): Task[Unit] =
    P.eval(Task.now(a)).to(sink).run

  def emitAll[A](as: Seq[A], sink: Sink[Task, A]): Task[Unit] =
    Task.delay (as.foreach(a => emitOne(a, sink).run))

  def runAsync(t: Task[Unit]): Unit = t.runAsync(_ => ())

  property("NAME_PENDING") = secure {

    val (upstream, downstreamW, downstreamO) = async.NAME_PENDING[List[Int], Int, Int](
      (q, as) => (as.toList ::: q).sorted //, recv: (Q, Seq[A]) => Q
      , q => if (q.isEmpty) None else Some((q.init, q.last)) //, pop: Q => Option[(Q, A)]
      , _ => false // , queueFull: Q => Boolean
      , q => q.size //, query: (P, Q) => W
    , Nil)

    try {
      val initialWorkerCount = 4
      val workBatch2 = List(1, 50, 3, 52, 2, 51)

      val latch1 = new CountDownLatch(initialWorkerCount + 1)
      val latch2 = new CountDownLatch(initialWorkerCount + 1)
      val latch3 = new CountDownLatch(initialWorkerCount + 1 + workBatch2.size)

      @volatile var got = Vector.empty[Int] // No sync because it will be single-threaded when it matters

      val captureS: Sink[Task, Int] =
        P.constant((i: Int) => Task.delay {
          got :+= i
          latch1.countDown()
          latch1.await(2, TimeUnit.SECONDS)
          latch2.countDown()
          latch2.await(2, TimeUnit.SECONDS)
          latch3.countDown()
        })

      val captureP = downstreamO.to(captureS)

      // Start initialWorkerCount workers
      runAsync(captureP.run)
      for (i <- 2 to initialWorkerCount)
        runAsync(captureP.take(1).run)

      // Give all workers a job but don't let them finish
      runAsync(emitAll((1 to initialWorkerCount).toList, upstream))
      latch1.countDown()
      latch1.await(2, TimeUnit.SECONDS) :| "All workers should have a job" && {

        // Queue up new jobs while all workers are busy
        runAsync(emitAll(workBatch2, upstream))
        downstreamW.take(1).runLog.run.toList == List(6) && {
          got = Vector.empty
          latch2.getCount == 1 && {

            // Release workers
            latch2.countDown()
            latch2.await(2, TimeUnit.SECONDS) :| "All workers should finish" && {

              // Wait for second batch of jobs to finish
              latch3.countDown()
              latch3.await(2, TimeUnit.SECONDS)
              latch3.getCount == 0 && {

                // Confirm jobs were prioritised before doled out
                (got == workBatch2.sorted.reverse) :| "Output should be emitted in order of priority"
              }
            }
          }
        }
      }

    } finally {
//      runAsync(j.downstreamClose(End))
    }


  }


}
