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

  implicit def orderInt = scalaz.Order.fromScalaOrdering[Int]

  property("prioritisedQueue") = forAll {
    (l: List[Int]) =>

    val (upstream, downstreamW, downstreamO) = async.prioritisedQueue[Int, Int](_.size)

    try {
      val initialWorkerCount = 4
      val expQueueSize = l.size

      val latch1 = new CountDownLatch(initialWorkerCount + 1)
      val latch2 = new CountDownLatch(initialWorkerCount + 1)
      val latch3 = new CountDownLatch(initialWorkerCount + 1 + l.size)

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
        runAsync(emitAll(l, upstream))
        val queueSize = downstreamW.take(1).runLog.run.toList.head
        (queueSize == expQueueSize) :| s"Queue size should be $expQueueSize" && {
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
                (got == l.sorted) :| s"Output should be emitted in order of priority: $got"
              }
            }
          }
        }
      }

    } finally {
//      runAsync(j.downstreamClose(End)) // ooops, better figure out how to close
    }


  }


}
