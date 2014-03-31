package scalaz.stream

import Process._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scalaz.{Order, Heap, \/}
import scalaz.concurrent._
import scalaz.stream.actor.actors
import scalaz.stream.actor.message
import scalaz.stream.async.mutable.Signal.Msg
import scalaz.stream.async.mutable.{WriterTopic, BoundedQueue}
import scalaz.stream.merge.{JunctionStrategies, Junction}
import scalaz.stream.async.immutable

package object async {

  import mutable.{Queue,Signal,Topic}

  /**
   * Convert from an `Actor` accepting `message.queue.Msg[A]` messages 
   * to a `Queue[A]`. 
   */
  def actorQueue[A](actor: Actor[message.queue.Msg[A]]): Queue[A] =
    new Queue[A] {
      def enqueueImpl(a: A): Unit = actor ! message.queue.enqueue(a)
      def dequeueImpl(cb: (Throwable \/ A) => Unit): Unit = actor ! message.queue.Dequeue(cb)
      def fail(err: Throwable): Unit = actor ! message.queue.fail(err)
      def cancel: Unit = actor ! message.queue.cancel
      def close: Unit = actor ! message.queue.close
    }

  /** 
   * Create a new continuous signal which may be controlled asynchronously.
   * All views into the returned signal are backed by the same underlying
   * asynchronous `Ref`.
   */
  def signal[A](implicit S: Strategy = Strategy.DefaultStrategy): Signal[A] = Signal(halt)


  /** 
   * Create a source that may be added to or halted asynchronously 
   * using the returned `Queue`, `q`. On calling `q.enqueue(a)`, 
   * unless another thread is already processing the elements 
   * in the queue, listeners on the queue will be run using the calling
   * thread of `q.enqueue(a)`, which means that enqueueing is not
   * guaranteed to take constant time. If this is not desired, use 
   * `queue` with a `Strategy` other than `Strategy.Sequential`.
   */
  def localQueue[A]: (Queue[A], Process[Task,A]) = 
    queue[A](Strategy.Sequential)

  /**
   * Converts discrete process to signal. Note that, resulting signal must be manually closed, in case the
   * source process would terminate. However if the source terminate with failure, the signal is terminated with that
   * failure
   * @param source discrete process publishing values to this signal
   */
  def toSignal[A](source: Process[Task, A])(implicit S: Strategy = Strategy.DefaultStrategy): mutable.Signal[A] =
     Signal(Process(source.map(Signal.Set(_))))

  /**
   * A signal constructor from discrete stream, that is backed by some sort of stateful primitive
   * like an Topic, another Signal or queue.
   *
   * If supplied process is normal process, it will, produce a signal that eventually may
   * be de-sync between changes, continuous, discrete or changed variants
   *
   * @param source
   * @tparam A
   * @return
   */
  private[stream] def stateSignal[A](source: Process[Task, A])(implicit S:Strategy = Strategy.DefaultStrategy) : immutable.Signal[A] =
    new immutable.Signal[A] {
      def changes: Process[Task, Unit] = discrete.map(_ => ())
      def continuous: Process[Task, A] = discrete.wye(Process.constant(()))(wye.echoLeft)(S)
      def discrete: Process[Task, A] = source
      def changed: Process[Task, Boolean] = (discrete.map(_ => true) merge Process.constant(false))
    }

  /**
   * Creates bounded queue that is bound by supplied max size bound.
   * Please see [[scalaz.stream.async.mutable.BoundedQueue]] for more details.
   * @param max maximum size of queue. When <= 0 (default) queue is unbounded
   */
  def boundedQueue[A](max: Int = 0)(implicit S: Strategy = Strategy.DefaultStrategy): BoundedQueue[A] = {
    val junction = Junction(JunctionStrategies.boundedQ[A](max), Process.halt)(S)
    new BoundedQueue[A] {
      def enqueueOne(a: A): Task[Unit] = junction.receiveOne(a)
      def dequeue: Process[Task, A] = junction.downstreamO
      def size: immutable.Signal[Int] = stateSignal(junction.downstreamW)
      def enqueueAll(xa: Seq[A]): Task[Unit] = junction.receiveAll(xa)
      def enqueue: Process.Sink[Task, A] = junction.upstreamSink
      def fail(rsn: Throwable): Task[Unit] = junction.downstreamClose(rsn)
    }
  }


  /** 
   * Create a source that may be added to or halted asynchronously 
   * using the returned `Queue`. See `async.Queue`. As long as the
   * `Strategy` is not `Strategy.Sequential`, enqueueing is 
   * guaranteed to take constant time, and consumers will be run on
   * a separate logical thread. Current implementation is based on 
   * `actor.queue`.
   */
  def queue[A](implicit S: Strategy = Strategy.DefaultStrategy): (Queue[A], Process[Task, A]) =
    actors.queue[A] match { case (snk, p) => (actorQueue(snk), p) }


  def NAME_PENDING[Q, W, A](
    recv: (Q, Seq[A]) => Q,
    pop: Q => Option[(A, Q)],
    queueFull: Q => Boolean,
    query: Q => W,
    initialQ: Q)
    (implicit S: Strategy = Strategy.DefaultStrategy)
    : (Sink[Task, A], Process[Task, W], Process[Task, A]) = {
    val js = JunctionStrategies.queueStrategy[Q, W, A](recv, pop, queueFull, query, initialQ)
    val j = Junction(js, Process.halt)(S)
    (j.upstreamSink, j.downstreamW, j.downstreamO)
  }

  def prioritisedQueue[W, A](query: Heap[A] => W)(implicit S: Strategy = Strategy.DefaultStrategy, o: Order[A]) =
    NAME_PENDING[Heap[A], W, A](
      (q, as) => (q /: as)((qq, a) => qq insert a) //, recv: (Q, Seq[A]) => Q
      , _.uncons //, pop: Q => Option[(Q, A)]
      , _ => false // , queueFull: Q => Boolean
      , query
      , Heap.Empty[A]
    )

  /** 
   * Convert an `Queue[A]` to a `Sink[Task, A]`. The `cleanup` action will be 
   * run when the `Sink` is terminated.
   */
  def toSink[A](q: Queue[A], cleanup: Queue[A] => Task[Unit] = (q: Queue[A]) => Task.delay {}): Process.Sink[Task,A] =
    io.resource(Task.now(q))(cleanup)(q => Task.delay { (a:A) => Task.now(q.enqueue(a)) })

  /**
   * Returns a topic, that can create publisher (sink) and subscriber (source)
   * processes that can be used to publish and subscribe asynchronously. 
   * Please see `Topic` for more info.
   */
  def topic[A](source:Process[Task,A] = halt)(implicit S: Strategy = Strategy.DefaultStrategy): Topic[A] = {
     val junction = Junction(JunctionStrategies.publishSubscribe[A], Process(source))(S)
     new Topic[A] {
       def publish: Process.Sink[Task, A] = junction.upstreamSink
       def subscribe: Process[Task, A] = junction.downstreamO
       def publishOne(a: A): Task[Unit] = junction.receiveOne(a)
       def fail(err: Throwable): Task[Unit] = junction.downstreamClose(err)
     }
  }

  /**
   * Returns Writer topic, that can create publisher(sink) of `I` and subscriber with signal of `W` values.
   * For more info see `WriterTopic`.
   * Note that when `source` ends, the topic does not terminate
   */
  def writerTopic[W,I,O](w:Writer1[W,I,O])(source:Process[Task,I] = halt)
    (implicit S: Strategy = Strategy.DefaultStrategy): WriterTopic[W,I,O] ={
    val q = boundedQueue[Process[Task,I]]()
    val junction = Junction(JunctionStrategies.liftWriter1(w), emit(source) ++ q.dequeue)(S)
    new WriterTopic[W,I,O] {
      def consumeOne(p:  Process[Task, I]): Task[Unit] = q.enqueueOne(p)
      def consume: Process.Sink[Task, Process[Task, I]] = q.enqueue
      def publish: Process.Sink[Task, I] = junction.upstreamSink
      def subscribe: Process.Writer[Task, W, O] = junction.downstreamBoth
      def subscribeO: Process[Task, O] = junction.downstreamO
      def subscribeW: Process[Task, W] = junction.downstreamW
      def signal: immutable.Signal[W] =  stateSignal(subscribeW)(S)
      def publishOne(i: I): Task[Unit] = junction.receiveOne(i)
      def fail(err: Throwable): Task[Unit] =
        for {
         _ <- junction.downstreamClose(err)
         _ <- q.fail(err)
        } yield ()
    }
  }


}

