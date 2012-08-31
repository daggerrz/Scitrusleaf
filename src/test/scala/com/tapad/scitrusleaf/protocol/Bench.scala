package com.tapad.scitrusleaf.protocol

import com.twitter.finagle.Service
import org.jboss.netty.buffer.ChannelBuffers
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import com.tapad.scitrusleaf.finagle.CitrusleafCodec
import com.twitter.finagle.tracing.NullTracer
import com.twitter.util.{Time, Future}
import com.twitter.finagle.stats.SummarizingStatsReceiver

/**
 *
 * User: liodden
 */

object Bench {

  def main(args: Array[String]) {

    val concurrency = 150
    val totalRequests = 50000

    val key = new AtomicInteger(0)
    val statsReceiver = new SummarizingStatsReceiver
    val responses = new AtomicInteger(0)
    val completedRequests = new AtomicInteger()
    val errors = new AtomicInteger(0)
    val bytesReceived = new AtomicLong(0)

    val client: Service[ClMessage, ClMessage] = com.twitter.finagle.builder.ClientBuilder()
      .codec(CitrusleafCodec)
      .hosts("192.168.0.16:3000")
      .hostConnectionLimit(concurrency)
      .hostConnectionCoresize(concurrency)
      .reportTo(statsReceiver)
      .build()

    val start = Time.now

    val requests = Future.parallel(concurrency) {
      Future.times(totalRequests / concurrency) {
        val request = Get(namespace = "test", key = key.incrementAndGet().toString)
        client(request) onSuccess { response =>
          response.asInstanceOf[Response].ops.headOption.map(_.value.readableBytes()).foreach(c => bytesReceived.addAndGet(c))
          responses.incrementAndGet()
        } handle { case e =>
          errors.incrementAndGet()
        } ensure {
          completedRequests.incrementAndGet()
        }
      }
    }


    Future.join(requests) ensure {
      client.release()

      val duration = start.untilNow
      println("%20s\t%s".format("Status", "Count"))
        println("%20s\t%d".format(0, responses.get()))
      println("================")
      println("%d requests completed in %dms (%f requests per second)".format(
        completedRequests.get, duration.inMilliseconds,
        totalRequests.toFloat / duration.inMillis.toFloat * 1000))
      println("%d errors".format(errors.get))

      println("%2f MBytes received".format(bytesReceived.get() / 1000000.0))
      println("stats")
      println("=====")

      statsReceiver.print()
    }
  }


//  val started = new AtomicInteger()
//  val completed = new AtomicInteger()
//  val startedTime = System.currentTimeMillis()
//  val bytesRead = new AtomicInteger()
//
//
//  def success(m: ClMessage) : Unit = {
//      m match {
//        case r : Response => r.ops.headOption.map(_.value.readableBytes()).foreach(c => bytesRead.addAndGet(c))
//        case _ =>
//      }
//      val c = completed.incrementAndGet()
//      if (c % 1000 == 0) {
//        println(completed + " of %d started, %.2f requests / s, %(,d bytes read".format(started.get(), c.toDouble * 1000 / (System.currentTimeMillis() - startedTime), bytesRead.get()))
//      }
//  }
//
//    def runBench() {
//      def data = ChannelBuffers.wrappedBuffer(Array.fill[Byte](1500)(47))
//
//      var i = 0
//
//      while (i < 100000) {
//        i += 1
//        val key = started.incrementAndGet()
////        client(Set(namespace = "test", key = key.toString, value = data)) map (success _)
//        client(Get(namespace = "test", key = key.toString))
//          .onSuccess(success _ )
//          .onFailure(_ => completed.decrementAndGet())
//      }
//      while (started.get() > completed.get()) {
//        println("Started %d, completed %d".format(started.get(), completed.get()))
//        Thread.sleep(1000)
//      }
//      println("Done submitting")
//
//
//      Thread.sleep(200000)
//
//    }
//
//    def spawn() {
//      new Thread() {
//        override def run() {
//          runBench()
//        }
//      }.start()
//    }
//    spawn()
//

}
