package com.tapad.scitrusleaf.protocol

import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import com.tapad.scitrusleaf.akka.AkkaClient
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.util.{Time}
import akka.dispatch._
import akka.util.Duration
import akka.actor.ActorSystem
import java.util.concurrent.TimeUnit
import org.jboss.netty.buffer.ChannelBuffers


object AkkaBench {

  def main(args: Array[String]) {
    import AkkaClient._
    implicit val system = ActorSystem("foo")
    implicit val executor = ExecutionContext.defaultExecutionContext


    val concurrency = 150
    val totalRequests = 50000

    val key = new AtomicInteger(0)
    val statsReceiver = new SummarizingStatsReceiver
    val responses = new AtomicInteger(0)
    val completedRequests = new AtomicInteger()
    val errors = new AtomicInteger(0)
    val bytesReceived = new AtomicLong(0)

    val client = new LoadBalancer(concurrency, () => ClientBuilder.build("192.168.0.16", 3000))

    val start = Time.now

    val blanks = Array.ofDim[Byte](500)
    val requests = (0 until concurrency).map { _ =>
        (0 until (totalRequests / concurrency)).map { _ =>
                  val request = Get(namespace = "test", key = key.incrementAndGet().toString)
//            val data = ChannelBuffers.copiedBuffer(blanks)
//            val request = Set(namespace = "test", key = key.incrementAndGet().toString, value = data)
            client(request) onSuccess { case response =>
              response.asInstanceOf[Response].ops.headOption.map(_.value.readableBytes()).foreach(c => bytesReceived.addAndGet(c))
              responses.incrementAndGet()
            } onFailure {
              case e =>
                errors.incrementAndGet()
            } onComplete {
              case _ =>
                completedRequests.incrementAndGet()
            }
        }
    }.flatten



    val f: Future[Seq[ClMessage]] = Future.sequence(requests)

    Await.result(f, Duration.apply(10, TimeUnit.MINUTES))


    val duration = start.untilNow
    println("%20s\t%s".format("Status", "Count"))
    println("%20s\t%d".format(0, responses.get()))
    println("================")
    println("%d requests completed in %dms (%f requests per second)".format(
      completedRequests.get, duration.inMilliseconds,
      totalRequests.toFloat / duration.inMillis.toFloat * 1000))
    println("%d errors".format(errors.get))
    println("%2f MBytes received".format(bytesReceived.get() / 1000000.0))

    System.exit(0)
  }
}
