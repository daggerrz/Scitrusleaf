package com.tapad.scitrusleaf.protocol

import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.buffer.ChannelBuffers
import com.tapad.scitrusleaf.akka.AkkaClient


object Debug {
  val rvc = new AtomicInteger()
  def receive() { println("R:" + rvc.incrementAndGet())}
  val tx = new AtomicInteger()
  def write() { println("W:" + tx.incrementAndGet())}
}




object AkkaBench {

  def main(args: Array[String]) {
    import AkkaClient._

    val clientCount = 150

    val client = new LoadBalancer(clientCount, () => ClientBuilder.build("192.168.0.10", 3000))

    Thread.sleep(1000)

    val started = new AtomicInteger()
    val completed = new AtomicInteger()
    val startedTime = System.currentTimeMillis()
    val bytesRead = new AtomicInteger()


    def success(m: ClMessage) : Unit = {
        m match {
          case r : Response => r.ops.headOption.map(_.value.readableBytes()).foreach(c => bytesRead.addAndGet(c))
          case _ =>
        }
        val c = completed.incrementAndGet()
        if (c % 1000 == 0) {
          println(completed + " of %d started, %.2f requests / s, %(,d bytes read".format(started.get(), c.toDouble * 1000 / (System.currentTimeMillis() - startedTime), bytesRead.get()))
        }
    }



    def runBench() {
      def data = ChannelBuffers.wrappedBuffer(Array.fill[Byte](1500)(47))

      var i = 0

      while (i < 1000000) {
        i += 1
        val key = started.incrementAndGet()
//        client(Set(namespace = "test", key = key.toString, value = data)) map (success _)
        client(Get(namespace = "test", key = key.toString)).onComplete(_ fold(
          { e =>
            println(e.getMessage)
            e.printStackTrace()
          },
          r => success(r)
        ))
//        while ((started.get() - completed.get()) > 10000) {
//          Thread.sleep(100)
//        }
//        Thread.sleep(1000)
      }
      while (started.get() > completed.get()) {
        println("Started %d, completed %d".format(started.get(), completed.get()))
        Thread.sleep(1000)
      }
      println("Done submitting")


      Thread.sleep(200000)

    }

    def spawn() {
      new Thread() {
        override def run() {
          runBench()
        }
      }.start()
    }
    spawn()
  }
}
