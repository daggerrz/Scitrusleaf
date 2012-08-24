package com.tapad.scitrusleaf.protocol

import com.twitter.finagle.Service
import org.jboss.netty.buffer.ChannelBuffers
import java.util.concurrent.atomic.AtomicInteger
import com.tapad.scitrusleaf.finagle.CitrusleafCodec

/**
 *
 * User: liodden
 */

object Bench {

  def main(args: Array[String]) {

    val client: Service[ClMessage, ClMessage] = com.twitter.finagle.builder.ClientBuilder()
      .codec(CitrusleafCodec)
      .hosts("192.168.0.12:3000")
      .hostConnectionLimit(1000)
      .build()

    class Citrusleaf(client: Service[ClMessage, ClMessage], ns: String) {
      def getString(key: String) = client(Get(ns, key = key)) map {
        _ match {
          case m: Response => m.ops.headOption.map(_.value)
          case m@_ => throw new IllegalArgumentException("Unable to handle mssage: " + m)
        }
      }
    }


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

      while (i < 100000) {
        i += 1
        val key = started.incrementAndGet()
//        client(Set(namespace = "test", key = key.toString, value = data)) map (success _)
        client(Get(namespace = "test", key = key.toString)).onSuccess(success _ )
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
