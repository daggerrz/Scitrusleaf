package com.tapad.scitrusleaf.protocol

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import java.net.InetSocketAddress
import com.twitter.util.{Promise, Future}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import org.jboss.netty.buffer.ChannelBuffers
import java.util.concurrent.{Executors, ArrayBlockingQueue}
import socket.oio.OioClientSocketChannelFactory

/**
 *
 * User: liodden
 */

object RawBench {

  def main(args: Array[String]) {

    trait Client[REQ, RESP] {
      def apply(req: REQ) : Future[RESP]
    }

    class LoadBalancer[REQ, RESP](targets: Array[Client[REQ, RESP]]) extends Client[REQ, RESP] {
      private[this] val nodeIndex = new AtomicInteger(0)
      private[this] val nodes = targets.length

      private[this] def next() : Int = {
        val c = nodeIndex.getAndIncrement()
        if (c >= nodes -1) {
          if (nodeIndex.compareAndSet(nodes, 0)) {
            0
          } else {
            next()
          }
        } else c
      }

      def apply(req: REQ) = targets(next()).apply(req)
    }

    val bossPool = Executors.newFixedThreadPool(1)
    val workerPool = Executors.newFixedThreadPool(4)
    val clientCount = 150
    val factory = new NioClientSocketChannelFactory(bossPool, workerPool)
//    val factory = new OioClientSocketChannelFactory(Executors.newFixedThreadPool(clientCount))
    val bs = new ClientBootstrap(factory)
    bs.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        Channels.pipeline(
          new ClMessageEncoder,
          new ClFrameDecoder,
          new ClMessageDecoder

        )
      }
    })
    bs.setOption("tcpNoDelay", true)
    bs.setOption("reuseAddress", true)
    bs.setOption("receiveBufferSize", 1600)


    class ClClient(private val host: String, private val port: Int) extends Client[ClMessage, ClMessage] {

      var connector: Channel = _

      private[this] val writeQueue = new java.util.concurrent.ArrayBlockingQueue[ClMessage](100)
        private[this] val requestQueue = new java.util.concurrent.ArrayBlockingQueue[Promise[ClMessage]](100)
        private[this] val requestPending = new AtomicBoolean()

      val handler = new SimpleChannelUpstreamHandler {
        override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
          val promise = requestQueue.poll()
          promise.setValue(e.getMessage.asInstanceOf[ClMessage])
          requestPending.set(false)
          sendNext()
        }
      }
      private def sendNext() {
        if (!writeQueue.isEmpty && requestPending.compareAndSet(false, true)) {
          try {
            connector.write(writeQueue.poll())
          } catch  {
            case e @ _ =>
              requestPending.set(false)
              requestQueue.poll().setException(e)
          }
        }
      }

      def apply(req: ClMessage) : Future[ClMessage] = {
        val f = new Promise[ClMessage]
        requestQueue.put(f)
        writeQueue.put(req)
        sendNext()
        f
      }


      def start() {
        val future = bs.connect(new InetSocketAddress(host, port))
        if (!future.awaitUninterruptibly().isSuccess()) {
          System.out.println("--- CLIENT - Failed to connect to server at " +
            "%s:%d".format(host, port))
          bs.releaseExternalResources();
          false
        } else {
          println(this + " connected")
          future.getChannel.getPipeline.addLast("client", handler)
          connector = future.getChannel
          connector.isConnected
        }
      }
    }


    val client = new LoadBalancer(Array.fill[Client[ClMessage, ClMessage]](clientCount) {
      val c= new ClClient("192.168.0.12", 3000)
      c.start()
      c
    })



    var i = 1000000
    var started = new AtomicInteger()
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

    def data = ChannelBuffers.wrappedBuffer(Array.fill[Byte](1500)(47))

    while (i > 0) {
//      client(Set(namespace = "test", key = i.toString, value = data)) onSuccess (success)
      client(Get(namespace = "test", key = i.toString)) onSuccess(success _)
      i -= 1
      started.incrementAndGet()
      if (i % 100000 == 0) println("Submitted " + i)
      if ((started.get() - completed.get()) > 10000) {
        Thread.sleep(10)
      }
    }
    println("Done submitting")


    Thread.sleep(200000)


  }
}
