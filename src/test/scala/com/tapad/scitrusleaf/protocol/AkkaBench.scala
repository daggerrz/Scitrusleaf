package com.tapad.scitrusleaf.protocol

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import java.net.InetSocketAddress
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import org.jboss.netty.buffer.ChannelBuffers
import java.util.concurrent.Executors
import akka.dispatch._
import akka.actor._
import akka.actor._
import akka.routing.RoundRobinRouter
import akka.util.Duration
import akka.util.duration._
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock

trait Client[REQ, RESP] {
  def apply(req: REQ) : Future[RESP]
}

class LoadBalancer[REQ, RESP](targets: Array[Client[REQ, RESP]]) extends Client[REQ, RESP] {
  private[this] val nodeIndex = new AtomicInteger(0)
  private[this] val nodeCount = targets.length

  private[this] def next() : Int = {
    val c = nodeIndex.getAndIncrement()
    if (c >= nodeCount -1) {
      if (nodeIndex.compareAndSet(nodeCount, 0)) {
        0
      } else {
        next()
      }
    } else c
  }

  def apply(req: REQ) = targets(next()).apply(req)
}

object Debug {
  val rvc = new AtomicInteger()
  def receive() { println("R:" + rvc.incrementAndGet())}
  val tx = new AtomicInteger()
  def write() { println("W:" + tx.incrementAndGet())}

}

class ClClient(private val host: String,
               private val port: Int,
               private val bs: ClientBootstrap,
               private implicit val executionContext : ExecutionContext) extends Client[ClMessage, ClMessage] {


  private[this] val writeQueue = new java.util.concurrent.ArrayBlockingQueue[ClMessage](100)
  private[this] val requestQueue = new java.util.concurrent.ArrayBlockingQueue[Promise[ClMessage]](100)
  private[this] val requestPending = new AtomicBoolean()

  private[this] var connector: Channel = _

  val handler = new SimpleChannelUpstreamHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val promise = requestQueue.poll()
      promise.success(e.getMessage.asInstanceOf[ClMessage])
      requestPending.set(false)
      sendNext()
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      e.getCause.printStackTrace()
    }
  }

  private def sendNext() {
    if (!writeQueue.isEmpty && requestPending.compareAndSet(false, true)) {
      try {
        connector.write(writeQueue.poll())
      } catch  {
        case e @ _ =>
          requestPending.set(false)
          requestQueue.poll().failure(e)
      }
    }
  }

  def apply(req: ClMessage) : Future[ClMessage] = {
    val f = Promise[ClMessage]()
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


object ClientBuilder {

  val resultPool = Executors.newFixedThreadPool(2)
  val bossPool = Executors.newFixedThreadPool(1)
  val workerPool = Executors.newFixedThreadPool(4)
  val factory = new NioClientSocketChannelFactory(bossPool, workerPool)
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
  val executionContext = ExecutionContext.fromExecutor(resultPool)


  def build(host: String, port: Int) = new ClClient(host, port, bs, executionContext)
}

object AkkaBench {

  def main(args: Array[String]) {

    val clientCount = 32

    val client = new LoadBalancer(Array.fill[Client[ClMessage, ClMessage]](clientCount) {
      val c = ClientBuilder.build("192.168.0.12", 3000)
      c.start()
      c
    })



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
          e => e.printStackTrace(),
          r => success(r)
        ))
//        while ((started.get() - completed.get()) > 10000) {
//          Thread.sleep(100)
//        }
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
