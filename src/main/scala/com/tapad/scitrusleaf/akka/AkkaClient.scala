package com.tapad.scitrusleaf.akka

import java.util.concurrent.Executors
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import com.tapad.scitrusleaf.protocol.{ClMessage, ClMessageDecoder, ClFrameDecoder, ClMessageEncoder}
import akka.dispatch.{Future, Promise, ExecutionContext}
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import java.net.InetSocketAddress
import annotation.tailrec
import java.nio.channels.ClosedChannelException


object AkkaClient {

  trait Client[REQ, RESP] {
    def apply(req: REQ): Future[RESP]
    def connect() : Future[Client[REQ, RESP]]
  }

  class LoadBalancer[REQ, RESP](nodeCount: Int, factory: () => Client[REQ, RESP]) extends Client[REQ, RESP] {

    private[this] val nodeIndex = new AtomicInteger(0)
    private[this] var nodes = Vector.empty[Client[REQ, RESP]]

    (0 until nodeCount).foreach { _ =>
      factory().connect().onComplete { _ fold (
        { error => println(error.getMessage) },
        n => nodes = n +: nodes
      )}
    }

    private[this] def next(): Int = {
      val c = nodeIndex.getAndIncrement()
      if (c >= nodes.size - 1) {
        if (nodeIndex.compareAndSet(nodeCount, 0)) {
          0
        } else {
          next()
        }
      } else c
    }

    def apply(req: REQ) = nodes(next()).apply(req)

    def connect() = null
  }

  class ClClient(host: String,
                 port: Int,
                 bs: ClientBootstrap,
                 private implicit val executionContext: ExecutionContext) extends Client[ClMessage, ClMessage] {


    private[this] val writeQueue = new java.util.concurrent.ArrayBlockingQueue[ClMessage](100)
    private[this] val requestQueue = new java.util.concurrent.ArrayBlockingQueue[Promise[ClMessage]](100)
    private[this] val requestPending = new AtomicBoolean()

    private[this] var connector: Channel = _

    @tailrec private def sendFailures(ex: Throwable) {
      requestQueue.poll() match {
        case null =>
        case promise =>
          promise.failure(ex)
          sendFailures(ex)
      }
    }

    val handler = new SimpleChannelUpstreamHandler {
      override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
        val promise = requestQueue.poll()
        promise.success(e.getMessage.asInstanceOf[ClMessage])
        requestPending.set(false)
        sendNext()
      }

      override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
        super.channelClosed(ctx, e)
        sendFailures(new ClosedChannelException())
      }

      override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
        ctx.getChannel.close()
        sendFailures(e.getCause())
      }
    }

    private def sendNext() {
      if (!writeQueue.isEmpty && requestPending.compareAndSet(false, true)) {
        try {
          connector.write(writeQueue.poll())
        } catch {
          case e : Exception =>
            requestPending.set(false)
            sendFailures(e)
        }
      }
    }

    def apply(req: ClMessage): Future[ClMessage] = {
      val f = Promise[ClMessage]()
      requestQueue.put(f)
      writeQueue.put(req)
      sendNext()
      f
    }


    def connect() = {
      val res = Promise[ClClient]
      val cf = bs.connect(new InetSocketAddress(host, port))
      cf.addListener(new ChannelFutureListener {
        def operationComplete(future: ChannelFuture) {
          if (future.isSuccess) {
            println(this + " connected")
            future.getChannel.getPipeline.addLast("client", handler)
            connector = future.getChannel
            res.success(ClClient.this)
          } else {
            System.out.println("--- CLIENT - Failed to connect to server at " +
              "%s:%d".format(host, port))
            res.failure(future.getCause)
          }
        }
      })
      res
    }
  }

  object ClientBuilder {
    val resultPool = Executors.newFixedThreadPool(1)
    val executionContext = ExecutionContext.fromExecutor(resultPool)

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
    bs.setOption("connectTimeoutMillis", 1)


    def build(host: String, port: Int) = new ClClient(host, port, bs, executionContext)
  }

}
