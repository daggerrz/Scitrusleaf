package com.tapad.scitrusleaf.protocol

import org.specs2.mutable._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.Service
import org.jboss.netty.buffer.ChannelBuffers
import java.util.concurrent.atomic.AtomicInteger

/**
 *
 * User: liodden
 */

class ScitrusleafClientSpec extends Specification {

  "client" should {
    "read info messages" in {

      val client: Service[ClMessage, ClMessage] = ClientBuilder()
        .codec(ClCodec)
        .hosts("192.168.0.16:3000")
        .hostConnectionLimit(150)
        .build()

      class Citrusleaf(client: Service[ClMessage, ClMessage], ns: String) {
        def getString(key: String) = client(Get(ns, key = key)) map {
          _ match {
            case m: Response => m.ops.headOption.map(_.value)
            case m@_ => throw new IllegalArgumentException("Unable to handle mssage: " + m)
          }
        }
      }
      val c = new Citrusleaf(client, "test")


      c.getString("foo")
        .onSuccess (res => println(res.map(b => new String(b.array(), "UTF-8"))))
        .onFailure(t => t.printStackTrace())
            client(Set(namespace = "test", key = "foo", value = ChannelBuffers.wrappedBuffer("Yay!".getBytes("UTF-8")))) onSuccess { println }
      //      client(Get("test", "", "foo", "")) onSuccess { println }
            c.getString("foo") onSuccess (res => println(res.map(b => new String(b.array(), "UTF-8"))))
      //      client(ClInfo()) onSuccess { println }
      //      client(Get("test", "", "foo", "")) onSuccess { println }
      //      client(ClInfo()) onSuccess { println }


      var i = 1000000
      var started = 0
      val completed = new AtomicInteger()
      val startedTime = System.currentTimeMillis()

      val success = {
        m: ClMessage =>
          val c = completed.incrementAndGet()
          if (c % 1000 == 0) {
            println(completed + " %.2f requests / s".format(c.toDouble * 1000 / (System.currentTimeMillis() - startedTime)))
          }
      }

      while (i > 0) {
        client(Set(
          namespace = "test", key = "foo",
          value = ChannelBuffers.wrappedBuffer(i.toString.getBytes("UTF-8")))
        ) onSuccess (success)
        i -= 1
        started += 1
        if (i % 100000 == 0) println("Submitted " + i)
        if ((started - completed.get()) > 1000) {
          Thread.sleep(100)
        }
      }
      println("Done submitting")


      Thread.sleep(200000)
      true must_== true
    }
  }
}
