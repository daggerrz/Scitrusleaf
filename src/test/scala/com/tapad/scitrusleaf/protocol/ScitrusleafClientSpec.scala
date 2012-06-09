package com.tapad.scitrusleaf.protocol

import org.specs2.mutable._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.Service
import org.jboss.netty.buffer.ChannelBuffers

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
        .hostConnectionLimit(1)
        .build()

      class Citrusleaf(client: Service[ClMessage, ClMessage], ns: String) {
        def getString(key: String) = client(Get(ns, key = key)) map { _ match {
          case m : Response => m.ops.headOption.map(_.value)
          case m @ _ => throw new IllegalArgumentException("Unable to handle mssage: " + m)
        }}
      }
      val c = new Citrusleaf(client, "test")
//      client(Set(namespace = "test", key = "foo", value = ChannelBuffers.wrappedBuffer("aaaaxx".getBytes("UTF-8")))) onSuccess { println }
      c.getString("foo") onSuccess (res => println(res.map(b => new String(b.array(), "UTF-8"))))
//      c.getString("foo") onSuccess (res => println(res.map(b => new String(b.array(), "UTF-8"))))
//      c.getString("fo2o") onSuccess (res => println(res.map(b => new String(b.array(), "UTF-8"))))
//      c.getString("fo2o") onSuccess (res => println(res.map(b => new String(b.array(), "UTF-8"))))
//      client(Get("test", "", "foo", "")) onSuccess { println }
//      client(ClInfo()) onSuccess { println }
      Thread.sleep(2000)
      true must_== true
    }
  }
}
