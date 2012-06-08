package com.tapad.scitrusleaf.protocol

import org.specs2.mutable._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.Service
import com.twitter.util.Duration._
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

//      client(Set("test", "key", ChannelBuffers.wrappedBuffer("value".getBytes("UTF-8")))) onSuccess { println }
      client(Get("test", "foo")) onSuccess { println }
//      client(ClInfo()) onSuccess { println }
      Thread.sleep(2000)
      true must_== true
    }
  }
}
