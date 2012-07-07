package com.tapad.scitrusleaf

import com.twitter.finagle.Service
import org.jboss.netty.buffer.ChannelBuffer
import protocol.{Response, ClMessage}
import com.twitter.util.Future

class Scitrusleaf(client: Service[ClMessage, ClMessage], ns: String) {

  def get(key: String, set: String = "", bin: String = "") = client(
    protocol.Get(ns, key = key, set = set, bin = bin)) map {
    _ match {
      case m: Response => m.ops.headOption.map(_.value)
      case m@_ => throw new IllegalArgumentException("Unable to handle mssage: " + m)
    }
  }

  def set(key: String, value: ChannelBuffer, set: String = "", bin: String = "", expiration: Int = 0) = client(
    protocol.Set(ns, key = key, set = set, bin = bin, value = value, expiration = expiration)) map {
    _ match {
      case m: Response => m.header.resultCode
      case m@_ => throw new IllegalArgumentException("Unable to handle mssage: " + m)
    }
  }

  def delete(key: String, set: String = "", bin: String = "", expiration: Int = 0) = Future.value(0)

}


