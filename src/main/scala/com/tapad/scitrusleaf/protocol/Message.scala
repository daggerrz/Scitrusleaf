package com.tapad.scitrusleaf.protocol

import org.jboss.netty.buffer.ChannelBuffers

sealed trait Op
case class Get()

case class Message(ops: Seq[Op])

case class MessageHeader(containsReads: Boolean,
                         containsWrites: Boolean,
                         containsResponse: Boolean,
                         resultCode: Int = 0,
                         generation: Int = 0,
                         expiration: Int = 0,
                         fieldCount: Int = 0,
                         opCount: Int = 0) {
  val readFlags : Byte = if (containsReads) 1 else 0
  val writeFlags: Byte = ((if (containsWrites) 1 else 0) | (if (generation != 0) 1 << 1 else 0)).asInstanceOf[Byte]
  val responseFlags : Byte = 0
}