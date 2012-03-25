package com.tapad.scitrusleaf.protocol

import org.jboss.netty.buffer.ChannelBuffers
import com.twitter.finagle.memcached.util.ChannelBufferUtils

object Protocol {
  val VERSION = 2
  val TYPE_MESSAGE = 3
}

object MessageEncoder {
  def encode(message: Message) = {
    val buf = ChannelBuffers.dynamicBuffer(100)

    buf
  }

  /**
   * Writes the protocol header _and_ message header.
   */
  private[protocol] def encodeHeader(h: MessageHeader, sizeOfPayload: Long) = {
    val buf = ChannelBuffers.buffer(22) // Fixed 22 bytes
    import buf._

    // Protocol header
    writeByte(Protocol.VERSION)
    writeByte(Protocol.TYPE_MESSAGE)
    // Size of payload is 48 bits bytes
    writeBytes(as48BitLong(sizeOfPayload)) // TODO: C code is payload - protocol header

    // Message header
    writeBytes(Array(
      h.readFlags, h.writeFlags, h.responseFlags
    ))
    writeByte(0) // Unused
    writeShort(h.resultCode)
    writeInt(h.generation)
    writeInt(h.expiration)
    writeShort(h.fieldCount)
    writeShort(h.opCount)
    buf
  }

  /**
   * Returns the byte array representation of a big endian 48 bit long.
   */
  @inline def as48BitLong(value: Long) = {
    val array = new Array[Byte](6)
    array(0)     = (value >>> 40).asInstanceOf[Byte]
    array(1)     = (value >>> 32).asInstanceOf[Byte]
    array(2)     = (value >>> 24).asInstanceOf[Byte]
    array(3)     = (value >>> 16).asInstanceOf[Byte]
    array(4)     = (value >>> 8).asInstanceOf[Byte]
    array(5)     = (value >>> 0).asInstanceOf[Byte]
    array
  }
}


