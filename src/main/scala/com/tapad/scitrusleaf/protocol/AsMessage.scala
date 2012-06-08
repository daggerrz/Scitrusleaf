package com.tapad.scitrusleaf.protocol

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class ProtocolHeader(typeId: Int, messageLength: Long)

sealed trait ClMessage {
  def typeId : Int
}

trait AsMessage extends ClMessage {
  val typeId = Protocol.TYPE_MESSAGE

  def fields: List[(Int, ChannelBuffer)]
  def ops : List[(Int, ChannelBuffer)]
  def header : MessageHeader
}

object Fields {
  val NAMESPACE = 0
  val SET = 1
  val KEY = 2

  def string2TypedChannelBuffer(s: String) = {
    val buf = ChannelBuffers.buffer(s.length + 1)
    buf.writeByte(0x03) // UTF-8 flag
    buf.writeBytes(s.getBytes("UTF-8"))
    buf
  }

  def string2ChannelBuffer(s: String) = ChannelBuffers.copiedBuffer(s.getBytes("UTF-8"))

  def fieldList(namespace: String, key: String, bin: String) = List(
    NAMESPACE -> string2ChannelBuffer(namespace),
    SET -> ChannelBuffers.EMPTY_BUFFER,
    // Namespace and sets can only be string, but keys can have different types
    // and this needs to be flagged in the actual key byte buffer
    // We only support String keys for now.
    KEY -> string2TypedChannelBuffer(key)
  )
}

object Ops {
  val READ = 1
  val WRITE = 2
}

case class Set(namespace: String, key: String, value: ChannelBuffer, bin: String = "", expiration: Int = 0, generation: Int = 0) extends AsMessage {

  val fields = Fields.fieldList(namespace, key, bin)

  val ops = List(Ops.WRITE -> value)
  val header = MessageHeader(
    readFlags = 0.asInstanceOf[Byte],
    writeFlags = (1 | (if (generation != 0) 1 << 1 else 0)).asInstanceOf[Byte],
    responseFlags = 0,
    expiration = expiration,
    fieldCount = fields.length,
    opCount = ops.length
  )
}

case class Get(namespace: String, key: String, bin: String = "") extends AsMessage {
  val fields = Fields.fieldList(namespace, key, bin)
  val ops = List(Ops.READ -> ChannelBuffers.EMPTY_BUFFER)
  val header = MessageHeader(
    readFlags = 1.asInstanceOf[Byte],
    writeFlags = 0.asInstanceOf[Byte],
    responseFlags = 0,
    expiration = 0,
    fieldCount = fields.length,
    opCount = ops.length
  )
}


case class Ack(header: MessageHeader) extends AsMessage {
  val fields = List.empty
  val ops = List.empty
}


case class ClInfo(values: Map[String, String] = Map.empty) extends ClMessage {
  val typeId = Protocol.INFO
}


case class MessageHeader(readFlags: Byte,
                         writeFlags: Byte,
                         responseFlags: Byte,
                         resultCode: Int = 0,
                         generation: Int = 0,
                         expiration: Int = 0,
                         transactionTtl: Int = 0,
                         fieldCount: Int = 0,
                         opCount: Int = 0) {
}