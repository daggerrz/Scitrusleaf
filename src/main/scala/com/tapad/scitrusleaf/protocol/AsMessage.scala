package com.tapad.scitrusleaf.protocol

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class ProtocolHeader(typeId: Int, messageLength: Long)

sealed trait ClMessage {
  def typeId : Int
}

trait AsMessage extends ClMessage {
  val typeId = Protocol.TYPE_MESSAGE

  def fields: Array[(Int, ChannelBuffer)]
  def ops : Array[Op]
  def header : MessageHeader
}

object Fields {
  val NAMESPACE = 0
  val SET = 1
  val KEY = 2


  def keyToChannelBuffer(s: String) = {
    val buf = ChannelBuffers.buffer(s.length + 1)
    buf.writeByte(0x03) // We only support strings, and this is type 0x03
    try {
      buf.writeBytes(s.getBytes("UTF-8"))
    } catch {
      case  e : Exception =>
        Logger.error("Error encoding key %s to channelbuffer".format(s, e))
        throw e
    }
    buf
  }

  def string2ChannelBuffer(s: String) = ChannelBuffers.copiedBuffer(s.getBytes("UTF-8"))

  def fieldList(namespace: String, set: String, key: String) = Array(
    NAMESPACE -> string2ChannelBuffer(namespace),
    SET -> string2ChannelBuffer(set),
    KEY -> keyToChannelBuffer(key)
  )
}

object Ops {
  val READ = 1
  val WRITE = 2
}
case class Op(opType: Int, bin: String, value: ChannelBuffer)

case class Set(namespace: String,
               set: String = "",
               key: String,
               bin: String = "",
               value: ChannelBuffer,
               expiration: Int = 0, generation: Int = 0) extends AsMessage {
  val fields = Fields.fieldList(namespace, set, key)
  val ops = Array(Op(Ops.WRITE, bin, value))

  val header = MessageHeader(
    readFlags = 0.asInstanceOf[Byte],
    writeFlags = (1 | (if (generation != 0) 1 << 1 else 0)).asInstanceOf[Byte],
    responseFlags = 0,
    expiration = expiration,
    fieldCount = fields.length,
    opCount = ops.length
  )
}

case class Get(namespace: String,
               set: String = "",
               key: String,
               bin: String = "") extends AsMessage {
  val fields = Fields.fieldList(namespace, set, key)
  val ops = Array(Op(Ops.READ, bin, ChannelBuffers.EMPTY_BUFFER))
  val header = MessageHeader(
    readFlags = 1.asInstanceOf[Byte],
    writeFlags = 0.asInstanceOf[Byte],
    responseFlags = 0,
    expiration = 0,
    fieldCount = fields.length,
    opCount = ops.length
  )
}


case class Response(header: MessageHeader, fields: Array[(Int, ChannelBuffer)], ops: Array[Op]) extends AsMessage

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