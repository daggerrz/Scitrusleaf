package com.tapad.scitrusleaf.protocol

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class ProtocolHeader(typeId: Int, messageLength: Long)

sealed trait ClMessage {
  def typeId : Int
}

trait AsMessage extends ClMessage {
  val typeId = Protocol.TYPE_MESSAGE

  def fields: List[(Int, String)]
  def ops : List[(Int, ChannelBuffer)]
  def header : MessageHeader
}

object Fields {
  val NAMESPACE = 0
  val SET = 1
  val KEY = 2
}

object Ops {
  val READ = 1
  val WRITE = 2
}

case class Set(namespace: String, key: String, value: ChannelBuffer, expiration: Int = 0, generation: Int = 0) extends AsMessage {
  val fields = List(Fields.NAMESPACE -> namespace, Fields.SET -> "", Fields.KEY -> key)
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

case class Get(namespace: String, key: String) extends AsMessage {
  val fields = List(Fields.NAMESPACE -> namespace, Fields.SET -> "", Fields.KEY -> key)
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