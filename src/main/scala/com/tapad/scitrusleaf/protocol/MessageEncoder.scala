package com.tapad.scitrusleaf.protocol

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._
import com.twitter.finagle.Codec
import org.jboss.netty.handler.codec.frame.{CorruptedFrameException, FrameDecoder}
import org.jboss.netty.handler.codec.oneone.{OneToOneDecoder, OneToOneEncoder}
import org.slf4j.LoggerFactory

object Protocol {
  val INFO = 1
  val VERSION = 2
  val TYPE_MESSAGE = 3
}

object ClCodec extends Codec[ClMessage, ClMessage] {
  def pipelineFactory = new ChannelPipelineFactory {
    def getPipeline = {
      Channels.pipeline(
        new ClFrameDecoder,
        new ProtocolDecoder,
        new MessageEncoder
      )
    }
  }
}

class ClFrameDecoder extends FrameDecoder {
  private[this] val HEADER_LENGTH = 8
  def decode(ctx: ChannelHandlerContext, channel: Channel, buf: ChannelBuffer) = {
    if (buf.readableBytes() < HEADER_LENGTH) {
      null
    } else {
      val startReaderIndex = buf.readerIndex()
      // Skip version and message type bytes
      buf.skipBytes(2)
      // 6 byte length field
      val length = ClWireFormat.read48BitLong(buf).toInt
      val frameLength = length + HEADER_LENGTH
      buf.readerIndex(startReaderIndex)
      if (buf.readableBytes() < frameLength) {
        null
      } else {
        buf.readBytes(frameLength)
      }
    }
  }
}

class ProtocolDecoder extends OneToOneDecoder {
  def decode(ctx: ChannelHandlerContext, channel: Channel, msg: Any) = {
    val buf = msg.asInstanceOf[ChannelBuffer]
    val version = buf.readByte()
    val messageType = buf.readByte()
    val length = ClWireFormat.read48BitLong(buf).toInt
    val header = ProtocolHeader(messageType, length)
    val frame = buf.readBytes(length)
    messageType match {
      case Protocol.INFO => parseInfoMessage(header, frame)
      case Protocol.TYPE_MESSAGE => parseAsMessage(header, frame)
      case other =>
        val array = Array.ofDim[Byte](header.messageLength.toInt)
        buf.readBytes(array)
        throw new CorruptedFrameException("Don't know how to handle message of type " + other)
    }
  }


  def parseInfoMessage(header: ProtocolHeader, buf: ChannelBuffer) = {
    val array = Array.ofDim[Byte](header.messageLength.toInt)
    buf.readBytes(array)
    val data = new String(array, "UTF-8")
    val pairs = data.split('\n').flatMap { _.split("\t") match {
        case Array(key, value) => Some(key -> value)
        case a => None // Ignore invalid entries
      }
    }.toMap
    ClInfo(pairs)
  }

  def parseAsMessage(header: ProtocolHeader, buf: ChannelBuffer) = {
    val headerSize = buf.readByte().toInt
    val h = Array.ofDim[Byte](headerSize - 1) // We already read the first byte
    buf.readBytes(h)

    val header = MessageHeader(
      readFlags = h(0),
      writeFlags = h(1),
      responseFlags = h(2),
      resultCode = h(4),
      fieldCount = (h(17) & 0xff) << 8 | (h(18) & 0xff),
      opCount = (h(19) & 0xff) << 8 | (h(20) & 0xff)
    )

    var i = 0
    var n = 0

    val fields = Array.ofDim[(Int, ChannelBuffer)](header.fieldCount)
    i = 0
    n = header.fieldCount
    while (i < n)  {
      fields(i) = Codecs.FieldCodec.decode(buf)
      i += 1
    }

    val ops = Array.ofDim[Op](header.opCount)
    i = 0
    n = header.opCount
    while (i < n)  {
      ops(i) = Codecs.OpCodec.decode(buf)
      i += 1
    }

    Response(header, fields, ops)

  }
}

object Codecs {

  object FieldCodec {

    def encode(f: (Int, ChannelBuffer), buf: ChannelBuffer) {
      val fieldType = f._1
      val data = f._2
      buf.writeInt(data.readableBytes() + 1)
      buf.writeByte(fieldType)
      buf.writeBytes(data)
    }

    def decode(buf: ChannelBuffer) = {
      val len = buf.readInt()
      val typeId = buf.readByte()
      val value = typeId match {
        case Fields.SET =>
          val buf = ChannelBuffers.buffer(len - 2)
          // Discard the UTF-8 flag
          buf.readByte()
          buf.readBytes(buf, 0, len - 2)
          buf
        case _ =>
          val buf = ChannelBuffers.buffer(len - 1)
          buf.readBytes(buf, 0, len)
          buf
      }
      typeId.toInt -> value
    }
  }

  object OpCodec {
    def encode(op: Op, buf: ChannelBuffer) {
      val binName = Fields.string2ChannelBuffer(op.bin)
      buf.writeInt(op.value.readableBytes() + binName.readableBytes() + 4)
      buf.writeByte(op.opType)
      // TYPE_NULL = 0 for get, TYPE_BLOB = 4.
      // We use binary data for everything, so no matter.
      val dataType = if (op.opType == Ops.READ) 0 else 4
      buf.writeByte(dataType)
      buf.writeByte(0)  // Version, undocumented!
      buf.writeByte(binName.readableBytes()) // Bin name length
      buf.writeBytes(binName)// buf.writeNothing bin name
      buf.writeBytes(op.value)
    }

    def decode(buf: ChannelBuffer) = {
      val len = buf.readInt()
      val opId = buf.readByte()
      val dataTypeId = buf.readByte()
      val version = buf.readByte()
      val binNameLen = buf.readByte()
      val binNameBytes = Array.ofDim[Byte](binNameLen)
      buf.readBytes(binNameBytes)
      val data = buf.readBytes(buf.readableBytes())
      buf.readBytes(data)
      Op(opId.toInt, new String(binNameBytes, "ASCII"), data)
    }
  }
}

class MessageEncoder extends OneToOneEncoder {

  @throws(classOf[Exception])
  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: Any) = {

    val clMsg = msg.asInstanceOf[ClMessage]

    val payload = clMsg match {
      case m : ClInfo => ChannelBuffers.dynamicBuffer(0)
      case m : AsMessage => encodeAsMessage(m)
    }

    val buf = ChannelBuffers.dynamicBuffer(22 + payload.readableBytes()) // Fixed 22 bytes
    import buf._
    writeByte(Protocol.VERSION)
    writeByte(clMsg.typeId)
    writeBytes(ClWireFormat.as48BitLong(payload.readableBytes()))
    writeBytes(payload)
    buf
  }

  /**
   * Writes the protocol header _and_ message header.
   */
  private[protocol] def encodeAsMessage(msg: AsMessage) = {
    val h = msg.header
    val buf = ChannelBuffers.dynamicBuffer(200)
    import buf._

    writeByte(22) // Header size

    // Message header
    writeByte(h.readFlags)
    writeByte(h.writeFlags)
    writeByte(h.responseFlags)
    writeByte(0) // Unused
    writeByte(h.resultCode)
    writeInt(h.generation)
    writeInt(h.expiration)
    writeInt(h.transactionTtl)
    writeShort(h.fieldCount)
    writeShort(h.opCount)

    msg.fields.foreach { Codecs.FieldCodec.encode(_, buf) }
    msg.ops.foreach { Codecs.OpCodec.encode(_, buf) }
    buf
  }
}



object ClWireFormat {
  /**
   * Returns the byte array representation of a big endian 48 bit long.
   */
  @inline def as48BitLong(value: Long) = {
    val array = new Array[Byte](6)
    array(0) = (value >>> 40).asInstanceOf[Byte]
    array(1) = (value >>> 32).asInstanceOf[Byte]
    array(2) = (value >>> 24).asInstanceOf[Byte]
    array(3) = (value >>> 16).asInstanceOf[Byte]
    array(4) = (value >>> 8).asInstanceOf[Byte]
    array(5) = (value >>> 0).asInstanceOf[Byte]
    array
  }

  @inline def read48BitLong(buf: ChannelBuffer): Long = {
    val array = Array.ofDim[Byte](6)
    buf.readBytes(array)
    (array(0) & 0xff) << 40 | (array(1) & 0xff) << 32 | (array(2) & 0xff) << 24 |
    (array(3) & 0xff) << 16 | (array(4) & 0xff) << 8 | (array(5) & 0xff) << 0
  }
}


