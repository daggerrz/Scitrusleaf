package com.tapad.scitrusleaf.protocol

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._
import com.twitter.finagle.Codec
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder

object Logger {
  def info(msg: String) {
    println(msg)
  }

  def error(msg: String) {
    println("ERROR: " + msg)
  }
}

object Protocol {
  val INFO = 1
  val VERSION = 2
  val TYPE_MESSAGE = 3
}

object ClCodec extends Codec[ClMessage, ClMessage] {
  def pipelineFactory = new ChannelPipelineFactory {
    def getPipeline = {
      Channels.pipeline(
        MessageEncoder,
        ProtocolDecoder
      )
    }
  }
}

object ProtocolDecoder extends FrameDecoder {

  def decode(ctx: ChannelHandlerContext, channel: Channel, buf: ChannelBuffer): ClMessage = {
    if (buf.readableBytes() < 8) {
      null
    } else {
      buf.markReaderIndex()
      val version = buf.getByte(0)
      val messageType = buf.getByte(1)
      buf.readerIndex(2)
      val length = ClWireFormat.read48BitLong(buf).toInt
      if (buf.readableBytes() < length) {
        buf.resetReaderIndex()
        null
      } else {
        val header = ProtocolHeader(messageType, length)
        Logger.info("Receiving message of type " + messageType)
        messageType match {
          case Protocol.INFO => parseInfoMessage(header, buf)
          case Protocol.TYPE_MESSAGE => parseAsMessage(header, buf)
          case other =>
            Logger.error("Ignoring unknown message type " + other)
            null // TODO: This will just resume framing. How do we really discard the message?
        }
      }
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
    val h = Array.ofDim[Byte](headerSize - 1) // We just read the first byte
    buf.readBytes(h)

    Ack(MessageHeader(
      readFlags = h(0),
      writeFlags = h(1),
      responseFlags = h(2),
      resultCode = h(4),
      fieldCount = (h(17) & 0xff) << 8 | (h(18) & 0xff),
      opCount = (h(19) & 0xff) << 8 | (h(20) & 0xff)
    ))

  }
}

object MessageEncoder extends OneToOneEncoder {

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
    // TODO: Merge buffers ?
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

    msg.fields.foreach { case (typeId, value) =>
      writeInt(value.readableBytes() + 1)
      writeByte(typeId)
      writeBytes(value)
    }

    msg.ops.foreach { case (opId, data) =>
      writeInt(data.writableBytes() + 4) // + Bin name length
      writeByte(opId)
      writeByte(1)  // PARTICLE_TYPE_BLOB
      writeByte(0) // Bin name length
      // buf.writeNothing bin name
      writeBytes(data)
    }
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


