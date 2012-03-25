package com.tapad.scitrusleaf.protocol

import org.specs2.mutable.Specification

class MessageEncoderSpec extends Specification {

  "A message encoder" should {
    "encode a message header buffer correctly" in {
      val buf = MessageEncoder.encodeHeader(
        MessageHeader(
          containsReads = true,
          containsWrites = false,
          containsResponse = false
        ),
        0L
      )
      println(buf)
      buf.array().length must_== 16
    }
  }
}
