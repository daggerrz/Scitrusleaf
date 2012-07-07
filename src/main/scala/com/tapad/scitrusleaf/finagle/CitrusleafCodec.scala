package com.tapad.scitrusleaf.finagle

import com.twitter.finagle.Codec
import com.tapad.scitrusleaf.protocol._
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}

object CitrusleafCodec extends Codec[ClMessage, ClMessage] {
  def pipelineFactory = new ChannelPipelineFactory {
    def getPipeline = {
      Channels.pipeline(
        new ClFrameDecoder,
        new ClMessageDecoder,
        new ClMessageEncoder
      )
    }
  }
}
