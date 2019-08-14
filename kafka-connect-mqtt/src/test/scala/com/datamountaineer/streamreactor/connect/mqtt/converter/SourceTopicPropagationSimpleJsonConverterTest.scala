package com.datamountaineer.streamreactor.connect.mqtt.converter

import org.apache.kafka.connect.data.Struct
import org.scalatest.{Matchers, WordSpec}

class SourceTopicPropagationSimpleJsonConverterTest extends WordSpec with Matchers {

  "SourceTopicPropagationSimpleJsonConverter" should {

    "convert" in {
      val conv = new SourceTopicPropagationSimpleJsonConverter()
      val sourceTopic = "sourceTopic"
      val result = conv.convert(
        "kafkaTopic",
        sourceTopic,
        "messageId",
        """{"Walter":"x","Werner": 12.3}""".getBytes("UTF-8")
      )

      result.value().asInstanceOf[Struct].get("SourceTopic") shouldBe sourceTopic
    }

  }
}
