package net.manub.embeddedkafka.streams

import java.util

import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.streams.kstream._
import org.json4s.DefaultFormats
import org.scalatest.{Matchers, WordSpec}

import scala.reflect.ClassTag

class ExampleKTableSpec extends WordSpec with Matchers with EmbeddedKafkaStreamsAllInOne {
  import ExampleKTableSpec._
  implicit val config = EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001)

  val (inTopic, outTopic, intermediateTableTopic) = ("toplevel-in", "toplevel-out", "intermediateTopic")
  implicit val stringSerde = Serdes.String()
  implicit val inputSerde = jsonSerde[Input]
  implicit val intermediateSerde = jsonSerde[Intermediate]
  implicit val outputSerde = jsonSerde[Output]

  case class Input(string: String, number: Int)
  case class Intermediate(string: String, number: Int, acc: Double)
  case class Output(string: String)

  val intermediateInitializer = new Initializer[Intermediate] {
    override def apply(): Intermediate = Intermediate("no-value", 0, 0)
  }

  val inputAggregator = new Aggregator[String, Input, Intermediate] {
    override def apply(aggKey: String, value: Input, aggregate: Intermediate): Intermediate = {
      val newTotal: Int = aggregate.number + 1
      Intermediate(
        value.string,
        newTotal,
        (aggregate.acc * aggregate.number + value.number) / newTotal
      )
    }
  }

  val intermediateToOuput = new ValueMapper[Intermediate, Output] {
    override def apply(aggregation: Intermediate): Output =
      Output(s"${aggregation.string}: Total[${aggregation.number}] - Mean[${aggregation.acc}]")
  }

  "A Kafka Stream test" should {
    "allow to consume internal KTable topics" in {
      val streamBuilder = new KStreamBuilder
      val stream: KStream[String, Input] = streamBuilder.stream(stringSerde, inputSerde, inTopic)
      stream.print(stringSerde, inputSerde)
      val aggregations: KTable[String, Intermediate] = stream.aggregateByKey(
        intermediateInitializer,
        inputAggregator,
        stringSerde,
        intermediateSerde,
        "intermediateAggregatesTable"
      )
      aggregations.print(stringSerde, intermediateSerde)

      val throughTable: KTable[String, Intermediate] = aggregations.through(
        stringSerde,
        intermediateSerde,
        intermediateTableTopic
      )
      throughTable.print(stringSerde, intermediateSerde)
      val outTable: KTable[String, Output] = throughTable.mapValues(intermediateToOuput)

      outTable.print(stringSerde, outputSerde)

      outTable.to(stringSerde, outputSerde, outTopic)

      runStreams(Seq(inTopic, outTopic), streamBuilder) {
        publishToKafka(inTopic, "one", Input("one", 1))
        publishToKafka(inTopic, "one", Input("one", 2))
        publishToKafka(inTopic, "one", Input("one", 3))

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(intermediateTableTopic)
          consumedMessages.take(3) should be (Seq(
            "one" -> """{"string":"one","number":1,"acc":1.0}""",
            "one" -> """{"string":"one","number":2,"acc":1.5}""",
            "one" -> """{"string":"one","number":3,"acc":2.0}"""
          ))
        }

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
          consumedMessages.take(3) should be (Seq(
            "one" -> """{"string":"one: Total[1] - Mean[1.0]"}""",
            "one" -> """{"string":"one: Total[2] - Mean[1.5]"}""",
            "one" -> """{"string":"one: Total[3] - Mean[2.0]"}"""
          ))
        }
      }
    }
  }

}

object ExampleKTableSpec {
  import org.json4s.native.Serialization.{read, write}
  import scala.reflect.runtime.universe.TypeTag
  val formats = DefaultFormats

  implicit def ser[T: Serde]: Serializer[T] = implicitly[Serde[T]].serializer()
  implicit def deser[T: Serde]: Deserializer[T] = implicitly[Serde[T]].deserializer()

  def jsonSerde[T <: AnyRef : TypeTag: ClassTag]: Serde[T] = new Serde[T] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
    override def close(): Unit = ()

    override def serializer(): Serializer[T] = new Serializer[T] {
      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
      override def serialize(topic: String, data: T): Array[Byte] = write(data)(formats).getBytes
      override def close(): Unit = ()
    }

    override def deserializer(): Deserializer[T] = new Deserializer[T] {
      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
      override def close(): Unit = ()
      override def deserialize(topic: String, data: Array[Byte]): T = {
        val json: String = new String(data)
        read(json)(formats, implicitly[Manifest[T]])
      }
    }
  }
}
