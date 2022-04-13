
import org.apache.kafka.clients.producer.ProducerRecord
import zio.ZIOAppDefault

object OfficialKafka extends ZIOAppDefault{
  import zio._
  import zio.kafka.consumer._
  import zio.kafka.producer._
  import zio.kafka.serde._
  import zio.Console._

  val settings: ConsumerSettings =
    ConsumerSettings(List("localhost:9092"))
      .withGroupId("group")
      .withClientId("client")
      .withCloseTimeout(30.seconds)


  val managedConsumer =
    Consumer.make(settings)

  val consumer =
    ZLayer.scoped(managedConsumer)


  val subscription = Subscription.topics("TestTopic")

  //consumer
  val data = Consumer.subscribeAnd(Subscription.topics("TestTopic"))
    .plainStream(Serde.string, Serde.string)
    .tap(cr => printLine(s"key: ${cr.record.key}, value: ${cr.record}"))
    .map(_.offset)
    .aggregateAsync(Consumer.offsetBatches)
    .mapZIO(_.commit)
    .runDrain

  val partitionData = {
  Consumer.subscribeAnd(Subscription.topics("TestTopic"))
    .partitionedStream(Serde.string, Serde.string)
    .tap(tpAndStr => printLine(s"topic: ${tpAndStr._1.topic}, partition: ${tpAndStr._1.partition}"))
    .flatMap(_._2)
    .tap(cr => printLine(s"key: ${cr.record.key}, value: ${cr.record.value}"))
//    .map(_.offset)
//    .aggregateAsync(Consumer.offsetBatches)
//    .mapZIO(_.commit)
    .runDrain


  }
  //producer
  val producerSettings: ProducerSettings = ProducerSettings(List("localhost:9092"))
  val producer = ZLayer.scoped(Producer.make(producerSettings))//, Serde.int, Serde.string))

  val prod = Producer.produce("my-topic",key = 1,value = "two",Serde.int,Serde.string)
def producer() = {
 val producerRecord: ProducerRecord[Int, String] = new ProducerRecord("my-output-topic", "key", "newValue")
 (producerRecord,"j").mapChunksZIO { chunk =>
 val records     = chunk.map(_._1)
 val offsetBatch = OffsetBatch(chunk.map(_._2).toSeq)

 Producer.produceChunk[Any, Int, String](records) *> offsetBatch.commit.as(Chunk(()))
}
 .runDrain
 .provideSomeLayer(producer)
}


  //consuming then send to producer
  import zio.ZLayer
  import zio.kafka.consumer._
  import zio.kafka.producer._
  import zio.kafka.serde._
  import org.apache.kafka.clients.producer.ProducerRecord

  val consumerSettings: ConsumerSettings = ConsumerSettings(List("localhost:9092")).withGroupId("group")
//  val producerSettings: ProducerSettings = ProducerSettings(List("localhost:9092"))

  val consumerAndProducer =
    ZLayer.scoped(Consumer.make(consumerSettings)) ++
      ZLayer.scoped(Producer.make(producerSettings))//, Serde.string, Serde.string))

  val consumeProduceStream = Consumer
    .subscribeAnd(Subscription.topics("TestTopic"))
    .plainStream(Serde.int, Serde.string)
    .map { record =>
      val key    = record.record.key()
      val value = record.record.value()
      val newValue = value.split(" ").length.toString

      val producerRecord: ProducerRecord[Int, String] = new ProducerRecord("my-topic", newValue)
      (producerRecord, record.offset)
    }
    .mapChunksZIO { chunk =>
      val records     = chunk.map(_._1)
      val offsetBatch = OffsetBatch(chunk.map(_._2).toSeq)

      Producer.produceChunk[Any, Int, String](records,Serde.int, Serde.string) *> offsetBatch.commit.as(Chunk(()))
    }
    .runDrain
//    .provideSomeLayer(consumerAndProducer ++ Console.live )

 def run  = consumeProduceStream.provideSomeLayer(consumerAndProducer ++ Console.live)

}
