package com.rcforte;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.ForeachWriter;

import java.util.Properties;

public class PriceForEachWriter extends ForeachWriter<Price> {
  private KafkaProducer<String,String> producer;

  @Override
  public boolean open(long partitionId, long epochId) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    this.producer = new KafkaProducer<>(props);
    return true;
  }

  @Override
  public void process(Price price) {
    producer.send(new ProducerRecord<>("PricesOutput", price.toJson()));
  }

  @Override
  public void close(Throwable errorOrNull) {
    producer.close();
  }
}
