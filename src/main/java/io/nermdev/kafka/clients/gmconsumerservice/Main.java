package io.nermdev.kafka.clients.gmconsumerservice;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

@Slf4j
public class Main {

  static final class ConfigUtils {
    private ConfigUtils() {}

    public static void loadConfig(final String file, final Properties properties) {
      try (final FileInputStream inputStream = new FileInputStream(file)) {
        properties.load(inputStream);
      } catch (IOException e) {
        log.error(e.getMessage(), e);
        System.exit(1);
      }
    }

    public static Properties getProperties(final String[] args) {
      if (args.length < 1) throw new IllegalArgumentException("Pass path to application.properties");
      final Properties properties = new Properties();
      loadConfig(args[0], properties);
      return properties;
    }
  }

  private static void printRecord(final ConsumerRecord<byte[], byte[]> r) {
    log.info(
            Optional.ofNullable(r.key()).map(String::new).orElse("NULL")
                    + " --- " +
                    Optional.ofNullable(r.value()).map(String::new).orElse("NULL")
    );
  }

  private static void removeEntries(final Properties properties) {
    properties.remove("topics");
    properties.remove("partition.lag");
  }

  public static void main(String[] args) {
    final Properties properties = ConfigUtils.getProperties(args);
    final List<String> topics = Arrays.asList(properties.getProperty("topics").split("\\s*, \\s*"));
    final boolean lagOnZeroPartition = Optional.ofNullable(properties.getProperty("partition.lag")).isPresent();
    removeEntries(properties);
    final Map<TopicPartition, OffsetAndMetadata> progress = new HashMap<>();
    try (final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
      consumer.subscribe(topics);
      try {
        while (true) {
          final ConsumerRecords<byte[], byte[]> cRecords = consumer.poll(Duration.ofMillis(500));
          for (var r: cRecords) {
            printRecord(r);
            if (r.partition() > 0) {
              progress.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset()+1, null));
            }
            if (r.partition() == 0 && !lagOnZeroPartition) {
              progress.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset()+1, null));
            }
          }
          consumer.commitSync(progress);
        }
      } finally {
        log.info("Exiting forever loop");
        consumer.commitSync(progress);
      }
    } finally {
      log.info("Shutting down");
    }
  }
}
