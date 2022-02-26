package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-seventh-application";
        String topic = "first_topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId); not required for seek
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

//        Assign and seek are mostly used to replay data or fetch a specific message

//        Create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
//      assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

//        seek
        long offsetToReadFrom = 15L;
        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        int numberOfMessageToRead = 5;
        int messageReadSoFar = 0;
        boolean notExitFromLoop = true;

//        poll for the new data
        while(notExitFromLoop)
        {
            ConsumerRecords<String, String> consumerRecord =  consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record : consumerRecord)
            {
                messageReadSoFar += 1;
                System.out.print("Key: " + record.key() + " value: " + record.value() + "\n");
                if(messageReadSoFar >= numberOfMessageToRead){
                    notExitFromLoop = false;
                    break;
                }
            }
        }
    }
}
