package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServer = "127.0.0.1:9092";
//        System.out.println("Hello World");
//        Create the producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        Create a Producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);


//        Send data
        for(int i = 0;i<10;i++) {

            String topic = "first_topic";
            String value = "Hello there wassup";
            String key = "id_" + String.valueOf(i);

            //        Creating a producer record
            ProducerRecord<String,String> record = new ProducerRecord<>(topic,key,value);

            System.out.println("Key: " + key);
            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                executes successfully once complete or exception thrown
                    if (e == null) {
                        System.out.println("Recieved new metadata. \n" +
                                "Topic " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp()+
                                "--------------------------------------------\n");
                    } else {
                        System.out.println(e);
                    }
                }
            });

        }
//      flush data
        kafkaProducer.flush();

//        Flush and close application
        kafkaProducer.close();
    }
}
