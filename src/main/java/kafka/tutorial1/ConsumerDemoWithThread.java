package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
       new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {
    }

    private void run()
    {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-fifth-application";
        String topic = "first_topic";

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Runnable consumerThread = new ConsumerThread(
                groupId,
                bootstrapServer,
                topic,
                countDownLatch
        );

//        Start the thread
        Thread myThread = new Thread(consumerThread);
        myThread.start();

//     Add Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            System.out.println("Caught Shutdown hook");

            ((ConsumerThread) consumerThread).shutdown();

            try{
                countDownLatch.await();
            }catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            System.out.println("Application has exited");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            System.out.println("Application got interrupted");
            System.out.println(e);
        }finally {
            System.out.println("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable
    {
       private CountDownLatch latch;
        //        Create a consumer
        KafkaConsumer<String,String> consumer;

        public ConsumerThread(String groupId,
                              String bootstrapServer,
                              String topic,
                              CountDownLatch latch) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            consumer = new KafkaConsumer<String, String>(properties);
            //        subscribe consumer to topic
            consumer.subscribe(Arrays.asList(topic));

        }

        @Override
        public void run() {
            //        poll for the new data
            try{
                while(true)
                {
                    ConsumerRecords<String, String> consumerRecord =  consumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String,String> record : consumerRecord)
                    {
                        System.out.print("Key: " + record.key() + " value: " + record.value() + "\n");

                    }
                }
            }catch (WakeupException e)
            {
                System.out.println("Recieved the shutdown signal");
            }finally {
                consumer.close();

                latch.countDown();
            }


        }

        public void shutdown()
        {
//            THe wakeup is a special method to interrupt consumer.poll()
//            It will throw exception WakeUpException
            consumer.wakeup();
        }
    }
}
