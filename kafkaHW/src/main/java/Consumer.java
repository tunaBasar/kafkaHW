import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("Hello-topic"));

        ObjectMapper mapper =new ObjectMapper();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                try {
                    JsonNode jsonNode = mapper.readTree(record.value());
                    int operand = jsonNode.get("operand").asInt();
                    String operation = jsonNode.get("operation").asText();

                    boolean isFibonacci = isFibonacci(operand);
                    boolean isPrime = isPrime(operand);
                    if (operation.equals("fibPrime")) {
                        if (isFibonacci && isPrime) {
                            System.out.println(operand + "is both Fibonacci");

                        } else {
                            System.out.println(operand + "is not both fibonacci");

                        }
                        }else if (operation.equals("fibonacci")) {
                        if (isFibonacci) {
                            System.out.println(operand + " is fibonacci");
                        } else {
                            System.out.println(operand + "is not fibonacci");
                        }
                    }else if (operation.equals("prime")) {
                        if (isPrime) {
                            System.out.println(operand + " is Prime.");
                        } else {
                            System.out.println(operand + " is not Prime.");
                        }
                    } else {
                        System.out.println("Error: Invalid operation.");
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }

    }
    public static boolean isFibonacci(int n) {
        int a = 0, b = 1, c = 1;
        while (c < n) {
            a = b;
            b = c;
            c = a + b;
        }
        return c == n;
    }

    public static boolean isPrime(int n) {
        if (n <= 1) {
            return false;
        }
        for (int i = 2; i <= Math.sqrt(n); i++) {
            if (n % i == 0) {
                return false;
            }
        }
        return true;
    }
}

