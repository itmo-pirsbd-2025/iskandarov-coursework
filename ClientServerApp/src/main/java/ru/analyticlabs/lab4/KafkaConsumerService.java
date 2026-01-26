package ru.analyticlabs.lab4;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.annotation.SubscribeMapping;
import org.springframework.stereotype.Service;
import ru.analyticlabs.dataModel.CryptoAggregatedData;


import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

@Service
@Slf4j
public class KafkaConsumerService {

    //private static final String TOPIC = "crypto_data_aggregated";
    @Getter
    @Autowired
    private final SimpMessagingTemplate messagingTemplate;

    @Autowired
    private ConsumerFactory<String, CryptoAggregatedData> kafkaConsumerFactory;
    private final int CACHE_SIZE = 1000; // Максимальный размер кэша
    
    // Получение всех накопленных сообщений из кэша
    @Getter
    private final List<CryptoAggregatedData> dataCache = new ArrayList<>();

    public KafkaConsumerService(SimpMessagingTemplate messagingTemplate, ObjectMapper objectMapper) {
        this.messagingTemplate = messagingTemplate;
    }
    
    // Kafka Listener для потребления сообщений из Kafka
    @KafkaListener(topics = "crypto_data_aggregated")
    public void handleKafkaMessage(@Payload CryptoAggregatedData message) {
        log.info("Получено сообщение из Kafka: {}", message);
        System.out.println("Consumed data: {" + message + "}");

        if (dataCache.size() >= CACHE_SIZE) {
            dataCache.remove(0); // Удаляем самое старое сообщение
        }
        dataCache.add(message);

        // Сортируем кэш по event_time
        dataCache.sort(Comparator.comparing(CryptoAggregatedData::getEvent_time));

        // Отправляем сообщение всем подписанным WebSocket клиентам
        this.messagingTemplate.convertAndSend("/topic/updates", message);
    }
}
