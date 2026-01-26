package ru.analyticlabs.deserialization;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import ru.analyticlabs.dataModel.CryptoData;

import java.io.IOException;

public class KafkaCryptoDataSchema extends AbstractDeserializationSchema<CryptoData> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    
    // Создаем ObjectMapper один раз по соображениям производительности
    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    /**
     Сейчас класс наследуется от AbstractDeserializationSchema и 
     получает только тело сообщения (byte[] message). 
     Если бы нашему методу десериализации требовался бы доступ к информации, 
     содержащейся в заголовках Kafka записи пользователя,
     мы бы реализовали Kafka Record Deserialization Schema вместо
     */
    @Override
    public CryptoData deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, CryptoData.class);
    }
}
