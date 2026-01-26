package ru.analyticlabs.deserialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import ru.analyticlabs.dataModel.CryptoAggregatedData;

import java.io.IOException;

public class CryptoAggregatedDataDeserializer implements Deserializer<CryptoAggregatedData> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public CryptoAggregatedData deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, CryptoAggregatedData.class);
        } catch (IOException e) {
            e.printStackTrace();    // Handle the error
            return null;            // Return null or handle error as needed
        }
    }
}
