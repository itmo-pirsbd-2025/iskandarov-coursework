package ru.analyticlabs.lab4;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.*;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    // Конфигурируем некий брокер в Websocket, при обращении по пути /topic/..
    // все подписанные клиенты получают сообщения с этого топика

    // config.setApplicationDestinationPrefixes определяет префиксы для сообщений,
    // которые клиент отправляет серверу, например, /app/someEndpoint
    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        config.enableSimpleBroker("/topic");
        config.setApplicationDestinationPrefixes("/app");
    }

    
    // Добавляем endpoint для подключения по Websocket - addEndpoint("/crypto")
    // Текущий путь: http://localhost:8084/crypto
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/crypto").setAllowedOriginPatterns("*").withSockJS();
    }

    @Override
    public boolean configureMessageConverters(java.util.List<org.springframework.messaging.converter.MessageConverter> messageConverters) {
        messageConverters.add(new MappingJackson2MessageConverter());
        return true;
    }
}
