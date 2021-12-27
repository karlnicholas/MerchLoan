package com.github.karlnicholas.merchloan.servicerequest.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {
    @Value("${rabbitmq.servicerequest.queue}")
    private String servicerequestQueue;
    @Value("${rabbitmq.exchange}")
    private String exchange;
    @Value("${rabbitmq.servicerequest.routingkey}")
    private String servicerequestRoutingKey;
    @Value("${rabbitmq.username}")
    private String username;
    @Value("${rabbitmq.password}")
    private String password;
    @Value("${rabbitmq.host}")
    private String host;
    @Value("${rabbitmq.port}")
    private Integer port;
    @Value("${rabbitmq.virtual-host}")
    private String virtualHost;
    @Bean
    Queue servicerequestQueue() {
        return new Queue(servicerequestQueue, false);
    }
    @Bean
    Exchange exchange() {
        return ExchangeBuilder.directExchange(exchange).durable(false).build();
    }
    @Bean
    Binding binding() {
        return BindingBuilder
                .bind(servicerequestQueue())
                .to(exchange())
                .with(servicerequestRoutingKey)
                .noargs();
    }
    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(host, port);
        cachingConnectionFactory.setUsername(username);
        cachingConnectionFactory.setPassword(password);
        cachingConnectionFactory.setVirtualHost(virtualHost);
        return cachingConnectionFactory;
    }
//    @Bean
//    public MessageConverter jsonMessageConverter() {
//        return new Jackson2JsonMessageConverter();
//    }
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        final RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
//        rabbitTemplate.setMessageConverter(jsonMessageConverter());
        return rabbitTemplate;
    }
}