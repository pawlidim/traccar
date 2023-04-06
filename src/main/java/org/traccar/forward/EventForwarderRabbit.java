package org.traccar.forward;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class EventForwarderRabbit implements EventForwarder {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventForwarderRabbit.class);

    private final ConnectionFactory factory;
    private final ObjectMapper objectMapper;
    private final String exchangeName;
    private final String routingKey;
    private final String url;

    public EventForwarderRabbit(Config config, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        exchangeName = config.getString(Keys.EVENT_FORWARD_TOPIC);
        routingKey = config.getString(Keys.EVENT_FORWARD_ROUTING);
        url = config.getString(Keys.EVENT_FORWARD_URL);
        factory = new ConnectionFactory();
    }

    @Override
    public void forward(EventData eventData, ResultHandler resultHandler) {
        try {
            factory.setUri(url);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            //String key = Long.toString(eventData.getDevice().getId());
            String value = objectMapper.writeValueAsString(eventData);

            channel.exchangeDeclare(exchangeName, "direct", true);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, exchangeName, routingKey);

            LOGGER.info("Create connection to RabbitMQ.... try to send event message to exchange " + exchangeName + " with routing " + routingKey);

            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json").build();
            channel.basicPublish(exchangeName, routingKey, true, properties, value.getBytes());

            channel.close();
            connection.close();
            resultHandler.onResult(true, null);
        } catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException | IOException | TimeoutException e) {
            resultHandler.onResult(false, e);
        }
    }
}
