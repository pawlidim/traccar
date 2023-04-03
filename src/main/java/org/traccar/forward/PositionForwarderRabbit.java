package org.traccar.forward;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.traccar.config.Config;
import org.traccar.config.Keys;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class PositionForwarderRabbit implements PositionForwarder {

    private final ConnectionFactory factory;
    private final ObjectMapper objectMapper;
    private final String exchangeName;
    private final String url;

    public PositionForwarderRabbit(Config config, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        exchangeName = config.getString(Keys.FORWARD_TOPIC);
        url = config.getString(Keys.FORWARD_URL);
        factory = new ConnectionFactory();
    }

    @Override
    public void forward(PositionData positionData, ResultHandler resultHandler) {
        try {
            factory.setUri(url);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            String key = Long.toString(positionData.getDevice().getId());
            String value = objectMapper.writeValueAsString(positionData);

            channel.exchangeDeclare(exchangeName, "direct", true);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, exchangeName, key);

            channel.basicPublish(exchangeName, key, true,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    value.getBytes());

            channel.close();
            connection.close();
            resultHandler.onResult(true, null);
        } catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException | IOException | TimeoutException e) {
            resultHandler.onResult(false, e);
        }


    }
}
