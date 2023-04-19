package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class Deserializers<K, V> implements AutoCloseable {

    public final Deserializer<K> keyDeserializer;
    public final Deserializer<V> valueDeserializer;

    public Deserializers(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.keyDeserializer = Objects.requireNonNull(keyDeserializer, "Key deserializer provided to Deserializers should not be null");
        this.valueDeserializer = Objects.requireNonNull(valueDeserializer, "Value deserializer provided to Deserializers should not be null");
    }

    @SuppressWarnings("unchecked")
    public Deserializers(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, String clientId) {
        if (keyDeserializer == null) {
            this.keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            this.keyDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), true);
        } else {
            config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
            this.keyDeserializer = keyDeserializer;
        }

        if (valueDeserializer == null) {
            this.valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            this.valueDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), false);
        } else {
            config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
            this.valueDeserializer = valueDeserializer;
        }
    }

    @Override
    public void close() {
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        Utils.closeQuietly(keyDeserializer, "key deserializer", firstException);
        Utils.closeQuietly(valueDeserializer, "value deserializer", firstException);
        Throwable exception = firstException.get();

        if (exception != null) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close deserializers", exception);
        }
    }
}
