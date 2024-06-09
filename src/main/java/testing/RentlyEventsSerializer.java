package testing;

import common.Events;
import common.User;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.kinesis.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Objects;

public class RentlyEventsSerializer implements SerializationSchema<RentlyEvents>, DeserializationSchema<RentlyEvents> {

    private static final long serialVersionUID = 1L;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(RentlyEvents event) {
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize RentlyEvents", e);
        }
    }

    @Override
    public RentlyEvents deserialize(byte[] message) throws IOException {
        try {
            return objectMapper.readValue(message, RentlyEvents.class);
        } catch (IOException e) {
            throw new IOException("Failed to deserialize RentlyEvents", e);
        }
    }

    @Override
    public boolean isEndOfStream(RentlyEvents nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RentlyEvents> getProducedType() {
        return TypeInformation.of(RentlyEvents.class);
    }
}
