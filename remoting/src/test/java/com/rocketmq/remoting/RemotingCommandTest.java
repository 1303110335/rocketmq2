package com.rocketmq.remoting;

import com.rocketmq.remoting.exception.RemotingCommandException;
import com.rocketmq.remoting.protocol.SerializeType;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class RemotingCommandTest {

    @Test
    public void testMarkProtocolType_JSONProtocolType() {
        int resource = 261;
        SerializeType type = SerializeType.JSON;
        byte[] result = RemotingCommand.markProtocolType(resource, type);
        assertThat(result).isEqualTo(new byte[]{0, 0, 1, 5});
    }

    @Test
    public void testEncodeAndDecode_EmptyBody() {
        int code = 103;
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);
        ByteBuffer buffer = cmd.encode();

        // Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = RemotingCommand.decode(buffer);
        assertThat(decodedCommand.getSerializeTypeCurrentRPC()).isEqualTo(SerializeType.JSON);
        assertThat(decodedCommand.getBody()).isNull();

    }

    @Test
    public void testEncodeAndDecode_FilledBody() {

        int code = 103; //org.apache.rocketmq.common.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);
        cmd.setBody(new byte[]{0, 1, 2, 3, 4});

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = RemotingCommand.decode(buffer);

        assertThat(decodedCommand.getSerializeTypeCurrentRPC()).isEqualTo(SerializeType.JSON);
        assertThat(decodedCommand.getBody()).isEqualTo(new byte[]{0, 1, 2, 3, 4});
    }


    @Test
    public void testEncodeAndDecode_FilledBodyWithExtFields() throws RemotingCommandException {

        int code = 103; //org.apache.rocketmq.common.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new ExtFieldsHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);

        cmd.addExtField("key", "value");

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = RemotingCommand.decode(buffer);

        assertThat(decodedCommand.getExtFields().get("stringValue")).isEqualTo("bilibili");
        assertThat(decodedCommand.getExtFields().get("intValue")).isEqualTo("2333");
        assertThat(decodedCommand.getExtFields().get("longValue")).isEqualTo("23333333");
        assertThat(decodedCommand.getExtFields().get("booleanValue")).isEqualTo("true");
        assertThat(decodedCommand.getExtFields().get("doubleValue")).isEqualTo("0.618");

        assertThat(decodedCommand.getExtFields().get("key")).isEqualTo("value");

        CommandCustomHeader decodedHeader = decodedCommand.decodeCommandCustomHeader(ExtFieldsHeader.class);
        assertThat(((ExtFieldsHeader) decodedHeader).getStringValue()).isEqualTo("bilibili");
        assertThat(((ExtFieldsHeader) decodedHeader).getIntValue()).isEqualTo(2333);
        assertThat(((ExtFieldsHeader) decodedHeader).getLongValue()).isEqualTo(23333333l);
        assertThat(((ExtFieldsHeader) decodedHeader).isBooleanValue()).isEqualTo(true);
        assertThat(((ExtFieldsHeader) decodedHeader).getDoubleValue()).isBetween(0.617, 0.619);
    }
}

class ExtFieldsHeader implements CommandCustomHeader {
    private String stringValue = "bilibili";
    private int intValue = 2333;
    private long longValue = 23333333l;
    private boolean booleanValue = true;
    private double doubleValue = 0.618;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getStringValue() {
        return stringValue;
    }

    public int getIntValue() {
        return intValue;
    }

    public long getLongValue() {
        return longValue;
    }

    public boolean isBooleanValue() {
        return booleanValue;
    }

    public double getDoubleValue() {
        return doubleValue;
    }
}


class SampleCommandCustomHeader implements CommandCustomHeader {
    @Override
    public void checkFields() throws RemotingCommandException {
    }
}