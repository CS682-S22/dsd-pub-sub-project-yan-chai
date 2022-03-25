package Consumer;

import Broker.Broker;
import Model.Connection;
import Model.DataRecord;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Author: Haoyu Yan
 * push base consumer
 */
public class PushConsumer {

    private Connection connection;
    private static final Logger logger = LogManager.getLogger(Broker.class);

    public PushConsumer(String broker, int port, String topic) {
        connection = new Connection(broker, port);
        connection.send(DataRecord.Record.newBuilder().setTopic("push").setMsg(ByteString.copyFrom(topic.getBytes())).build().toByteArray());
    }

    public void start() {
        logger.info("push base start");
        while (true) {
            byte[] tmp = connection.receive();
            try {
                DataRecord.Record record = DataRecord.Record.parseFrom(tmp);
                logger.info(new String(record.getMsg().toByteArray()));
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }
}
