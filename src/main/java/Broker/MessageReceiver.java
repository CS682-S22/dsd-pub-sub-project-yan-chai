package Broker;

import Model.DataRecord;
import Model.FaultConnection;

import java.util.Date;
import java.util.concurrent.Callable;

/**
 * Author: Haoyu Yan
 * Reciver message in certain time slot, if not receive, it will time out
 */
public class MessageReceiver implements Callable<DataRecord.Record> {

    private FaultConnection connection;

    public MessageReceiver(FaultConnection connection) {
        this.connection = connection;
    }
    @Override
    public DataRecord.Record call() throws Exception {
        byte[] tmp = connection.receive();
        if (tmp == null) {
            return null;
        }
        return DataRecord.Record.parseFrom(tmp);
    }
}
