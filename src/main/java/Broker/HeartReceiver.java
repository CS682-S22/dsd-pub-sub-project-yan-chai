package Broker;

import Model.DataRecord;
import Model.FaultConnection;

import java.util.Date;
import java.util.concurrent.Callable;

public class HeartReceiver implements Callable<DataRecord.Record> {

    private FaultConnection connection;

    public HeartReceiver(FaultConnection connection) {
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
