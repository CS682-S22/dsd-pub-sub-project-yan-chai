package Broker;

import Model.Config;
import Model.DataRecord;
import Model.FaultConnection;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MemberTable {

    private ConcurrentHashMap<Integer, FaultConnection> members;
    private Set<Integer> live;
    private Set<Integer> ackTable;
    private Config config;
    private volatile int leader;
    private volatile boolean isBusy;
    private static final Logger logger = LogManager.getLogger(MemberTable.class);

    public MemberTable(Config config) {
        isBusy = false;
        leader = -1;
        live = Collections.synchronizedSet(new HashSet<>());
        ackTable = Collections.synchronizedSet(new HashSet<>());
        this.config = config;
        numberTableInit(config.getBrokers(), config.getPorts());
    }

    private void numberTableInit(String[] brokers, int[] ports) {
        members = new ConcurrentHashMap<>();
        FaultConnection c = null;
        for (int i = 0; i < brokers.length; i ++) {
            if (i == config.getId()) {
                continue;
            }
            try {
                c = new FaultConnection(brokers[i], ports[i]);
            } catch (IOException ignored) { }
            if (c == null) {
                logger.info("Cannot connect to " + brokers[i]);
                continue;
            }
            c.send(DataRecord.Record.newBuilder().setId(config.getId()).setTopic("broker").setMsg(ByteString.EMPTY).build().toByteArray());
            try {
                DataRecord.Record record = DataRecord.Record.parseFrom(c.receive());
                leader = record.getId();
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            members.put(i, c);
            live.add(i);
        }
        if (members.size() == 0) {
            leader = config.getId();
        }
    }

    public ConcurrentHashMap<Integer, FaultConnection> getMembers() {
        return members;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public boolean isBusy() {
        return isBusy;
    }

    public void setBusy(boolean busy) {
        isBusy = busy;
    }

    public Set<Integer> getLive() {
        return live;
    }

    public void addNew(int i) {
        live.add(i);
    }

    public void down(int i) {
        live.remove(i);
    }

    public void addAck(int i) {
        ackTable.add(i);
    }

    public void clearAck() {
        ackTable.clear();
    }

}
