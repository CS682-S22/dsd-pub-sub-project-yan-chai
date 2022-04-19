package Broker;

import Model.Config;
import Model.Connection;
import Model.DataRecord;
import Model.FaultConnection;
import com.google.protobuf.ByteString;

import java.util.*;
import java.util.concurrent.*;

public class HeartBeatServer implements Runnable{

    private MemberTable table;
    private Config config;
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    public HeartBeatServer(MemberTable table, Config config) {
        this.table = table;
        this.config = config;
    }

    private void sendHb(FaultConnection c) {
        c.sendHeartBeat(config.getId());
        System.out.println("broker " + config.getId() + " send heart beat.");
    }

    @Override
    public void run() {
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                if (table.getLeader() == -1) {
                    System.out.println("electing");
                    for (int i = 0; i < config.getId(); i ++) {
                        FaultConnection c = table.getMembers().get(i);
                        if (c == null) {
                            continue;
                        }
                        sendHb(c);
                    }
                    boolean hasUpper = false;
                    for (int i = config.getId()+1; i < config.getNumber(); i ++) {
                        FaultConnection c = table.getMembers().get(i);
                        if (c != null) {
                            hasUpper = true;
                            c.send(DataRecord.Record.newBuilder().setId(config.getId()).setTopic("election").setMsg(ByteString.EMPTY).build().toByteArray());
                        }
                    }
                    if (!hasUpper) {
                        table.setLeader(config.getId());
                        for (int i = 0; i < config.getNumber(); i ++) {
                            FaultConnection c = table.getMembers().get(i);
                            if (c == null) {
                                continue;
                            }
                            c.send(DataRecord.Record.newBuilder().setId(config.getId()).setTopic("newLeader").setMsg(ByteString.EMPTY).build().toByteArray());
                        }
                    }
                }
                for (int i = 0; i < config.getNumber(); i ++) {
                    FaultConnection c = table.getMembers().get(i);
                    if (c == null) {
                        continue;
                    }
                    sendHb(c);
                }
            }
        };
        timer.schedule(task, 0, 1000);


    }
}
