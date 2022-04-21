package Broker;

import Model.Config;
import Model.DataRecord;
import Model.FaultConnection;
import com.google.protobuf.ByteString;

import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.*;

public class ReceivingServer implements Runnable{

    private MemberTable table;
    private Config config;
    private ConcurrentHashMap<Integer, Date> timeTable;
    private HashSet<Integer> added;
    private Storage storage;
    private boolean running;
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    
    public ReceivingServer(MemberTable table, Config config, Storage storage) {
        this.table = table;
        this.config = config;
        this.storage = storage;
        running = true;
        added = new HashSet<>();
        timeTable = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        Date date = new Date();
        for (int i = 0; i < config.getNumber(); i ++) {
            timeTable.put(i, date);
        }
        while (running) {
            for (int i = 0; i < config.getNumber(); i ++) {
                if (added.contains(i)) {
                    continue;
                }
                FaultConnection c = table.getMembers().get(i);
                if (i != config.getId() && c != null) {
                    int finalI = i;
                    Thread t = new Thread(() -> {
                        Future future = null;
                        while (true) {
                            future = executor.submit(new MessageReceiver(c));
                            DataRecord.Record rec = null;
                            try {
                                rec = (DataRecord.Record) future.get(2000, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (ExecutionException e) {
                                e.printStackTrace();
                            } catch (TimeoutException e) {
                                future.cancel(true);
                            }
                            if (rec == null) {
                                if (new Date().getTime() - timeTable.get(finalI).getTime() > 5000) {
                                    table.getMembers().remove(finalI);
                                    System.out.println("remove " + finalI);
                                    if (finalI == table.getLeader()) {
                                        table.setLeader(-1);
                                        table.setBusy(true);
                                    }
                                    return;
                                }
                            } else {
                                timeTable.put(rec.getId(), new Date());
                                if (rec.getTopic().equals("election")) {
                                    table.setLeader(-1);
                                    table.setBusy(true);
                                } else if (rec.getTopic().equals("newLeader")) {
                                    table.setLeader(rec.getId());
                                    System.out.println("Set leader " + rec.getId());
                                    byte[] tmp = new byte[1];
                                    tmp[0] = (byte) storage.getVersion();
                                    table.getMembers().get(rec.getId()).send(DataRecord.Record.newBuilder().setId(config.getId()).setTopic("sync").setMsg(ByteString.copyFrom(tmp)).build().toByteArray());
                                } else if (rec.getMsg() != ByteString.EMPTY) {
                                    System.out.println(rec);
                                    storage.put(rec.getTopic(), rec.getMsg());
                                }
                            }
                        }
                    });
                    added.add(i);
                    t.start();
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
