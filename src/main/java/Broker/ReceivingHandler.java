package Broker;

import Model.Config;
import Model.DataRecord;
import Model.FaultConnection;
import com.google.protobuf.ByteString;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.*;

public class ReceivingHandler implements Runnable{

    private int id;
    private Config config;
    private FaultConnection connection;
    private MemberTable table;
    private Storage storage;
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private Sync sync;

    public ReceivingHandler(int id, Config config, MemberTable table, FaultConnection connection, Storage storage) {
        this.id = id;
        this.config = config;
        this.table = table;
        this.connection = connection;
        this.storage = storage;
        sync = new Sync();

    }

    @Override
    public void run() {
        Date date = new Date();
        Future<DataRecord.Record> future = null;
        while (true) {
            future = executor.submit(new MessageReceiver(connection));
            DataRecord.Record rec = null;
            try {
                rec = future.get(2000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
            if (rec == null) {
                if (new Date().getTime() - date.getTime() > 5000) {
                    table.down(id);
                    System.out.println("remove " + id);
                    table.getMembers().remove(id);
                    if (id == table.getLeader()) {
                        table.setLeader(-1);
                        table.setBusy(true);
                    }
                    return;
                }
            } else {
                date = new Date();
                if (rec.getTopic().equals("election")) {
                    table.setLeader(-1);
                    table.setBusy(true);
                } else if (rec.getTopic().equals("newLeader")) {
                    table.setLeader(rec.getId());
                    System.out.println("Set leader " + rec.getId());
                    connection.send(DataRecord.Record.newBuilder().setId(config.getId()).setTopic("sync").setMsg(ByteString.copyFrom(storage.getInfo().getBytes(StandardCharsets.UTF_8))).build().toByteArray());
                } else if (rec.getTopic().equals("sync")) {
                    sync.sync(storage.getInfo());
                    sync.newSync();
                    sync.sync(rec.getMsg().toString(StandardCharsets.UTF_8));
                    if (sync.getSyncCount() == table.getLive().size() - 1) {
                        for (int i : table.getLive()) {
                            FaultConnection c = table.getMembers().get(i);
                            connection.send(DataRecord.Record.newBuilder().setId(config.getId()).setTopic("info").setMsg(ByteString.copyFrom(sync.toString().getBytes(StandardCharsets.UTF_8))).build().toByteArray());
                        }
                        sync = new Sync();
                        table.setBusy(false);
                    }
                } else if (rec.getTopic().equals("info")) {
                    System.out.println("Sync!!!");
                    String[] tmp = rec.getMsg().toString(StandardCharsets.UTF_8).split(",");
                    for (int i = 0; i < tmp.length; i += 2) {
                        int compare = storage.getVersion(tmp[i]) - Integer.parseInt(tmp[i+1]);
                        for (int j = storage.getVersion(tmp[i])-1; j > compare; j++) {
                            storage.rollback(tmp[i]);
                        }
                    }
                    table.setBusy(false);
                } else if (rec.getMsg() != ByteString.EMPTY && !table.isBusy()) {
                    System.out.println(rec);
                    storage.put(rec.getTopic(), rec.getMsg());
                }
            }
        }
    }
}
