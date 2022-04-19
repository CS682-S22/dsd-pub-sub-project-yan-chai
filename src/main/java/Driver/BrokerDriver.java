package Driver;

import Broker.*;
import Model.Config;

import java.util.Scanner;

public class BrokerDriver {

    private Broker broker;

    public static void main(String[] args) {
        Config config = new Config("config.properties");
        MemberTable table = new MemberTable(config);
        Storage storage = new Storage();
        Broker broker = new Broker(table, config, storage);
        ReceivingServer rs = new ReceivingServer(table, config, storage);
        HeartBeatServer hbs =  new HeartBeatServer(table, config);
        System.out.println(table.getLeader());
        Thread t1 = new Thread(broker);
        Thread t2 = new Thread(rs);
        Thread t3 = new Thread(hbs);
        t1.start();
        t2.start();
        t3.start();

        /*Broker broker = new Broker("config.properties");
        Thread t = new Thread(broker);
        t.start();
        System.out.println(broker.getLeader());*/
        Scanner sc =  new Scanner(System.in);
        String input;
        while (!(input = sc.next()).equals("exit")) {
            if (input.equals("fail")) {
                broker.fail();
            }
        }
        System.out.println(table.getLeader());
        System.exit(0);
    }
}
