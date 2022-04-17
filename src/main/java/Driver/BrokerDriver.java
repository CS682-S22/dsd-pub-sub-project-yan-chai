package Driver;

import Broker.Broker;

import java.util.Scanner;

public class BrokerDriver {

    private Broker broker;

    public static void main(String[] args) {
        Broker broker = new Broker("config.properties");
        Thread t = new Thread(broker);
        t.start();
        System.out.println(broker.getLeader());
        Scanner sc =  new Scanner(System.in);
        String input;
        while (!(input = sc.next()).equals("exit")) {
            if (input.equals("fail")) {
                broker.fail();
            }
        }
        System.exit(0);
    }
}
