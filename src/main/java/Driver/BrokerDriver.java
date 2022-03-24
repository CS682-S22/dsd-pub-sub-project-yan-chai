package Driver;

import Broker.Broker;

/**
 * Author: Haoyu Yan
 * Broker driver
 */
public class BrokerDriver {

    public static void main(String[] args) {
         Broker broker = new Broker("config.properties");
         broker.start();
    }
}
