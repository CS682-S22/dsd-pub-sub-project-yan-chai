package Driver;

import Model.Config;
import Producer.Producer;

import java.util.Scanner;

public class producerDriver {

    public static void main(String[] args) {
        Config config = new Config("config.properties");
        Producer p = new Producer(config);
        Thread t = new Thread(p);
        t.start();
        Scanner sc =  new Scanner(System.in);
        String input;
        while (!(input = sc.next()).equals("q")) { }
    }
}
