package Broker;

import Model.Message;

import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Broker {

    HashMap<String, CopyOnWriteArrayList<Message.Msg>[]> map;

}
