package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by Keswani on 3/26/2018.
 */

public class DHT_MetaData {

    String myKey;
    DHT_MetaData preDecessor;
    DHT_MetaData successor;
    boolean parent_changed;
    String portNumber;

    DHT_MetaData(String myKey, DHT_MetaData preDecessor, DHT_MetaData successor){
        this.myKey = myKey;
        this.preDecessor = preDecessor;
        this.successor = successor;
    }
    
}
