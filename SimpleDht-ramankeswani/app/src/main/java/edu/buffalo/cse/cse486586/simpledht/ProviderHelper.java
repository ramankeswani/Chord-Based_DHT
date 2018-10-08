package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.logging.SocketHandler;

/**
 * Created by Keswani on 3/26/2018.
 */

public class ProviderHelper {

    int size;
    String head_key;

    ProviderHelper(){
        size = 0;
    }

    DHT_MetaData insertFirstNode(String myKey, String myPort){
        DHT_MetaData dht_metaData = new DHT_MetaData(myKey, null, null);
        size++;
        Log.v("cp", "ProviderHelper insertFirstNode myKey: " + myKey);
        head_key = myKey;
        dht_metaData.portNumber = myPort;
        return dht_metaData;
    }

    DHT_MetaData insertNewNode(DHT_MetaData parent, String nodeKey, String port) {
        DHT_MetaData node = new DHT_MetaData(nodeKey, null, null);
        if(size == 1) {
            parent.successor = node;
            parent.preDecessor = node;
            node.preDecessor = parent;
            node.successor = parent;
            size++;
            if(parent.myKey.compareTo(node.myKey) > 0){
                node.parent_changed = true;
                parent = node;
                head_key = nodeKey;
                printList(parent);
            }
            Log.v("cp", "Inserted key: " + node.myKey + " Pre: " + node.preDecessor.myKey + " Suc: " + node.successor.myKey);
            node.portNumber = port;
            sendTableUpdates(parent);
            return node;
        } else if (parent.myKey.compareTo(node.myKey) > 0) {
            node.successor = parent;
            node.preDecessor = parent.preDecessor;
            parent.preDecessor.successor = node;
            parent.preDecessor = node;
            parent = node;
            head_key = nodeKey;
            size++;
            node.parent_changed = true;
            Log.v("cp", "Inserted key: " + node.myKey + " Pre: " + node.preDecessor.myKey + " Suc: " + node.successor.myKey);
            printList(parent);
            node.portNumber = port;
            sendTableUpdates(parent);
            return node;
        }
        else {
            DHT_MetaData currentNode = parent;
            for(int i=0; i<size; i++) {
                if(i == size-1) {
                    node.preDecessor = currentNode;
                    node.successor = parent;
                    parent.preDecessor = node;
                    currentNode.successor = node;
                    size++;
                    Log.v("cp", "Inserted key: " + node.myKey + " Pre: " + node.preDecessor.myKey + " Suc: " + node.successor.myKey);
                    printList(parent);
                    node.portNumber = port;
                    sendTableUpdates(parent);
                    return node;
                } else {
                    if(currentNode.myKey.compareTo(node.myKey) < 0 && node.myKey.compareTo(currentNode.successor.myKey) < 0) {
                        node.preDecessor = currentNode;
                        node.successor = currentNode.successor;
                        currentNode.successor.preDecessor = node;
                        currentNode.successor = node;
                        size++;
                        Log.v("cp", "Inserted key: " + node.myKey + " Pre: " + node.preDecessor.myKey + " Suc: " + node.successor.myKey);
                        printList(parent);
                        node.portNumber = port;
                        sendTableUpdates(parent);
                        return node;
                    } else {
                        currentNode = currentNode.successor;
                    }
                }
            }
        }
        Log.v("cp","ProviderHelper insertNewNode Something is wrong, did not insert anything");
        return null;
    }

    private class ClientTask extends AsyncTask<DHT_MetaData, Void, Void> {

        @Override
        protected Void doInBackground(DHT_MetaData... dhtMetaDatas) {
            DHT_MetaData node = dhtMetaDatas[0];
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.portNumber));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeInt(SimpleDHTConstants.MESSAGE_UPDATE_TABLE);
                objectOutputStream.writeUTF(node.preDecessor.portNumber);
                objectOutputStream.writeUTF(node.successor.portNumber);
                objectOutputStream.flush();
                Log.v("cp5","node.preDecessor.portNumber: " + node.preDecessor.portNumber);
                Log.v("cp5","node.successor.portNumber: " + node.successor.portNumber);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private void sendTableUpdates(DHT_MetaData parent){
        if(null != parent) {
            DHT_MetaData currentNode = parent;
            for (int i = 0; i < size; i++) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, currentNode);
                Log.v("cp5", "ProviderHelper sendTableUpdates currentNode: " + currentNode.myKey);
                currentNode = currentNode.successor;
            }
        }
    }

    public String findCorrectLocation(DHT_MetaData parent, String keyHashToInsert){
        String correctLocationPort = null;
        if(null == parent.successor) {
            correctLocationPort = parent.portNumber;
        } else if (keyHashToInsert.compareTo(parent.myKey) < 0) {
            correctLocationPort = parent.portNumber;
        } else {
            boolean found = false;
            DHT_MetaData currentNode = parent;
            do {
                if(keyHashToInsert.compareTo(currentNode.myKey) > 0 &&
                        keyHashToInsert.compareTo(currentNode.successor.myKey) < 0) {
                    correctLocationPort = currentNode.successor.portNumber;
                    found = true;
                    break;
                }
                currentNode = currentNode.successor;
            } while (!currentNode.myKey.equals(parent.myKey));
            if(!found) {
                correctLocationPort = parent.portNumber;
            }
        }
        return correctLocationPort;
    }

    public String findCorrectLocation_Others(String keyHashToInsert, String myKeyHash, String preDecHash, String sucHash, String sucPort, ContentValues contentValues) {
        Log.v("cp5","findCorrectLocation_Others");
        Log.v("cp5","keyHashToInsert: " + keyHashToInsert + " myKeyHash: " + myKeyHash + " sucHash: " + sucHash);
        String correctLocation;
        if(keyHashToInsert.compareTo(myKeyHash) > 0 && keyHashToInsert.compareTo(sucHash) < 0) {
            correctLocation = sucPort;
        } else if(keyHashToInsert.compareTo(myKeyHash) < 0 && keyHashToInsert.compareTo(preDecHash) > 0) {
            correctLocation = SimpleDHTConstants.INSERT_ON_SELF;
        } else if(keyHashToInsert.compareTo(myKeyHash) > 0 && myKeyHash.compareTo(sucHash) > 0) {
            correctLocation = SimpleDHTConstants.INSERT_ON_PARENT;
        } else {
            MetaDataForFind metaDataForFind = new MetaDataForFind(contentValues, sucPort);
            new FindLocationTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, metaDataForFind);
            correctLocation = "Somewhere I don't care and know";
        }
        return correctLocation;
    }

    private class FindLocationTask extends AsyncTask<MetaDataForFind, Void, Void> {

        @Override
        protected Void doInBackground(MetaDataForFind... MetaDataForFindArr) {
            MetaDataForFind metaDataForFind = MetaDataForFindArr[0];
            String remotePort = metaDataForFind.successorPort;
            String keyToInsert = metaDataForFind.contentValues.get("key").toString();
            String valueToInsert = metaDataForFind.contentValues.get("value").toString();
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeInt(SimpleDHTConstants.MESSAGE_FIND_TO_INSERT);
                objectOutputStream.writeUTF(keyToInsert);
                objectOutputStream.writeUTF(valueToInsert);
                objectOutputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    public String findLocation_ParentWasUpdated(String hashKeyToInsert, String myHashKey, String sucHash, String preDecHash, String sucPort, ContentValues contentValues) {
        String correctLocation;
        if(hashKeyToInsert.compareTo(myHashKey) < 0) {
            correctLocation = SimpleDHTConstants.INSERT_ON_SELF;
        } else if(hashKeyToInsert.compareTo(myHashKey) > 0 && hashKeyToInsert.compareTo(sucHash) < 0) {
            correctLocation = sucPort;
        } else if (hashKeyToInsert.compareTo(myHashKey) > 0 && myHashKey.compareTo(sucHash) > 0) {
            correctLocation = SimpleDHTConstants.INSERT_ON_SELF;
        } else if(hashKeyToInsert.compareTo(preDecHash) > 0) {
            correctLocation = SimpleDHTConstants.INSERT_ON_SELF;
        } else {
            MetaDataForFind metaDataForFind = new MetaDataForFind(contentValues, sucPort);
            new FindLocationTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, metaDataForFind);
            correctLocation = "I don't care or know";
        }
        return correctLocation;
    }

    public void notifyParentChanged(String parentPort, String remotePort) {
        Log.v("cp3","notifyParentChanged");
        new NotifyParentChangeTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, parentPort, remotePort);
    }

    private class NotifyParentChangeTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {
            String parentPort = strings[0];
            String remotePort = strings[1];
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeInt(SimpleDHTConstants.MESSAGE_NOTIFY_PARENT_CHANGED);
                objectOutputStream.writeUTF(parentPort);
                objectOutputStream.flush();
            } catch (IOException e){
                e.printStackTrace();
            }
            return null;
        }
    }

    public void sendFinalInsertRequest(String portToSend, ContentValues contentValues) {
        MetaDataForFind metaDataForFind = new MetaDataForFind(contentValues, portToSend);
        new SendFinalInsertRequestTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, metaDataForFind);
    }

    private class SendFinalInsertRequestTask extends AsyncTask<MetaDataForFind, Void, Void> {

        @Override
        protected Void doInBackground(MetaDataForFind... metaDataForFindArr) {
            String portToSend = metaDataForFindArr[0].successorPort;
            ContentValues contentValues = metaDataForFindArr[0].contentValues;
            String key = contentValues.get(SimpleDHTConstants.COLUMN_KEY).toString();
            String value = contentValues.get(SimpleDHTConstants.COLUMN_VALUE).toString();
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portToSend));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeInt(SimpleDHTConstants.MESSAGE_FINAL_INSERT);
                objectOutputStream.writeUTF(key);
                objectOutputStream.writeUTF(value);
                objectOutputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private void printList(DHT_MetaData parent) {
        Log.v("cp", "Print Starts");
        Log.v("cp", "size: " + size);
        Log.v("cp","myKey: " + parent.myKey);
        DHT_MetaData current = parent;
        boolean traversed = false;
        while(current != null) {
            if(traversed && current.myKey.equals(head_key)) {
                break;
            }
            Log.v("cp","key: " + current.myKey + " pre: " + current.preDecessor.myKey + " suc: " +
                    current.successor.myKey);
            current = current.successor;
            traversed = true;
        }
        Log.v("cp", "Print Ends");
    }

   /* public void requestingOthers_findCorrectLocation_Others(String keyHashToInsert, String myKeyHash, String preDecHash, String sucHash,
                                                            String sucPort, ContentValues contentValues) {
        String correctLocation;
        if(keyHashToInsert.compareTo(myKeyHash) > 0 && keyHashToInsert.compareTo(sucHash) < 0) {
            correctLocation = sucHash;
        } else {
            MetaDataForFind metaDataForFind = new MetaDataForFind(contentValues, keyHashToInsert, sucPort);
            new FindLocationTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, metaDataForFind);
        }
    }*/
}
