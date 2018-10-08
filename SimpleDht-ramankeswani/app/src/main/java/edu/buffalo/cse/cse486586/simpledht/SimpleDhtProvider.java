package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.SimpleCursorAdapter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

public class SimpleDhtProvider extends ContentProvider {

    SimpleDHT_DBHelper dbHelper;
    static String myNode_ID;
    boolean firstNodeCreated, isServerSetUp;
    ProviderHelper providerHelper;
    DHT_MetaData dht_metaData_Parent;
    final static String FIRST_NODE = "5554";
    static String FIRST_NODE_PORT;
    static String ring_Parent_Port;
    static final int SERVER_PORT = 10000;
    String myPort_Socket, myHash;
    String preDecPort, sucPort, preDecHash, sucHash;
    HelperClassToInsert helperClassToInsert;
    HelperForQuery helperForQuery;
    HelperForDeleteAll helperForDeleteAll;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.v("cp1", "Delete starts selection: " + selection);
        int deleteCount = 0;
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        if(null == preDecHash) {
            if(SimpleDHTConstants.allNodes.equals(selection) || SimpleDHTConstants.ownPartition.equals(selection)) {
                Log.v("cp1", "delete null == preDecHash ownPartition S");
                deleteCount = db.delete(SimpleDHTConstants.TABLE_NAME, null, null);
                Log.v("cp1", "delete null == preDecHash deleteCount: " + deleteCount);
                getContext().getContentResolver().notifyChange(uri, null);
                Log.v("cp1", "delete null == preDecHash ownPartition E");
            } else {
                deleteCount = db.delete(SimpleDHTConstants.TABLE_NAME, SimpleDHTConstants.COLUMN_KEY + " LIKE ? ", new String[]{selection});
                Log.v("cp1", "delete  null == preDecHash else deleteCount: " + deleteCount);
                getContext().getContentResolver().notifyChange(uri, null);
            }
            return deleteCount;
        }
        try {
            boolean deleteFromSelf = false;
            if (SimpleDHTConstants.allNodes.equals(selection)) {
                helperForDeleteAll.callDelete("@", getContext().getContentResolver());
                helperForDeleteAll.deleteAllData(myPort_Socket, sucPort);
            } else if (SimpleDHTConstants.ownPartition.equals(selection)) {
                Log.v("cp1", "delete ownPartition S");
                deleteCount = db.delete(SimpleDHTConstants.TABLE_NAME, null, null);
                Log.v("cp1", "delete else @ deleteCount: " + deleteCount);
                getContext().getContentResolver().notifyChange(uri, null);
                Log.v("cp1", "delete ownPartition E");
            } else {
                String hash = genHash(selection);
                Log.v("cp1", "delete else selection: " + selection + " hash: " + hash);
                if(myPort_Socket.equals(FIRST_NODE_PORT) && myPort_Socket.equals(ring_Parent_Port)){
                    String correctLocation = helperForDeleteAll.findCorrectLocation(dht_metaData_Parent, hash);
                    Log.v("cp1", "delete FIRST_NODE_PORT correctLocation: " + correctLocation);
                    if(SimpleDHTConstants.DELETE_FROM_SELF.equals(correctLocation)) {
                        deleteFromSelf = true;
                    } else  {
                        helperForDeleteAll.deleteFromOthersLocFinal(correctLocation, selection);
                    }
                } else if(myPort_Socket.equals(ring_Parent_Port)) {
                    String correctLocation = helperForDeleteAll.findLocation_ParentWasUpdated(hash, myHash, sucHash, preDecHash, sucPort, selection);
                    Log.v("cp1", "delete ring_Parent_Port correctLocation: " + correctLocation);
                    if(SimpleDHTConstants.DELETE_FROM_SELF.equals(correctLocation)){
                        deleteFromSelf = true;
                    } else if(SimpleDHTConstants.DELETION_PERFORMED_ON_OTHER.equals(correctLocation)) {
                        Log.v("cp1", "delete DELETION_PERFORMED_ON_OTHER");
                    } else {
                        helperForDeleteAll.deleteFromOthersLocFinal(correctLocation, selection);
                    }
                } else {
                    String correctLocation = helperForDeleteAll.findCorrectLocation_Others(hash, myHash, preDecHash, sucHash, sucPort, selection);
                    Log.v("cp1", "delete else correctLocation: " + correctLocation);
                    if(SimpleDHTConstants.DELETE_FROM_SELF.equals(correctLocation)) {
                        deleteFromSelf = true;
                    } else if(SimpleDHTConstants.DELETE_FROM_PARENT.equals(correctLocation)) {
                        helperForDeleteAll.deleteFromOthersLocFinal(ring_Parent_Port, selection);
                    } else if(SimpleDHTConstants.DELETION_PERFORMED_ON_OTHER.equals(correctLocation)){
                        Log.v("cp1", "delete else DELETION_PERFORMED_ON_OTHER: ");
                    } else {
                        helperForDeleteAll.deleteFromOthersLocFinal(correctLocation, selection);
                    }
                }
                if(deleteFromSelf) {
                    deleteCount = db.delete(SimpleDHTConstants.TABLE_NAME, SimpleDHTConstants.COLUMN_KEY + " LIKE ? ", new String[]{selection});
                    Log.v("cp1", "delete else deleteCount: " + deleteCount);
                    getContext().getContentResolver().notifyChange(uri, null);
                }
            }
        } catch (Exception e) {
            Log.v("cp1" , "delete exception " + e.toString());
        }
        return deleteCount;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String portToInsertOn;
        boolean insertHere = false;
        try {
            String hash = genHash(values.get("key").toString());
            Log.v("cp5", "TOCHECK key: " + values.get("key").toString() + " hash: " + hash);
            Log.v("cp5", "myPort_Socket: " + myPort_Socket + " ring_Parent_Port: " + ring_Parent_Port);
            Log.v("cp5", "preDecHash: " + preDecHash);
            if (null == preDecHash) {
                insertHere = true;
            }
            if (null != myPort_Socket && null != preDecHash) {
                Log.v("cp5", "myPort_Socket: " + myPort_Socket + " ring_Parent_Port: " + ring_Parent_Port);
                if (null != dht_metaData_Parent && myPort_Socket.equals(FIRST_NODE_PORT)) {
                    portToInsertOn = providerHelper.findCorrectLocation(dht_metaData_Parent, hash);
                    Log.v("cp2", "Message Key: " + values.get("key") + " Correct Port: " + portToInsertOn + " Hash: " + hash);
                    if (myPort_Socket.equals(portToInsertOn)) {
                        insertHere = true;
                    } else {
                        providerHelper.sendFinalInsertRequest(portToInsertOn, values);
                    }
                } else if (null != ring_Parent_Port && myPort_Socket.equals(ring_Parent_Port)) {
                    portToInsertOn = providerHelper.findLocation_ParentWasUpdated(hash, myHash, sucHash, preDecHash, sucPort, values);
                    Log.v("cp2", "Parent_was_updated Message Key: " + values.get("key") + " Correct Port: " + portToInsertOn + " Hash: " + hash);
                    if (portToInsertOn.equals(SimpleDHTConstants.INSERT_ON_SELF)) {
                        insertHere = true;
                    } else if(!("I don't care or know".equals(portToInsertOn))){
                        providerHelper.sendFinalInsertRequest(portToInsertOn, values);
                    }

                } else if (!myPort_Socket.equals(ring_Parent_Port)) {
                    Log.v("cp3", "SimpleDhtProvider insert: Finding correct location I am not the parent");
                    Log.v("cp3", "SimpleDhtProvider insert: myHashL: " + myHash + " key hash: " + hash);
                    String correctLocation = providerHelper.findCorrectLocation_Others(hash, myHash, preDecHash, sucHash, sucPort, values);
                    Log.v("cp3", "SimpleDhtProvider insert: correctLocation: " + correctLocation);
                    if (sucPort.equals(correctLocation)) {
                        providerHelper.sendFinalInsertRequest(correctLocation, values);
                    } else if (SimpleDHTConstants.INSERT_ON_SELF.equals(correctLocation)) {
                        insertHere = true;
                    } else if (SimpleDHTConstants.INSERT_ON_PARENT.equals(correctLocation)) {
                        providerHelper.sendFinalInsertRequest(ring_Parent_Port, values);
                    }
                }
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if (insertHere) {
            SQLiteDatabase db = dbHelper.getWritableDatabase();
            long id = db.insert(SimpleDHTConstants.TABLE_NAME, null, values);
            if (id <= 0) {
                throw new SQLException("could not insert Content provider");
            }
            Log.v("cp", "ContentProvider Inserted: values.toString()" + values.toString());
            getContext().getContentResolver().notifyChange(uri, null);
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        firstNodeCreated = false;
        providerHelper = new ProviderHelper();

        Context context = getContext();
        dbHelper = new SimpleDHT_DBHelper(context);

        new SetupTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
        isServerSetUp = false;
        helperClassToInsert = new HelperClassToInsert();
        helperForQuery = new HelperForQuery();
        helperForDeleteAll = new HelperForDeleteAll();
        Log.v("cp", "SimpleDhtProvider onCreate myNode_ID: " + myNode_ID);
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        Log.v("cp6", "TESTING myPort_Socket: " + myPort_Socket);
        SQLiteDatabase db = dbHelper.getReadableDatabase();
        Cursor cursor;
        boolean queryOnSelf = true;
        if(null != preDecHash) {
            Log.v("cp6", "selection: " + selection);
            if (null != projection) {
                Log.v("cp6", "projection: " + projection.toString());
            }
            if (null != selectionArgs) {
                Log.v("cp6", "selectionArgs: " + selectionArgs.toString());
            }
            String dataLocationPort = null;
            QueryReplyContainer queryReplyContainer = null;
            queryOnSelf = false;
            Log.v("cp6", Boolean.toString(!selection.equals("*")));
            Log.v("cp6", Boolean.toString(!selection.equals("@")));
            if (!selection.equals("*") && !selection.equals("@")) {
                Log.v("cp6", "Neither * nor @");
                try {
                    String hash = genHash(selection);
                    Log.v("cp6", "ring_Parent_Port: " + ring_Parent_Port + " myPort_Socket: " + myPort_Socket);
                    if (FIRST_NODE.equals(myNode_ID) && myPort_Socket.equals(ring_Parent_Port)) {
                        dataLocationPort = helperForQuery.findCorrectLocation(dht_metaData_Parent, hash);
                        Log.v("cp6", "dataLocationPort: " + dataLocationPort);
                    } else if (myPort_Socket.equals(ring_Parent_Port)) {
                        queryReplyContainer = helperForQuery.findLocation_ParentWasUpdated(
                                hash, myHash, sucHash, preDecHash, sucPort, selection);
                        queryOnSelf = queryReplyContainer.queryOnSelf;
                    } else {
                        Log.v("cp6", "else findCorrectLocation_Others");
                        queryReplyContainer = helperForQuery.findCorrectLocation_Others(hash, myHash, preDecHash, sucHash, sucPort, selection);
                        queryOnSelf = queryReplyContainer.queryOnSelf;
                    }

                    if(!queryOnSelf) {
                        QueryReplyContainer queryReplyContainerResponse = helperForQuery.finalizeQueryFind(dataLocationPort, queryReplyContainer, selection, ring_Parent_Port);
                        Log.v("cp6", "null == queryReplyContainerResponse " + (null == queryReplyContainerResponse));
                        String keyReply = queryReplyContainerResponse.keyReply;
                        String valueReply = queryReplyContainerResponse.valueReply;
                        Log.v("cp6","keyReply: " + keyReply + " valueReply:" + valueReply);
                        if (queryReplyContainerResponse.queryOnSelf) {
                            queryOnSelf = true;
                        } else {
                            cursor = helperForQuery.getCursor(queryReplyContainerResponse.keyReply, queryReplyContainerResponse.valueReply);
                            return cursor;
                        }
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            } else if("@".equals(selection)) {
                Log.v("cp2","lDump Test");
                cursor = db.query(SimpleDHTConstants.TABLE_NAME, projection,
                        null, null, null, null, sortOrder);
                cursor.setNotificationUri(getContext().getContentResolver(), uri);
                Log.v("query", selection);
                return cursor;
            } else if("*".equals(selection)){
                Log.v("cp2","gDump Test");
                cursor = helperForQuery.getAllData(myPort_Socket, sucPort, getContext().getContentResolver());
                return  cursor;
            }
        }

        if (selection.equals("*") || selection.equals("@")) {

            cursor = db.query(SimpleDHTConstants.TABLE_NAME, projection,
                    null, null, null, null, sortOrder);
            cursor.setNotificationUri(getContext().getContentResolver(), uri);
            Log.v("query", selection);
           return cursor;
        } else if (queryOnSelf) {
            Log.v("cp6", "finally here queryOnSelf: " + queryOnSelf);
            cursor = db.query(SimpleDHTConstants.TABLE_NAME, projection,
                    SimpleDHTConstants.COLUMN_KEY + " LIKE ? ", new String[]{selection}, null, null, sortOrder);
            cursor.setNotificationUri(getContext().getContentResolver(), uri);
            Log.v("cp6", "null == cursor" + (null == cursor));
            Log.v("query", selection);
            return cursor;
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public static void setMyPortNumber(String myNode_ID_From_Main) {
        myNode_ID = myNode_ID_From_Main;
        Log.v("cp", "SimpleDhtProvider setMyPortNumber myNode_ID: " + myNode_ID);
    }

    private class SetupTask extends AsyncTask<Void, Void, Void> {

        @Override
        protected Void doInBackground(Void... voids) {
            while (null == myNode_ID) {
                continue;
            }
            Log.v("cp", "TOCHECK myNode_ID: " + myNode_ID);
            Log.v("cp", "TOCHECK FIRST_NODE: " + FIRST_NODE);
            try {
                myHash = genHash(myNode_ID);
                Log.v("cp", "SetupTask doInBackground myNode_ID: " + myNode_ID + " key: " + myHash);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            myPort_Socket = String.valueOf((Integer.parseInt(myNode_ID) * 2));
            ring_Parent_Port = "11108";

            if (FIRST_NODE.equals(myNode_ID) && !firstNodeCreated) {
                try {
                    Log.v("cp", "SimpleDhtProvider NotFirstNodeCreated myNode_ID: " + myNode_ID);
                    dht_metaData_Parent = providerHelper.insertFirstNode(genHash(myNode_ID), myPort_Socket);
                    firstNodeCreated = true;

                    ServerSocket serverSocket = null;
                    try {
                        serverSocket = new ServerSocket(SERVER_PORT);
                        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            } else if (!isServerSetUp) {
                ServerSocket serverSocket = null;
                try {
                    serverSocket = new ServerSocket(SERVER_PORT);
                    new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
                    isServerSetUp = true;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (!myNode_ID.equals(FIRST_NODE)) {
                FIRST_NODE_PORT = String.valueOf((Integer.parseInt(FIRST_NODE) * 2));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myNode_ID);
            }

            Log.v("cp", "SimpleDhtProvider setup_Ring myNode_ID: " + myNode_ID);
            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, Void, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    Log.v("cp", "ServerTask doInBackground Accepted Connection ");
                    ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                    int message_type = objectInputStream.readInt();
                    if (message_type == SimpleDHTConstants.MESSAGE_JOIN_RING) {
                        String node_ID = objectInputStream.readUTF();
                        Log.v("cp", "ServerTask doInBackground Received Join Request node_ID:" + node_ID);
                        String remote_Port = String.valueOf((Integer.parseInt(node_ID) * 2));
                        DHT_MetaData node = providerHelper.insertNewNode(dht_metaData_Parent, genHash(node_ID), remote_Port);
                        Log.v("cp5", "node_ID: " + node_ID + " node.parent_changed: " + node.parent_changed);
                        if (node.parent_changed) {
                            dht_metaData_Parent = node;
                            if (!ring_Parent_Port.equals(FIRST_NODE_PORT)) {
                                providerHelper.notifyParentChanged(dht_metaData_Parent.portNumber, ring_Parent_Port);
                            }
                            ring_Parent_Port = node.portNumber;
                            Log.v("cp3", "ring_Parent_Port: " + ring_Parent_Port);
                        }
                    } else if (message_type == SimpleDHTConstants.MESSAGE_UPDATE_TABLE) {
                        preDecPort = objectInputStream.readUTF();
                        sucPort = objectInputStream.readUTF();
                        preDecHash = String.valueOf((Integer.parseInt(preDecPort) / 2));
                        sucHash = String.valueOf((Integer.parseInt(sucPort) / 2));
                        preDecHash = genHash(preDecHash);
                        sucHash = genHash(sucHash);
                        Log.v("cp5", "MESSAGE_UPDATE_TABLE myHash: " + myHash);
                        Log.v("cp5", "MESSAGE_UPDATE_TABLE preDecHash: " + preDecHash);
                        Log.v("cp5", "MESSAGE_UPDATE_TABLE sucHash: " + sucHash);
                        if (myHash.compareTo(preDecHash) < 0) {
                            Log.v("cp5", "MESSAGE_UPDATE_TABLE updating parent to: " + myPort_Socket);
                            ring_Parent_Port = myPort_Socket;
                        }
                        if (myHash.compareTo(sucHash) > 0) {
                            Log.v("cp5", "MESSAGE_UPDATE_TABLE updating parent to: " + sucPort);
                            ring_Parent_Port = sucPort;
                        }
                        Log.v("cp5", "MESSAGE_UPDATE_TABLE preDecessor: " + preDecPort);
                        Log.v("cp5", "MESSAGE_UPDATE_TABLE successor: " + sucPort);
                        Log.v("cp5", "MESSAGE_UPDATE_TABLE preDecessor Hash: " + preDecHash);
                        Log.v("cp5", "MESSAGE_UPDATE_TABLE successor: Hash: " + sucHash);
                    } else if (message_type == SimpleDHTConstants.MESSAGE_FIND_TO_INSERT) {
                        String keyToInsert = objectInputStream.readUTF();
                        String valueToInsert = objectInputStream.readUTF();
                        ContentValues contentValues = new ContentValues();
                        contentValues.put(SimpleDHTConstants.COLUMN_KEY, keyToInsert);
                        contentValues.put(SimpleDHTConstants.COLUMN_VALUE, valueToInsert);
                        String hash = genHash(keyToInsert);
                        if (myNode_ID.equals(FIRST_NODE)) {
                            Log.v("cp5", "Request to find came to 11108");
                            helperClassToInsert.functionToInsert(contentValues, getContext().getContentResolver());
                        } else if (myPort_Socket.equals(ring_Parent_Port)) {
                            Log.v("cp5", "Request to find came to current parent: " + ring_Parent_Port);
                            String portToInsertOn = providerHelper.findLocation_ParentWasUpdated(hash, myHash, sucHash, preDecHash, sucPort, contentValues);
                            Log.v("cp5", "Message Key: " + keyToInsert + " Correct Port: " + portToInsertOn + " Hash: " + hash);
                            if (SimpleDHTConstants.INSERT_ON_SELF.equals(portToInsertOn)) {
                                helperClassToInsert.functionToInsert(contentValues, getContext().getContentResolver());
                            } else  if (!portToInsertOn.equals("I don't care or know")){
                                providerHelper.sendFinalInsertRequest(portToInsertOn, contentValues);
                            }
                        } else {
                            Log.v("cp3", "SimpleDhtProvider MESSAGE_FIND_TO_INSERT: Finding correct location I am not the parent");
                            Log.v("cp3", "SimpleDhtProvider MESSAGE_FIND_TO_INSERT: myHash: " + myHash + " key hash: " + hash);
                            String correctLocation = providerHelper.findCorrectLocation_Others(hash, myHash, preDecHash, sucHash, sucPort, contentValues);
                            Log.v("cp3", "SimpleDhtProvider MESSAGE_FIND_TO_INSERT: correctLocation: " + correctLocation);
                            if (sucPort.equals(correctLocation)) {
                                providerHelper.sendFinalInsertRequest(correctLocation, contentValues);
                            } else if (SimpleDHTConstants.INSERT_ON_SELF.equals(correctLocation)) {
                                helperClassToInsert.functionToInsert(contentValues, getContext().getContentResolver());
                            } else if (SimpleDHTConstants.INSERT_ON_PARENT.equals(correctLocation)) {
                                providerHelper.sendFinalInsertRequest(ring_Parent_Port, contentValues);
                            }
                        }
                    } else if (message_type == SimpleDHTConstants.MESSAGE_NOTIFY_PARENT_CHANGED) {
                        String currentParent = objectInputStream.readUTF();
                        ring_Parent_Port = currentParent;
                    } else if (message_type == SimpleDHTConstants.MESSAGE_FINAL_INSERT) {
                        String key = objectInputStream.readUTF();
                        String value = objectInputStream.readUTF();
                        ContentValues contentValues = new ContentValues();
                        contentValues.put(SimpleDHTConstants.COLUMN_KEY, key);
                        contentValues.put(SimpleDHTConstants.COLUMN_VALUE, value);
                        helperClassToInsert.functionToInsert(contentValues, getContext().getContentResolver());
                    } else if (message_type == SimpleDHTConstants.MESSAGE_FIND_KEY) {
                        Log.v("cp6", "Provider MESSAGE_FIND_KEY");
                        String key = objectInputStream.readUTF();
                        String hash;
                        String correctLocation = null;
                        QueryReplyContainer queryReplyContainer = null;
                        try {
                            hash = genHash(key);
                            Log.v("cp6", "key: " + key + " hash: " + hash + " myNode_ID: " + myNode_ID + " FIRST_NODE: " + FIRST_NODE);
                            Log.v("cp6", "myPort_Socket: " + myPort_Socket + " ring_Parent_Port: " + ring_Parent_Port);
                            if (myNode_ID.equals(FIRST_NODE) && myPort_Socket.equals(ring_Parent_Port)) {
                                correctLocation = helperForQuery.findCorrectLocation(dht_metaData_Parent, hash);
                            } else if (myPort_Socket.equals(ring_Parent_Port)) {
                                queryReplyContainer = helperForQuery.findLocation_ParentWasUpdated(hash, myHash, sucHash, preDecHash, sucPort, key);
                            } else {
                                Log.v("cp6", "provider others");
                                queryReplyContainer = helperForQuery.findCorrectLocation_Others(hash, myHash, preDecHash, sucHash, sucPort, key);
                            }
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                        QueryReplyContainer replyContainer = helperForQuery.fulfilQueryReply(correctLocation, queryReplyContainer, key, getContext().getContentResolver(), ring_Parent_Port);
                        Log.v("cp6", "null == replyContainer " + (null == replyContainer));
                        String keyReply = replyContainer.keyReply;
                        String valueReply = replyContainer.valueReply;
                        Log.v("cp6","keyReply: " + keyReply + " valueReply:" + valueReply);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeInt(SimpleDHTConstants.MESSAGE_FIND_KEY_REPLY);
                        objectOutputStream.writeUTF(replyContainer.keyReply);
                        objectOutputStream.writeUTF(replyContainer.valueReply);
                        objectOutputStream.flush();
                    } else if(SimpleDHTConstants.MESSAGE_GET_ALL_DATA == message_type) {
                        String sourcePort = objectInputStream.readUTF();
                        Log.v("cp2", "MESSAGE_GET_ALL_DATA sucPort: " + sucPort + " sourcePort: " + sourcePort);
                        if(sucPort.equals(sourcePort)) {
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            Log.v("cp2", "sucPort.equals(sourcePort)");
                            EntireRingData myData = helperForQuery.justMyData(getContext().getContentResolver());
                            String[] keys = myData.keys;
                            String[] values = myData.values;
                            int count = keys.length;
                            Log.v("cp2", "count: " + count);
                            objectOutputStream.writeInt(SimpleDHTConstants.MESSAGE_GET_ALL_DATA_REPLY);
                            objectOutputStream.writeInt(count);
                            for(int i=0; i<count; i++){
                                objectOutputStream.writeUTF(keys[i]);
                                objectOutputStream.writeUTF(values[i]);
                            }
                            objectOutputStream.flush();
                            Log.v("cp2","sucPort.equals(sourcePort) ENDS");
                        } else {
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            Log.v("cp2"," Not the final node");
                            EntireRingData myData = helperForQuery.justMyData(getContext().getContentResolver());
                            EntireRingData restData = helperForQuery.getAllDataOnBehalf(sourcePort,sucPort);
                            EntireRingData combinedData = helperForQuery.clubEntireRingData(myData, restData);
                            String[] keys = combinedData.keys;
                            String[] values = combinedData.values;
                            int count = keys.length;
                            Log.v("cp2"," Not the final node count: " + count);
                            objectOutputStream.writeInt(SimpleDHTConstants.MESSAGE_GET_ALL_DATA_REPLY);
                            objectOutputStream.writeInt(count);
                            for(int i=0; i<count; i++){
                                objectOutputStream.writeUTF(keys[i]);
                                objectOutputStream.writeUTF(values[i]);
                            }
                            objectOutputStream.flush();
                        }
                    } else if (SimpleDHTConstants.MESSAGE_DELETE_LOC_FINAL == message_type) {
                        String keyToDelete = objectInputStream.readUTF();
                        Log.v("cp1", "MESSAGE_DELETE_LOC_FINAL keyToDelete: " + keyToDelete);
                        helperForDeleteAll.callDelete(keyToDelete, getContext().getContentResolver());
                    } else if(SimpleDHTConstants.MESSAGE_DELETE == message_type) {
                        boolean deleteFromSelf = false;
                        String keyToDelete = objectInputStream.readUTF();
                        Log.v("cp1", "MESSAGE_DELETE keyToDelete: " + keyToDelete);
                        String hash = genHash(keyToDelete);
                        Log.v("cp1", "MESSAGE_DELETE delete else keyToDelete: " + keyToDelete + " hash: " + hash);
                        if(myPort_Socket.equals(FIRST_NODE_PORT) && myPort_Socket.equals(ring_Parent_Port)){
                            String correctLocation = helperForDeleteAll.findCorrectLocation(dht_metaData_Parent, hash);
                            Log.v("cp1", "MESSAGE_DELETE FIRST_NODE_PORT correctLocation: " + correctLocation);
                            if(SimpleDHTConstants.DELETE_FROM_SELF.equals(correctLocation)) {
                                deleteFromSelf = true;
                            } else  {
                                helperForDeleteAll.deleteFromOthersLocFinal(correctLocation, keyToDelete);
                            }
                        } else if(myPort_Socket.equals(ring_Parent_Port)) {
                            String correctLocation = helperForDeleteAll.findLocation_ParentWasUpdated(hash, myHash, sucHash, preDecHash, sucPort, keyToDelete);
                            Log.v("cp1", "MESSAGE_DELETE ring_Parent_Port correctLocation: " + correctLocation);
                            if(SimpleDHTConstants.DELETE_FROM_SELF.equals(correctLocation)){
                                deleteFromSelf = true;
                            } else if(SimpleDHTConstants.DELETION_PERFORMED_ON_OTHER.equals(correctLocation)) {
                                Log.v("cp1", "MESSAGE_DELETE DELETION_PERFORMED_ON_OTHER");
                            } else {
                                helperForDeleteAll.deleteFromOthersLocFinal(correctLocation, keyToDelete);
                            }
                        } else {
                            String correctLocation = helperForDeleteAll.findCorrectLocation_Others(hash, myHash, preDecHash, sucHash, sucPort, keyToDelete);
                            Log.v("cp1", "MESSAGE_DELETE else correctLocation: " + correctLocation);
                            if(SimpleDHTConstants.DELETE_FROM_SELF.equals(correctLocation)) {
                                deleteFromSelf = true;
                            } else if(SimpleDHTConstants.DELETE_FROM_PARENT.equals(correctLocation)) {
                                helperForDeleteAll.deleteFromOthersLocFinal(ring_Parent_Port, keyToDelete);
                            } else if(SimpleDHTConstants.DELETION_PERFORMED_ON_OTHER.equals(correctLocation)){
                                Log.v("cp1", "MESSAGE_DELETE else DELETION_PERFORMED_ON_OTHER: ");
                            } else {
                                helperForDeleteAll.deleteFromOthersLocFinal(correctLocation, keyToDelete);
                            }
                        }
                        if(deleteFromSelf) {
                            helperForDeleteAll.callDelete(keyToDelete, getContext().getContentResolver());
                        }
                    } else if(message_type == SimpleDHTConstants.MESSAGE_DELETE_EVERYTHING) {
                        String origin = objectInputStream.readUTF();
                        Log.v("cp1", "MESSAGE_DELETE_EVERYTHING origin: " + origin + " myPort_Socket: " + myPort_Socket);
                        if(myPort_Socket.equals(origin)) {
                            Log.v("cp1", "MESSAGE_DELETE_EVERYTHING END FINAL FINISH");
                        } else {
                            helperForDeleteAll.callDelete("@", getContext().getContentResolver());
                            helperForDeleteAll.requestDeleteAllOnBehalf(origin, sucPort);
                        }
                    }
                } catch (Exception e) {
                    Log.v("cp6", "Exception MESSAGE_FIND_KEY_REPLY " + e.toString());
                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {
            String myNode_ID = strings[0];
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(FIRST_NODE_PORT));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeInt(SimpleDHTConstants.MESSAGE_JOIN_RING);
                objectOutputStream.writeUTF(myNode_ID);
                objectOutputStream.flush();
                Log.v("cp", "sent myNode_ID: " + myNode_ID + " to: " + FIRST_NODE_PORT);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

}
