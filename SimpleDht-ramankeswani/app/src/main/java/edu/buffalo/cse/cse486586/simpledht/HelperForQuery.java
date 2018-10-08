package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by Keswani on 3/30/2018.
 */

public class HelperForQuery {

    Uri mUri;

    HelperForQuery() {
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    }

    public String findCorrectLocation(DHT_MetaData parent, String keyHashToQuery) {
        Log.v("cp6", "findCorrectLocation starts");
        String correctLocationPort = null;
        if (null == parent.successor) {
            correctLocationPort = SimpleDHTConstants.QUERY_ON_SELF;
        } else if (keyHashToQuery.compareTo(parent.myKey) < 0) {
            correctLocationPort = SimpleDHTConstants.QUERY_ON_SELF;
        } else {
            boolean found = false;
            DHT_MetaData currentNode = parent;
            do {
                if (keyHashToQuery.compareTo(currentNode.myKey) > 0 &&
                        keyHashToQuery.compareTo(currentNode.successor.myKey) < 0) {
                    correctLocationPort = currentNode.successor.portNumber;
                    found = true;
                    break;
                }
                currentNode = currentNode.successor;
            } while (!currentNode.myKey.equals(parent.myKey));
            if (!found) {
                correctLocationPort = SimpleDHTConstants.QUERY_ON_SELF;
            }
        }
        Log.v("cp6", "findCorrectLocation ends correctLocationPort: " + correctLocationPort);
        return correctLocationPort;
    }

    public QueryReplyContainer findLocation_ParentWasUpdated(String hashKeyToQuery, String myHashKey, String sucHash, String preDecHash, String sucPort, String keyToQuery) {
        Log.v("cp6", "findLocation_ParentWasUpdated starts");
        QueryReplyContainer queryReplyContainer = new QueryReplyContainer();
        String correctLocation = null;
        Log.v("cp6", "hashKeyToQuery: " + hashKeyToQuery + " myHashKey: " + myHashKey + " sucHash: " + sucHash + " preDecHash: " + preDecHash);
        if (hashKeyToQuery.compareTo(myHashKey) < 0) {
            correctLocation = SimpleDHTConstants.QUERY_ON_SELF;
            queryReplyContainer.port = correctLocation;
            queryReplyContainer.queryOnSelf = true;
        } else if (hashKeyToQuery.compareTo(myHashKey) > 0 && hashKeyToQuery.compareTo(sucHash) < 0) {
            correctLocation = sucPort;
            queryReplyContainer.port = correctLocation;
        } else if (hashKeyToQuery.compareTo(preDecHash) > 0) {
            correctLocation = SimpleDHTConstants.QUERY_ON_SELF;
            queryReplyContainer.queryOnSelf = true;
            queryReplyContainer.port = correctLocation;
        } else {
            Log.v("cp6", "else");
            try {
                Log.v("cp6", "HelperForQuery findLocation_ParentWasUpdated Finding on others");
                queryReplyContainer = new FindLocationTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyToQuery, sucPort).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        Log.v("cp6", "HelperForQuery findLocation_ParentWasUpdated correctLocation: " + correctLocation);

        return queryReplyContainer;
    }

    private class FindLocationTask extends AsyncTask<String, Void, QueryReplyContainer> {

        @Override
        protected QueryReplyContainer doInBackground(String... strings) {
            Log.v("cp6", "FindLocationTask Starts ");
            String key = strings[0];
            String port = strings[1];
            Log.v("cp6", "port: " + port + " key: " + key);
            QueryReplyContainer queryReplyContainer = new QueryReplyContainer();
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeInt(SimpleDHTConstants.MESSAGE_FIND_KEY);
                objectOutputStream.writeUTF(key);
                objectOutputStream.flush();
                ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                int messageType = objectInputStream.readInt();
                if (SimpleDHTConstants.MESSAGE_FIND_KEY_REPLY == messageType) {
                    Log.v("cp6", "MESSAGE_FIND_KEY_REPLY");
                    String replyKey = objectInputStream.readUTF();
                    String replyValue = objectInputStream.readUTF();
                    Log.v("cp6", "replyKey: " + replyKey + " replyValue: " + replyValue);
                    queryReplyContainer.keyReply = replyKey;
                    queryReplyContainer.valueReply = replyValue;
                }
            } catch (Exception e) {
                Log.v("cp6", "EXCEPTION TASK" + e.toString());
            }
            Log.v("cp6", "FindLocationTask Ends");
            return queryReplyContainer;
        }
    }

    public QueryReplyContainer findCorrectLocation_Others(String keyHashToInsert, String myKeyHash, String preDecHash, String sucHash, String sucPort, String key) {
        Log.v("cp6", "HelperForQuery findCorrectLocation_Others starts");
        Log.v("cp6", "HelperForQuery keyHashToInsert: " + keyHashToInsert + " myKeyHash: " + myKeyHash + " sucHash: " + sucHash);
        String correctLocation = null;
        QueryReplyContainer queryReplyContainer = new QueryReplyContainer();
        if (keyHashToInsert.compareTo(myKeyHash) > 0 && keyHashToInsert.compareTo(sucHash) < 0) {
            correctLocation = sucPort;
            queryReplyContainer.port = correctLocation;
        } else if (keyHashToInsert.compareTo(myKeyHash) < 0 && keyHashToInsert.compareTo(preDecHash) > 0) {
            correctLocation = SimpleDHTConstants.QUERY_ON_SELF;
            queryReplyContainer.queryOnSelf = true;
            queryReplyContainer.port = correctLocation;
        } else if (keyHashToInsert.compareTo(myKeyHash) > 0 && myKeyHash.compareTo(sucHash) > 0) {
            correctLocation = SimpleDHTConstants.QUERY_ON_PARENT;
            queryReplyContainer.port = correctLocation;
        } else {
            try {
                Log.v("cp6", "else findCorrectLocation_Others key: " + key + " sucPort: " + sucPort);
                queryReplyContainer = new FindLocationTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key, sucPort).get();
            } catch (Exception e) {
                Log.v("cp6", "EXCEPTION: " + e.toString());
            }
        }
        Log.v("cp6", "HelperForQuery findCorrectLocation_Others ends correctLocation: " + correctLocation);
        return queryReplyContainer;
    }

    public QueryReplyContainer fulfilQueryReply(String correctLocation, QueryReplyContainer queryReplyContainer, String key, ContentResolver contentResolver, String parentPort) {
        Cursor cursor = null;
        Log.v("cp6", "fulfilQueryReply starts");
        Log.v("cp6", "correctLocation: " + correctLocation);
        QueryReplyContainer replyContainer = new QueryReplyContainer();
        if (null != correctLocation) {
            if (SimpleDHTConstants.QUERY_ON_SELF.equals(correctLocation)) {
                cursor = callQueryMethod(key, contentResolver);
                String[] response = cursorToString(cursor);
                Log.v("cp6", "keyReply: " + response[0] + " valueReply: " + response[1]);
                replyContainer.keyReply = response[0];
                replyContainer.valueReply = response[1];
            } else {
                try {
                    QueryReplyContainer container = new FindLocationTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key, correctLocation).get();
                    String keyReply = container.keyReply;
                    String valueReply = container.valueReply;
                    Log.v("cp6", "keyReply: " + keyReply + " valueReply:" + valueReply);
                    replyContainer.keyReply = keyReply;
                    replyContainer.valueReply = valueReply;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        } else {
            Log.v("cp6", "null != queryReplyContainer.port: " + (null != queryReplyContainer.port));
            if (null != queryReplyContainer.port) {
                Log.v("cp6", "queryReplyContainer.port: " + queryReplyContainer.port);
                if (SimpleDHTConstants.QUERY_ON_SELF.equals(queryReplyContainer.port)) {
                    cursor = callQueryMethod(key, contentResolver);
                    String[] response = cursorToString(cursor);
                    Log.v("cp6", "keyReply: " + response[0] + " valueReply: " + response[1]);
                    replyContainer.keyReply = response[0];
                    replyContainer.valueReply = response[1];
                } else if (SimpleDHTConstants.QUERY_ON_PARENT.equals(queryReplyContainer.port)) {
                    try {
                        QueryReplyContainer container = new FindLocationTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key, parentPort).get();
                        String keyReply = container.keyReply;
                        String valueReply = container.valueReply;
                        Log.v("cp6", "keyReply: " + keyReply + " valueReply:" + valueReply);
                        replyContainer.keyReply = keyReply;
                        replyContainer.valueReply = valueReply;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        QueryReplyContainer container = new FindLocationTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key, queryReplyContainer.port).get();
                        String keyReply = container.keyReply;
                        String valueReply = container.valueReply;
                        Log.v("cp6", "keyReply: " + keyReply + " valueReply:" + valueReply);
                        replyContainer.keyReply = keyReply;
                        replyContainer.valueReply = valueReply;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                String keyReply = queryReplyContainer.keyReply;
                String valueReply = queryReplyContainer.valueReply;
                Log.v("cp6", "keyReply: " + keyReply + " valueReply:" + valueReply);
                replyContainer.keyReply = keyReply;
                replyContainer.valueReply = valueReply;
            }
        }
        Log.v("cp6", "fulfilQueryReply ends");
        return replyContainer;
    }

    public Cursor getCursor(String keyReply, String valueReply) {
        try {
            String[] columns = new String[]{"key", "value"};
            MatrixCursor matrixCursor = new MatrixCursor(columns);
            matrixCursor.addRow(new Object[]{keyReply, valueReply});
           /* int keyIndex = cursor.getColumnIndex("key");
            int valueIndex = cursor.getColumnIndex("value");
            Log.v("cp6", "TESTING keyIndex: " + keyIndex + " valueIndex: " + valueIndex);
            cursor.moveToFirst();
            String[] columns = new String[]{"key", "value"};
            MatrixCursor matrixCursor = new MatrixCursor(columns);
            for(int i = 0; i< cursor.getCount(); i++) {
                String key = cursor.getString(keyIndex);
                String value = cursor.getString(valueIndex);
                Log.v("cp6", "TESTING i:" + i + " key: " + key + " value: " + value);
                matrixCursor.addRow(new Object[]{key, value});
                cursor.moveToNext();
            }
            cursor = matrixCursor;*/
            return matrixCursor;
        } catch (Exception e) {
            Log.v("cp6", "TESTING exception " + e.toString());
            return null;
        }
    }

    public QueryReplyContainer finalizeQueryFind(String correctLocation, QueryReplyContainer queryReplyContainer, String key, String parentPort) {
        Log.v("cp6", "finalizeQueryFind starts");
        QueryReplyContainer queryReplyContainerReply = new QueryReplyContainer();
        queryReplyContainerReply.queryOnSelf = false;
        Cursor cursor;
        Log.v("cp6", "correctLocation: " + correctLocation);
        if (null != correctLocation) {
            Log.v("cp6", "correctLocation: " + correctLocation);
            if (SimpleDHTConstants.QUERY_ON_SELF.equals(correctLocation)) {
                queryReplyContainerReply.queryOnSelf = true;
            } else {
                try {
                    QueryReplyContainer container = new FindLocationTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key, correctLocation).get();
                    String keyReply = container.keyReply;
                    String valueReply = container.valueReply;
                    Log.v("cp6", "keyReply: " + keyReply + " valueReply:" + valueReply);
                    queryReplyContainerReply.keyReply = keyReply;
                    queryReplyContainerReply.valueReply = valueReply;

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        } else {
            Log.v("cp6", "null != queryReplyContainer.port: " + (null != queryReplyContainer.port));
            if (null != queryReplyContainer.port) {
                Log.v("cp6", "queryReplyContainer.port: " + queryReplyContainer.port);
                if (SimpleDHTConstants.QUERY_ON_SELF.equals(queryReplyContainer.port)) {
                    queryReplyContainerReply.queryOnSelf = true;
                } else if (SimpleDHTConstants.QUERY_ON_PARENT.equals(queryReplyContainer.port)) {
                    try {
                        QueryReplyContainer container = new FindLocationTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key, parentPort).get();
                        String keyReply = container.keyReply;
                        String valueReply = container.valueReply;
                        Log.v("cp6", "keyReply: " + keyReply + " valueReply:" + valueReply);
                        queryReplyContainerReply.keyReply = keyReply;
                        queryReplyContainerReply.valueReply = valueReply;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        QueryReplyContainer container = new FindLocationTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key, queryReplyContainer.port).get();
                        String keyReply = container.keyReply;
                        String valueReply = container.valueReply;
                        Log.v("cp6", "keyReply: " + keyReply + " valueReply:" + valueReply);
                        queryReplyContainerReply.keyReply = keyReply;
                        queryReplyContainerReply.valueReply = valueReply;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                String keyReply = queryReplyContainer.keyReply;
                String valueReply = queryReplyContainer.valueReply;
                Log.v("cp6", "keyReply: " + keyReply + " valueReply:" + valueReply);
                queryReplyContainerReply.keyReply = keyReply;
                queryReplyContainerReply.valueReply = valueReply;
            }
        }
        Log.v("cp6", "finalizeQueryFind ends");
        return queryReplyContainerReply;
    }

    public Cursor callQueryMethod(String key, ContentResolver mContentResolver) {
        Log.v("cp2", "callQueryMethod");
        Cursor resultCursor = mContentResolver.query(mUri, null, key, null, null);
        if (resultCursor == null) {
            Log.v("cp2", "HelperForQuery Result null");
        }
        return resultCursor;
    }

    public String[] cursorToString(Cursor cursor) {
        String[] response = new String[2];
        int keyIndex = cursor.getColumnIndex("key");
        int valueIndex = cursor.getColumnIndex("value");
        Log.v("cp6", "TESTING keyIndex: " + keyIndex + " valueIndex: " + valueIndex);
        cursor.moveToFirst();
        String key = cursor.getString(keyIndex);
        String value = cursor.getString(valueIndex);
        response[0] = key;
        response[1] = value;
        return response;
    }

    public Cursor getAllData(String sourcePort, String sucPort, ContentResolver mContentResolver) {
        EntireRingData entireRingData = new EntireRingData();
        int counter = 0;
        Cursor ownData = callQueryMethod("@", mContentResolver);
        EntireRingData myData = cursorToRingData(ownData);
        EntireRingData restData;
        try {
            restData = new GetAllDataTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sucPort, sourcePort).get();
        } catch (Exception e) {
            Log.v("cp2", "getAllData Exception: " + e.toString());
            return null;
        }
        Log.v("cp2", "getAllData task ran fine");
        entireRingData = clubEntireRingData(myData, restData);
        Cursor cursor = getCursorAllData(entireRingData);
        return cursor;
    }

    private EntireRingData cursorToRingData(Cursor cursor) {
        EntireRingData data = new EntireRingData();
        int keyIndex = cursor.getColumnIndex("key");
        int valueIndex = cursor.getColumnIndex("value");
        Log.v("cp2", "cursorToRingData TESTING keyIndex: " + keyIndex + " valueIndex: " + valueIndex);
        int count = cursor.getCount();
        String[] keys = new String[count];
        String[] values = new String[count];
        cursor.moveToFirst();
        for (int i = 0; i < count; i++) {
            String key = cursor.getString(keyIndex);
            String value = cursor.getString(valueIndex);
            Log.v("cp2", "cursorToRingData TESTING key: " + key + " value: " + value);
            keys[i] = key;
            values[i] = value;
            cursor.moveToNext();
        }
        data.values = values;
        data.keys = keys;
        return data;
    }

    private class GetAllDataTask extends AsyncTask<String, Void, EntireRingData> {

        @Override
        protected EntireRingData doInBackground(String... strings) {
            String queryPort = strings[0];
            String sourcePort = strings[1];
            EntireRingData entireRingData = new EntireRingData();
            entireRingData.sourcePort = sourcePort;
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(queryPort));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeInt(SimpleDHTConstants.MESSAGE_GET_ALL_DATA);
                objectOutputStream.writeUTF(sourcePort);
                objectOutputStream.flush();
                Log.v("cp2", "GetAllDataTask sent req");
                ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                int messageType = objectInputStream.readInt();
                Log.v("cp2", "GetAllDataTask messageType: " + messageType);
                if (SimpleDHTConstants.MESSAGE_GET_ALL_DATA_REPLY == messageType) {
                    int countKVPair = objectInputStream.readInt();
                    Log.v("cp2", "GetAllDataTask countKVPair: " + countKVPair);
                    String[] keys = new String[countKVPair];
                    String[] values = new String[countKVPair];
                    for (int i = 0; i < countKVPair; i++) {
                        keys[i] = objectInputStream.readUTF();
                        Log.v("cp2", "keys[i]: " + keys[i]);
                        values[i] = objectInputStream.readUTF();
                        Log.v("cp2", "values[i]: " + values[i]);
                    }
                    entireRingData.keys = keys;
                    entireRingData.values = values;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            Log.v("cp2", "GetAllDataTask ends");
            return entireRingData;
        }
    }

    public EntireRingData justMyData(ContentResolver contentResolver) {
        EntireRingData entireRingData = new EntireRingData();
        Cursor ownData = callQueryMethod("@", contentResolver);
        int keyIndex = ownData.getColumnIndex("key");
        int valueIndex = ownData.getColumnIndex("value");
        Log.v("cp2", "justMyData TESTING keyIndex: " + keyIndex + " valueIndex: " + valueIndex);
        int count = ownData.getCount();
        Log.v("cp2", "justMyData count: " + count);
        String[] keys = new String[count];
        String[] values = new String[count];
        ownData.moveToFirst();
        for (int i = 0; i < count; i++) {
            String key = ownData.getString(keyIndex);
            String value = ownData.getString(valueIndex);
            Log.v("cp2", "justMyData TESTING key: " + key + " value: " + value);
            keys[i] = key;
            values[i] = value;
            ownData.moveToNext();
        }
        entireRingData.values = values;
        entireRingData.keys = keys;
        return entireRingData;
    }

    public EntireRingData getAllDataOnBehalf(String sourcePort, String sucPort) {
        EntireRingData entireRingData;
        Log.v("cp2", "getAllDataOnBehalf sourcePort: " + sourcePort + " sucPort: " + sucPort);
        try {
            entireRingData = new GetAllDataTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sucPort, sourcePort).get();
            return entireRingData;
        } catch (Exception e) {
            Log.v("cp2", "Exception getAllDataOnBehalf: " + e.toString());
            entireRingData = null;
        }
        return entireRingData;
    }

    public EntireRingData clubEntireRingData(EntireRingData myData, EntireRingData restData) {
        EntireRingData entireRingData = new EntireRingData();
        try {
            String[] myKeys = myData.keys;
            String[] otherKeys = restData.keys;
            String[] myValues = myData.values;
            String[] otherValues = restData.values;
            Log.v("cp2", "myKeys: " + myKeys.length + " otherKeys: " + otherKeys.length +
                    " myValues: " + myValues.length + " otherValues: " + otherValues.length);
            List myKeyList = new ArrayList(Arrays.asList(myKeys));
            List otherKeysList = new ArrayList(Arrays.asList(otherKeys));
            myKeyList.addAll(otherKeysList);
            String[] combinedKeys = (String[]) myKeyList.toArray(new String[myKeyList.size()]);
            List myValuesList = new ArrayList(Arrays.asList(myValues));
            List otherValuesList = new ArrayList(Arrays.asList(otherValues));
            myValuesList.addAll(otherValuesList);
            String[] combinedValues = (String[]) myValuesList.toArray(new String[myValuesList.size()]);
            entireRingData.keys = combinedKeys;
            entireRingData.values = combinedValues;
            Log.v("cp2", "clubEntireRingData ends combinedKeys: " + combinedKeys.length + " combinedValues: " + combinedValues.length);
        } catch (Exception e) {
            Log.v("cp2", "clubEntireRingData exception " + e.toString());
        }
        return entireRingData;
    }

    private Cursor getCursorAllData(EntireRingData entireRingData) {
        String[] columns = new String[]{"key", "value"};
        MatrixCursor matrixCursor = new MatrixCursor(columns);
        String[] keysReply = entireRingData.keys;
        String[] valuesReply = entireRingData.values;
        int count = keysReply.length;
        for (int i = 0; i < count; i++) {
            try {
                matrixCursor.addRow(new Object[]{keysReply[i], valuesReply[i]});
            } catch (Exception e) {
                Log.v("cp6", "TESTING getCursorAllData exception " + e.toString());
                return null;
            }
        }
        return matrixCursor;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


}
