package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.TextView;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ExecutionException;

/**
 * Created by Keswani on 4/1/2018.
 */

public class HelperForDeleteAll {

    public String findCorrectLocation(DHT_MetaData parent, String keyHashToDelete) {
        Log.v("cp2", "HelperForDeleteAll findCorrectLocation starts");
        String correctLocationPort = null;
        if (null == parent.successor) {
            correctLocationPort = SimpleDHTConstants.DELETE_FROM_SELF;
        } else if (keyHashToDelete.compareTo(parent.myKey) < 0) {
            correctLocationPort = SimpleDHTConstants.DELETE_FROM_SELF;
        } else {
            boolean found = false;
            DHT_MetaData currentNode = parent;
            do {
                if (keyHashToDelete.compareTo(currentNode.myKey) > 0 &&
                        keyHashToDelete.compareTo(currentNode.successor.myKey) < 0) {
                    correctLocationPort = currentNode.successor.portNumber;
                    found = true;
                    break;
                }
                currentNode = currentNode.successor;
            } while (!currentNode.myKey.equals(parent.myKey));
            if (!found) {
                correctLocationPort = SimpleDHTConstants.DELETE_FROM_SELF;
            }
        }
        Log.v("cp6", "HelperForDeleteAll findCorrectLocation ends correctLocationPort: " + correctLocationPort);
        return correctLocationPort;
    }

    public void deleteFromOthersLocFinal(String ownerPort, String keyToDelete) {
        Log.v("cp1", "deleteFromOthersLocFinal starts ownerPort: " + ownerPort + " keyToDelete: " + keyToDelete);
        new DeleteFromOthersLocFinal().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, ownerPort, keyToDelete);
        Log.v("cp1", "deleteFromOthersLocFinal ends");
    }

    private class DeleteFromOthersLocFinal extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {
            Log.v("cp1", "DeleteFromOthersLocFinal starts");
            String ownerPort = strings[0];
            String keyToDelete = strings[1];
            try {
                Log.v("cp1", "DeleteFromOthersLocFinal Sent req ownerPort: " + ownerPort + " keyToDelete: " + keyToDelete);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ownerPort));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeInt(SimpleDHTConstants.MESSAGE_DELETE_LOC_FINAL);
                objectOutputStream.writeUTF(keyToDelete);
                objectOutputStream.flush();
            } catch (IOException e) {
                Log.v("cp1", "DeleteFromOthersLocFinal IOException " + e.toString());
            }
            return null;
        }
    }

    public String findLocation_ParentWasUpdated(String hashKeyToDelete, String myHashKey, String sucHash, String preDecHash, String sucPort, String keyToDelete) {
        Log.v("cp2", "HelperForDeleteAll findLocation_ParentWasUpdated starts");
        String correctLocation = null;
        Log.v("cp2", "hashKeyToDelete: " + hashKeyToDelete + " myHashKey: " + myHashKey + " sucHash: " + sucHash + " preDecHash: " + preDecHash);
        if (hashKeyToDelete.compareTo(myHashKey) < 0) {
            correctLocation = SimpleDHTConstants.DELETE_FROM_SELF;
        } else if (hashKeyToDelete.compareTo(myHashKey) > 0 && hashKeyToDelete.compareTo(sucHash) < 0) {
            correctLocation = sucPort;
        } else if (hashKeyToDelete.compareTo(preDecHash) > 0) {
            correctLocation = SimpleDHTConstants.DELETE_FROM_SELF;
        } else {
            Log.v("cp2", "HelperForDeleteAll findLocation_ParentWasUpdated else");
            Log.v("cp2", "HelperForDeleteAll findLocation_ParentWasUpdated Deleting on others End");
            new DeleteFromOtherTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyToDelete, sucPort);
            Log.v("cp2", "HelperForDeleteAll findLocation_ParentWasUpdated Deleting on others End");
            correctLocation = SimpleDHTConstants.DELETION_PERFORMED_ON_OTHER;
        }
        Log.v("cp2", "HelperForQuery findLocation_ParentWasUpdated correctLocation: " + correctLocation);
        return correctLocation;
    }

    private class DeleteFromOtherTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {
            String keyToDelete = strings[0];
            String sucPort = strings[1];
            try {
                Log.v("cp1", "DeleteFromOtherTask Sent req sucPort: " + sucPort + " keyToDelete: " + keyToDelete);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sucPort));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeInt(SimpleDHTConstants.MESSAGE_DELETE);
                objectOutputStream.writeUTF(keyToDelete);
                objectOutputStream.flush();
            } catch (IOException e) {
                Log.v("cp1", "DeleteFromOtherTask Exception " + e.toString());
            }
            return null;
        }
    }

    public String findCorrectLocation_Others(String keyHashToDelete, String myKeyHash, String preDecHash, String sucHash, String sucPort, String key) {
        Log.v("cp1", "HelperForDeleteAll findCorrectLocation_Others starts");
        Log.v("cp1", "HelperForDeleteAll keyHashToDelete: " + keyHashToDelete + " myKeyHash: " + myKeyHash + " sucHash: " + sucHash);
        String correctLocation = null;
        if (keyHashToDelete.compareTo(myKeyHash) > 0 && keyHashToDelete.compareTo(sucHash) < 0) {
            correctLocation = sucPort;
        } else if (keyHashToDelete.compareTo(myKeyHash) < 0 && keyHashToDelete.compareTo(preDecHash) > 0) {
            correctLocation = SimpleDHTConstants.DELETE_FROM_SELF;
        } else if (keyHashToDelete.compareTo(myKeyHash) > 0 && myKeyHash.compareTo(sucHash) > 0) {
            correctLocation = SimpleDHTConstants.DELETE_FROM_PARENT;
        } else {
            Log.v("cp1", "else HelperForDeleteAll findCorrectLocation_Others key: " + key + " sucPort: " + sucPort);
            new DeleteFromOtherTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key, sucPort);
            correctLocation = SimpleDHTConstants.DELETION_PERFORMED_ON_OTHER;
        }
        Log.v("cp1", "HelperForDeleteAll findCorrectLocation_Others ends correctLocation: " + correctLocation);
        return correctLocation;
    }

    public void deleteAllData(String sourcePort, String sucPort) {
        Log.v("cp1", "deleteAllData starts");
        new DeleteAllDataTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sourcePort, sucPort);
        Log.v("cp1", "deleteAllData ends");
    }

    private class DeleteAllDataTask extends  AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {
            String sourcePort = strings[0];
            String sucPort = strings[1];
            try {
                Log.v("cp1", "DeleteAllDataTask Sent req sucPort: " + sucPort + " sourcePort: " + sourcePort);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sucPort));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeInt(SimpleDHTConstants.MESSAGE_DELETE_EVERYTHING);
                objectOutputStream.writeUTF(sourcePort);
                objectOutputStream.flush();
            } catch (IOException e) {
                Log.v("cp1", "DeleteAllDataTask Exception: " + e.toString());
            }
            return null;
        }
    }

    public void requestDeleteAllOnBehalf(String origin, String sucPort) {
        Log.v("cp1", "requestDeleteAllOnBehalf starts");
        new DeleteAllDataTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, origin, sucPort);
        Log.v("cp1", "requestDeleteAllOnBehalf ends");
    }

    public void callDelete(String selection, ContentResolver contentResolver) {
        Log.v("cp1", "callDelete Start");
        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        int deleteCount = contentResolver.delete(mUri, selection, null);
        Log.v("cp1", " callDelete deleteCount: " + deleteCount);
        Log.v("cp1", "callDelete End");
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
