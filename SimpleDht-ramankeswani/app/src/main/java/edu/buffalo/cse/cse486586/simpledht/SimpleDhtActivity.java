package edu.buffalo.cse.cse486586.simpledht;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {

    String myPort;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
        Button lDump = (Button) findViewById(R.id.button1);
        lDump.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                ldumpClicked();
            }
        });

        Button gDump = (Button) findViewById(R.id.button2);
        gDump.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                gDumpClicked();
            }
        });

        final Button lDelete = (Button) findViewById(R.id.button);
        lDelete.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                lDeleteTest();
            }
        });

        passMyPortNumber();
    }

    private void ldumpClicked(){
        TextView textView = (TextView)findViewById(R.id.textView1);
        ContentResolver contentResolver = getContentResolver();
        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        Cursor resultCursor = contentResolver.query(mUri, null, "@", null, null);
        int keyIndex = resultCursor.getColumnIndex("key");
        int valueIndex = resultCursor.getColumnIndex("value");
        Log.v("cp2", "TESTING keyIndex: " + keyIndex + " valueIndex: " + valueIndex);
        resultCursor.moveToFirst();
        for(int i=0; i<resultCursor.getCount(); i++) {
            String key = resultCursor.getString(keyIndex);
            String value = resultCursor.getString(valueIndex);
            textView.append("key: " + key + " value: " + value + "\n");
            Log.v("cp2", "TESTING ldumpClicked key: " + key + " value: " + value);
            resultCursor.moveToNext();
        }
    }

    private void gDumpClicked(){
        TextView textView = (TextView)findViewById(R.id.textView1);
        ContentResolver contentResolver = getContentResolver();
        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        Cursor resultCursor = contentResolver.query(mUri, null, "*", null, null);
        int keyIndex = resultCursor.getColumnIndex("key");
        int valueIndex = resultCursor.getColumnIndex("value");
        Log.v("cp2", "TESTING gDumpClicked keyIndex: " + keyIndex + " valueIndex: " + valueIndex);
        resultCursor.moveToFirst();
        for(int i=0; i<resultCursor.getCount(); i++) {
            String key = resultCursor.getString(keyIndex);
            String value = resultCursor.getString(valueIndex);
            textView.append("key: " + key + " value: " + value + "\n");
            Log.v("cp2", "TESTING key: " + key + " value: " + value);
            resultCursor.moveToNext();
        }
    }

    private void lDeleteTest(){
        Log.v("cp1", "SimpleDhtActivity lDeleteTest Start");
        TextView textView = (TextView)findViewById(R.id.textView1);
        ContentResolver contentResolver = getContentResolver();
        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        String selection = "*";
        int deleteCount = contentResolver.delete(mUri, selection, null);
        Log.v("cp1", " lDeleteTest deleteCount: " + deleteCount);
        Log.v("cp1", "SimpleDhtActivity lDeleteTest End");
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

    public void passMyPortNumber(){
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        Log.v("cp", "SimpleDhtActivity passMyPortNumber myNode_ID: " + portStr);
        SimpleDhtProvider.setMyPortNumber(portStr);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

}
