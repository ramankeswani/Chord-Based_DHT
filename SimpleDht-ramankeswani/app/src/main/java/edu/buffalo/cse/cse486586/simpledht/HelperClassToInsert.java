package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.net.Uri;

/**
 * Created by Keswani on 3/29/2018.
 */

public class HelperClassToInsert {

    public void functionToInsert(ContentValues contentValues, ContentResolver contentResolver) {

        Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        try {
            contentResolver.insert(uri, contentValues);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
