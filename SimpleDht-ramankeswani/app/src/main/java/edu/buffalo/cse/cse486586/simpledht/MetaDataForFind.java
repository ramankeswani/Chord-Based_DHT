package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;

/**
 * Created by Keswani on 3/29/2018.
 */

public class MetaDataForFind {

    ContentValues contentValues;
    String successorPort;

    MetaDataForFind(ContentValues contentValues, String successorPort) {
        this.contentValues = contentValues;
        this.successorPort = successorPort;
    }
}
