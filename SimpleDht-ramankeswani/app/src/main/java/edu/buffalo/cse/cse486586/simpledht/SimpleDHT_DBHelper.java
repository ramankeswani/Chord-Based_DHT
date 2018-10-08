package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by Keswani on 2/25/2018.
 */

/*
Referred Android Developer Guide For SQLiteOpenHelper
https://developer.android.com/
https://developer.android.com/reference/android/arch/persistence/db/SupportSQLiteOpenHelper.html
 */


public class SimpleDHT_DBHelper extends SQLiteOpenHelper{

    SimpleDHT_DBHelper(Context context){
        super(context, SimpleDHTConstants.DB_NAME, null,SimpleDHTConstants.DB_VERSION );
    }

    @Override
    public void onCreate(SQLiteDatabase db){
        String CREATE_TABLE = "CREATE TABLE " + SimpleDHTConstants.TABLE_NAME + " (" +
                SimpleDHTConstants.COLUMN_KEY + " TEXT PRIMARY KEY ON CONFLICT REPLACE, " +
                SimpleDHTConstants.COLUMN_VALUE + " TEXT);";
        db.execSQL(CREATE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int vOld, int vNew){
        db.execSQL("DROP TABLE IF EXISTS " + SimpleDHTConstants.TABLE_NAME);
        onCreate(db);
    }
}
