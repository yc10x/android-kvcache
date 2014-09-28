
package me.caiying.kvcache;

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class KVCache extends SQLiteOpenHelper {
    public static final int CACHE_VER = 1;
    public static final String CACHE_DBNAME = "kvcache";
    public static final String COL_K = "k";
    public static final String COL_V = "v";
    public static final String COL_TSDIFF = "tsdiff";
    private static KVCache sInstance;

    private KVCache(Context context) {
        super(context, CACHE_DBNAME, null, CACHE_VER);
    }

    public static KVCache getInstance(Context context) {
        if (sInstance == null) {
            sInstance = new KVCache(context);
        }
        return sInstance;
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("create table kv_cache (" +
                "k varchar(10000) not null primary key," +
                "v text default ''," +
                "updated_time datetime," +
                "created_time timestamp not null default current_timestamp" +
                ")");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }

    public synchronized boolean set(String k, String v) {
        if (v == null || v.equals("")) {
            Log.v("cache", "empty data set");
            return false;
        }

        SQLiteDatabase db = getWritableDatabase();
        String sql = "replace into kv_cache" +
                " (k, v, updated_time)" +
                " values (?, ?, current_timestamp)";

        Log.v("cache", "cache set for " + k + " done");
        try {
            db.execSQL(sql, new Object[] {
                    k, v
            });
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                db.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    public synchronized void delete(String key) {
        SQLiteDatabase db = getWritableDatabase();
        String sql = "delete from kv_cache where k = '" + key + "'";

        Log.v("cache", "cache delete for " + key + " done");
        try {
            db.execSQL(sql);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                db.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized String get(String k, String d, int expire) {
        SQLiteDatabase db = getReadableDatabase();
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(
                    "select k,v, " +
                            "(strftime('%s','now') - strftime('%s', updated_time)) as tsdiff " +
                            "from kv_cache " +
                            "where k=?", new String[] {
                        k
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (cursor == null)
            return d;

        if (cursor.moveToFirst() == false) {
            try {
                cursor.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                db.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return d;
        } else {
            String v = cursor.getString(cursor.getColumnIndex(COL_V));
            int tsdiff = cursor.getInt(cursor.getColumnIndex(COL_TSDIFF));
            try {
                cursor.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                db.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (expire == -1 || expire > tsdiff) {
                Log.v("cache", "k" + k + " not expired");
                return v;
            } else {
                Log.v("cache", "k" + k + "expired");
                return d;
            }
        }
    }

}
