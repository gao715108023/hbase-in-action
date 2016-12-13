package com.hbase.dao;

import com.hbase.util.Md5Utils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author gaochuanjun
 * @since 2016/12/13
 */
public class TwitsDAO {

    public static final byte[] TABLE_NAME = Bytes.toBytes("twits");
    public static final byte[] TWITS_FAM = Bytes.toBytes("twits");

    public static final byte[] USER_COL = Bytes.toBytes("user");
    public static final byte[] TWIT_COL = Bytes.toBytes("twit");
    private static final int longLength = 8; // bytes
    private static final Logger log = Logger.getLogger(TwitsDAO.class);
    private Connection connection;
    private Admin admin;

    public TwitsDAO(Connection connection) throws IOException {
        this(connection, connection.getAdmin());
    }

    public TwitsDAO(Connection connection, Admin admin) {
        this.connection = connection;
        this.admin = admin;
    }

    private static byte[] buildRowKey(Twit t) {
        return buildRowKey(t.user, t.dt);
    }

    private static byte[] buildRowKey(String user, DateTime dt) {
        byte[] userHash = Md5Utils.md5sum(user);
        byte[] timestamp = Bytes.toBytes(-1 * dt.getMillis());
        byte[] rowKey = new byte[Md5Utils.MD5_LENGTH + longLength];
        int offset = 0;
        offset = Bytes.putBytes(rowKey, offset, userHash, 0, userHash.length);
        Bytes.putBytes(rowKey, offset, timestamp, 0, timestamp.length);
        return rowKey;
    }

    private static Put buildPut(Twit t) {
        Put p = new Put(buildRowKey(t));
        p.addColumn(TWITS_FAM, USER_COL, Bytes.toBytes(t.user));
        p.addColumn(TWITS_FAM, TWIT_COL, Bytes.toBytes(t.text));
        return p;
    }

    private static Get buildGet(String user, DateTime dt) {
        Get g = new Get(buildRowKey(user, dt));
        g.addColumn(TWITS_FAM, USER_COL);
        g.addColumn(TWITS_FAM, TWIT_COL);
        return g;
    }

    private static String byteToStr(byte[] xs) {
        StringBuilder sb = new StringBuilder(xs.length * 2);
        for (byte b : xs) {
            sb.append(b).append(" ");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    private static Scan buildScan(String user) {
        byte[] userHash = Md5Utils.md5sum(user);
        byte[] startRow = Bytes.padTail(userHash, longLength); // 212d...866f00...
        byte[] stopRow = Bytes.padTail(userHash, longLength);
        stopRow[Md5Utils.MD5_LENGTH - 1]++;                      // 212d...867000...

        log.debug("Scan starting at: '" + byteToStr(startRow) + "'");
        log.debug("Scan stopping at: '" + byteToStr(stopRow) + "'");

        Scan s = new Scan(startRow, stopRow);
        s.addColumn(TWITS_FAM, USER_COL);
        s.addColumn(TWITS_FAM, TWIT_COL);
        return s;
    }

    public void createTwit() throws IOException {
        String tableNameAsString = Bytes.toString(TABLE_NAME);
        TableName tableName = TableName.valueOf(tableNameAsString);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        HColumnDescriptor c = new HColumnDescriptor(TWITS_FAM);
        c.setMaxVersions(1);
        desc.addFamily(c);
        admin.createTable(desc);
    }

    public void postTwit(String user, DateTime dt, String text) throws IOException {
        Table twits = connection.getTable(TableName.valueOf(TABLE_NAME));
        Put p = buildPut(new Twit(user, dt, text));
        twits.put(p);
        twits.close();
    }

    public com.hbase.model.Twit getTwit(String user, DateTime dt) throws IOException {
        Table twits = connection.getTable(TableName.valueOf(TABLE_NAME));
        Get g = buildGet(user, dt);
        Result result = twits.get(g);
        if (result.isEmpty())
            return null;
        Twit t = new Twit(result);
        twits.close();
        return t;
    }

    public List<com.hbase.model.Twit> list(String user) throws IOException {
        Table twits = connection.getTable(TableName.valueOf(TABLE_NAME));
        ResultScanner results = twits.getScanner(buildScan(user));
        List<com.hbase.model.Twit> ret = new ArrayList<com.hbase.model.Twit>();
        for (Result r : results) {
            ret.add(new Twit(r));
        }
        twits.close();
        return ret;
    }

    public List<com.hbase.model.Twit> filterScan(String expr) throws IOException {
        Table twits = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();
        scan.addColumn(TWITS_FAM, TWIT_COL);
        Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(expr));
        scan.setFilter(filter);
        ResultScanner results = twits.getScanner(scan);
        List<com.hbase.model.Twit> ret = new ArrayList<com.hbase.model.Twit>();
        for (Result r : results) {
            ret.add(new Twit(r));
        }
        twits.close();
        return ret;
    }

    private static class Twit extends com.hbase.model.Twit {
        private Twit(Result r) {
            this(r.getValue(TWITS_FAM, USER_COL), Arrays.copyOfRange(r.getRow(), Md5Utils.MD5_LENGTH, Md5Utils.MD5_LENGTH + longLength), r.getValue(TWITS_FAM, TWIT_COL));
        }

        private Twit(byte[] user, byte[] dt, byte[] text) {
            this(Bytes.toString(user), new DateTime(-1 * Bytes.toLong(dt)), Bytes.toString(text));
        }

        private Twit(String user, DateTime dt, String text) {
            this.user = user;
            this.dt = dt;
            this.text = text;
        }
    }
}
