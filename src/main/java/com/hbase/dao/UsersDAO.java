package com.hbase.dao;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author gaochuanjun
 * @since 2016/12/12
 */
public class UsersDAO {

    public static final byte[] TABLE_NAME = Bytes.toBytes("users");
    public static final byte[] INFO_FAM = Bytes.toBytes("info");

    public static final byte[] USER_COL = Bytes.toBytes("user");
    public static final byte[] NAME_COL = Bytes.toBytes("name");
    public static final byte[] EMAIL_COL = Bytes.toBytes("email");
    public static final byte[] PASS_COL = Bytes.toBytes("password");
    public static final byte[] TWEETS_COL = Bytes.toBytes("tweet_count");

    public static final byte[] HAMLET_COL = Bytes.toBytes("hamlet_tag");

    private static final Logger LOGGER = Logger.getLogger(UsersDAO.class);

    private Connection connection;

    public UsersDAO(Connection connection) {
        this.connection = connection;
    }

    private static Get buildGet(String user) throws IOException {
        LOGGER.debug(String.format("Creating Get for %s", user));
        Get g = new Get(Bytes.toBytes(user));
        g.addFamily(INFO_FAM);
        return g;
    }

    private static Put buildPut(User u) {
        LOGGER.debug(String.format("Creating Put for %s", u));
        Put p = new Put(Bytes.toBytes(u.user));
        p.addColumn(INFO_FAM, USER_COL, Bytes.toBytes(u.user));
        p.addColumn(INFO_FAM, NAME_COL, Bytes.toBytes(u.name));
        p.addColumn(INFO_FAM, EMAIL_COL, Bytes.toBytes(u.email));
        p.addColumn(INFO_FAM, PASS_COL, Bytes.toBytes(u.password));
        return p;
    }

    public static Put buildPut(String username,
                               byte[] fam,
                               byte[] qual,
                               byte[] val) {
        Put p = new Put(Bytes.toBytes(username));
        p.addColumn(fam, qual, val);
        return p;
    }

    private static Delete buildDelete(String user) {
        LOGGER.debug(String.format("Creating Delete for %s", user));
        return new Delete(Bytes.toBytes(user));
    }

    private static Scan mkScan() {
        Scan s = new Scan();
        s.addFamily(INFO_FAM);
        return s;
    }

    public void addUser(String user,
                        String name,
                        String email,
                        String password)
            throws IOException {
        Table users = connection.getTable(TableName.valueOf(TABLE_NAME));
        Put p = buildPut(new User(user, name, email, password));
        users.put(p);
        users.close();
    }

    public com.hbase.model.User getUser(String user) throws IOException {
        Table users = connection.getTable(TableName.valueOf(TABLE_NAME));

        Get g = buildGet(user);
        Result result = users.get(g);
        if (result.isEmpty()) {
            LOGGER.info(String.format("user %s not found.", user));
            return null;
        }

        User u = new User(result);
        users.close();
        return u;
    }

    public void deleteUser(String user) throws IOException {
        Table users = connection.getTable(TableName.valueOf(TABLE_NAME));
        Delete d = buildDelete(user);
        users.delete(d);
        users.close();
    }

    public List<com.hbase.model.User> getUsers() throws IOException {
        Table users = connection.getTable(TableName.valueOf(TABLE_NAME));
        ResultScanner results = users.getScanner(mkScan());
        ArrayList<com.hbase.model.User> ret = new ArrayList<com.hbase.model.User>();
        for (Result r : results) {
            ret.add(new User(r));
        }
        users.close();
        return ret;
    }

    public long incTweetCount(String user) throws IOException {
        Table users = connection.getTable(TableName.valueOf(TABLE_NAME));
        long ret = users.incrementColumnValue(Bytes.toBytes(user),
                INFO_FAM,
                TWEETS_COL,
                1L);
        users.close();
        return ret;
    }

    private static class User extends com.hbase.model.User {
        private User(Result r) {
            this(r.getValue(INFO_FAM, USER_COL),
                    r.getValue(INFO_FAM, NAME_COL),
                    r.getValue(INFO_FAM, EMAIL_COL),
                    r.getValue(INFO_FAM, PASS_COL),
                    r.getValue(INFO_FAM, TWEETS_COL) == null ? Bytes.toBytes(0L) : r.getValue(INFO_FAM, TWEETS_COL));
        }

        private User(byte[] user,
                     byte[] name,
                     byte[] email,
                     byte[] password,
                     byte[] tweetCount) {
            this(Bytes.toString(user),
                    Bytes.toString(name),
                    Bytes.toString(email),
                    Bytes.toString(password));
            this.tweetCount = Bytes.toLong(tweetCount);
        }

        private User(String user,
                     String name,
                     String email,
                     String password) {
            this.user = user;
            this.name = name;
            this.email = email;
            this.password = password;
        }
    }
}
