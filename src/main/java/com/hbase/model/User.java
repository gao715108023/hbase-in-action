package com.hbase.model;

/**
 * @author gaochuanjun
 * @since 2016/12/12
 */
public abstract class User {

    public String user;
    public String name;
    public String email;
    public String password;
    public long tweetCount;

    @Override
    public String toString() {
        return String.format("<User: %s, %s, %s, %s, %s>", user, name, email, password, tweetCount);
    }
}
