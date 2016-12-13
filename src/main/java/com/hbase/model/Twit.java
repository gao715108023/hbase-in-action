package com.hbase.model;

import org.joda.time.DateTime;

/**
 * @author gaochuanjun
 * @since 2016/12/13
 */
public abstract class Twit {
    public String user;
    public DateTime dt;
    public String text;

    @Override
    public String toString() {
        return String.format("<Twit: %s %s %s>", user, dt, text);
    }
}
