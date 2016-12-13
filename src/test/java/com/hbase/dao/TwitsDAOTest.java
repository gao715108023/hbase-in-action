package com.hbase.dao;

import com.hbase.model.Twit;
import com.hbase.test.CommonDAOTest;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author gaochuanjun
 * @since 2016/12/13
 */
public class TwitsDAOTest extends CommonDAOTest {

    private TwitsDAO twitsDao;

    private UsersDAO usersDao;

    @Override
    public void initialDao() throws IOException {
        twitsDao = new TwitsDAO(connection);
        usersDao = new UsersDAO(connection);
    }

    @Test
    public void testCreateTwit() throws IOException {
        twitsDao.createTwit();
    }

    @Test
    public void testPostTwit() throws IOException {
        DateTime now = new DateTime();
        System.out.println(String.format("Posting twit at %s", now));
        twitsDao.postTwit("TheRealMT", now, "Hello, TwitBase!");
        Twit t = twitsDao.getTwit("TheRealMT", now);
        usersDao.incTweetCount("TheRealMT");
        System.out.println("Successfully posted " + t);
    }

    @Test
    public void testList() throws IOException {
        List<Twit> twits = twitsDao.list("TheRealMT");
        System.out.println(String.format("Found %s twits.", twits.size()));
        for (Twit t : twits) {
            System.out.println(t);
        }
    }

    @Test
    public void testFilterScan() throws IOException {
        List<Twit> twits = twitsDao.filterScan(".*TwitBase.*");
        System.out.println(String.format("Found %s twits.", twits.size()));
        for (Twit t : twits) {
            System.out.println(t);
        }
    }
}
