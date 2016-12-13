package com.hbase.tool;

import com.hbase.dao.TwitsDAO;
import com.hbase.dao.UsersDAO;
import com.hbase.model.Twit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;

/**
 * @author gaochuanjun
 * @since 2016/12/13
 */
public class TwitsTool {
    public static final String usage =
            "twits tool action ...\n" +
                    "  help - print this message and exit.\n" +
                    "  post user text - post a new twit on user's behalf.\n" +
                    "  list user - list all twits for the specified user.\n";
    private static final Logger log = Logger.getLogger(TwitsTool.class);

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || "help".equals(args[0])) {
            System.out.println(usage);
            System.exit(0);
        }
        Configuration conf = HBaseConfiguration.create();
        //hbase使用zookeeper的地址
        conf.set("hbase.zookeeper.quorum", "zk1:2181,zk2:2181,zk3:2181");
        //ZooKeeper中的Hbase的根ZNode。
        // 所有的Hbase的ZooKeeper会用这个目录配置相对路径。
        // 默认情况下，所有的Hbase的ZooKeeper文件路径是用相对路径，所以他们会都去这个目录下面。
        // 默认: /hbase
        conf.set("zookeeper.znode.parent", "/hphoenix");
        Connection connection = ConnectionFactory.createConnection(conf);
        TwitsDAO twitsDao = new TwitsDAO(connection);
        UsersDAO usersDao = new UsersDAO(connection);

        if ("create".equals(args[0])) {
            twitsDao.createTwit();
            System.out.println("Successfully create table, table_name = twits, family = twits");
        }

        if ("post".equals(args[0])) {
            DateTime now = new DateTime();
            log.debug(String.format("Posting twit at %s", now));
            twitsDao.postTwit(args[1], now, args[2]);
            Twit t = twitsDao.getTwit(args[1], now);
            usersDao.incTweetCount(args[1]);
            System.out.println("Successfully posted " + t);
        }

        if ("list".equals(args[0])) {
            List<Twit> twits = twitsDao.list(args[1]);
            log.info(String.format("Found %s twits.", twits.size()));
            for (Twit t : twits) {
                System.out.println(t);
            }
        }
        connection.close();
    }
}
