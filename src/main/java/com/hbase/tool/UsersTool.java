package com.hbase.tool;

import com.hbase.dao.UsersDAO;
import com.hbase.model.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @author gaochuanjun
 * @since 2016/12/12
 */
public class UsersTool {

    public static final String usage =
            "user tool action ...\n" +
                    "  help - print this message and exit.\n" +
                    "  add user name email password - add a new user.\n" +
                    "  get user - retrieve a specific user.\n" +
                    "  list - list all installed users.\n";
    private static final Logger log = Logger.getLogger(UsersTool.class);

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
        UsersDAO dao = new UsersDAO(connection);

        if ("get".equals(args[0])) {
            log.debug(String.format("Getting user %s", args[1]));
            User u = dao.getUser(args[1]);
            System.out.println(u);
        }

        if ("add".equals(args[0])) {
            log.debug("Adding user...");
            dao.addUser(args[1], args[2], args[3], args[4]);
            User u = dao.getUser(args[1]);
            System.out.println("Successfully added user " + u);
        }

        if ("list".equals(args[0])) {
            List<User> users = dao.getUsers();
            log.info(String.format("Found %s users.", users.size()));
            for (User u : users) {
                System.out.println(u);
            }
        }
        connection.close();
    }
}
