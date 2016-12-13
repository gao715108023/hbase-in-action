package com.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.io.IOException;

/**
 * @author gaochuanjun
 * @since 2016/12/13
 */
public abstract class CommonDAOTest {

    protected Connection connection;

    @BeforeTest
    public void initial() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        //hbase使用zookeeper的地址
        conf.set("hbase.zookeeper.quorum", "zk1:2181,zk2:2181,zk3:2181");
        //ZooKeeper中的Hbase的根ZNode。
        // 所有的Hbase的ZooKeeper会用这个目录配置相对路径。
        // 默认情况下，所有的Hbase的ZooKeeper文件路径是用相对路径，所以他们会都去这个目录下面。
        // 默认: /hbase
        conf.set("zookeeper.znode.parent", "/hphoenix");
        connection = ConnectionFactory.createConnection(conf);
        initialDao();
    }

    public abstract void initialDao() throws IOException;

    @AfterTest
    public void close() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }
}
