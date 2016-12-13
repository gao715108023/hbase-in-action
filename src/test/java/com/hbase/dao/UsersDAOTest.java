package com.hbase.dao;

import com.hbase.model.User;
import com.hbase.test.CommonDAOTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author gaochuanjun
 * @since 2016/12/13
 */
public class UsersDAOTest extends CommonDAOTest {

    private UsersDAO dao;

    @Override
    public void initialDao() {
        dao = new UsersDAO(connection);
    }

    @Test
    public void testGetUsers() throws IOException {
        List<User> users = dao.getUsers();
        System.out.println(String.format("Found %s users.", users.size()));
        for (User u : users) {
            System.out.println(u);
        }
    }

    @Test
    public void testDeleteUser() throws IOException {
        dao.deleteUser("TheReatMT");
    }


}
