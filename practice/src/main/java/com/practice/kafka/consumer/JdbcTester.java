package com.practice.kafka.consumer;

import java.sql.*;

public class JdbcTester {
    public static void main(String[] args) {

        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;

        String url = "jdbc:mariadb://localhost:3306/kafka";
        String user = "root";
        String password = "1234";

        try {
            conn = DriverManager.getConnection(url, user , password);
            st = conn.createStatement();
            rs = st.executeQuery("select 'mariadb is connected'");
            if(rs.next()) {
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                rs.close();
                st.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}
