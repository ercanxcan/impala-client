package com.ercancan.cdh;

import java.sql.*;

public class ImpalaExample {

    private static final String SQL_STATEMENT = "select * from table_1";
    private static final String IMPALAD_HOST = "host_ip";
    private static final String IMPALAD_DB = "test_dbi";
    private static final String IMPALAD_JDBC_PORT = "10000"; //"21050";
    private static final String CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/"+ IMPALAD_DB;
    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_USERNAME = "hadoop";
    private static final String HIVE_PASSWORD = "";

    public static void main(String[] args) {

        System.out.println("\n==============HADOOP IMPALA JDBC EXAMPLE==========================");
        Connection con = null;

        try {
            Class.forName(JDBC_DRIVER_NAME);
            con = DriverManager.getConnection(CONNECTION_URL, HIVE_USERNAME, HIVE_PASSWORD);

            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(SQL_STATEMENT);

            System.out.println("\n============== TEST TABLE RESULTS ==============================");
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                con.close();
            } catch (Exception e) {
            }
        }
    }
}
