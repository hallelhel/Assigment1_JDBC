package com.company;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class Assignment {
        private String Connection;
        private String DBusername;
        private String DBpassword;


        public Assignment(String _connection,String _DBusername,String _DBpassword) {
            this.Connection = _connection;
            this.DBusername = _DBusername;
            this.DBpassword = _DBpassword;
        }

        public Connection connectionToDB(){
            Connection conn = null;
            try{
                Class.forName("oracle.jdbc.driver.OracleDriver");
                conn = DriverManager.getConnection(this.Connection, this.DBusername, this.DBpassword);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return conn;
        }

        public void  fileToDataBase(String path){
            Connection conn = null;
            PreparedStatement ps = null;
            try{
                conn = connectionToDB();
                String sql = "INSERT INTO MediaItems (TITLE, PROD_YEAR) VALUES (?, ?)";
                ps = conn.prepareStatement(sql);

                BufferedReader lineReader  = new BufferedReader(new FileReader(path));
                String lineText = null;

                while ((lineText = lineReader.readLine()) != null) {
                    String[] data = lineText.split(",");
                    String title = data[0];
                    String prod_year = data[1];

                    ps.setString(1, title);
                    ps.setString(2, prod_year);
                    ps.executeUpdate();
                    conn.commit();
                }
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
            catch (FileNotFoundException e)
            {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                try{
                    if (ps != null) {
                        ps.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                try{
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        public void calculateSimilarity(){
            Connection conn = null;
            PreparedStatement psSelectAll = null;
            CallableStatement cs = null;
            ResultSet AllRecords= null;
            try{
                conn = connectionToDB();

                int maximal = maximalDis(conn); //return max distance

                String sqlSelectAll = "select * from MediaItems";
                psSelectAll = conn.prepareStatement(sqlSelectAll);
                AllRecords = psSelectAll.executeQuery();
//                psSelectAll.close();
//
                while (AllRecords.next()){
                    long mid1 = AllRecords.getLong("MID");
                    while(AllRecords.next()){
                        long mid2 = AllRecords.getLong("MID");
                        cs = conn.prepareCall("{?= call SimCalculation(?,?,?)}");
                        cs.setLong(2,mid1);
                        cs.setLong(3,mid2);
                        cs.setInt(4,maximal);
                        cs.registerOutParameter(1, oracle.jdbc.OracleTypes.FLOAT);
                        cs.execute();
                        float curSim = cs.getFloat(1);
                        cs.close();
                        insertToTable(mid1,mid2,curSim, conn);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            finally {
                try{
                    if (psSelectAll != null) {
                        psSelectAll.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                try{
                    if (cs != null) {
                        cs.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                try{
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        public void insertToTable(long mid1, long mid2, float curSim, Connection conn){
//            Connection conn = _conn;
            PreparedStatement preparedStatementForInsert = null;
            try {
                String insertQuery = "INSERT INTO SIMILARITY (MID1,MID2,SIMILARITY) VALUES(?,?,?)";
                preparedStatementForInsert = conn.prepareStatement(insertQuery);
                preparedStatementForInsert.setLong(1, mid1);
                preparedStatementForInsert.setLong(2, mid2);
                preparedStatementForInsert.setFloat(3, curSim);
                preparedStatementForInsert.executeUpdate();
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            finally {
                try {
                    if (preparedStatementForInsert != null) {
                        preparedStatementForInsert.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
//                try{
//                    if (conn != null) {
//                        conn.close();
//                    }
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }

            }}

        public int maximalDis(Connection conn){
//            Connection conn = null;
//            PreparedStatement ps = null;
            CallableStatement cs = null;
            int max = 0;
            try{
//                Class.forName("oracle.jdbc.driver.OracleDriver");
//                conn = DriverManager.getConnection(this.Connection, this.DBusername, this.DBpassword);
                cs= conn.prepareCall("{?= call MaximalDistance()}");
                cs.registerOutParameter(1, oracle.jdbc.OracleTypes.NUMBER);
                cs.execute();
                max = cs.getInt(1);
//            } catch (ClassNotFoundException e) {
//                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            finally {
                try{
                    if (cs != null) {
                        cs.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
//                try{
//                    if (conn != null) {
//                        conn.close();
//                    }
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
            }
            return max;
        }
    }



