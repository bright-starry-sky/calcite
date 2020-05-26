/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.test;

import org.apache.calcite.jdbc.CalciteConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
//import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @Description:
 * @Date:Create in 下午5:38 2018/9/15
 * @Modified By:
 */
public class CbsMySqlTest {
  public static void main(String[] args){
    try {
      Class.forName("com.mysql.jdbc.Driver");
      Properties info = new Properties();
      info.put("model", "inline:"
                    + "{\n"
                    + "  version: '1.0',\n"
                    + "  defaultSchema: 'cbs_db',\n"
                    + "  schemas: [\n"
                    + "    {\n"
                    + "      name: 'cbs_db',\n"
                    + "      type: 'custom',\n"
                    + "      factory: 'org.apache.calcite.adapter.jdbc.JdbcSchema$Factory',\n"
                    + "      operand: {\n"
                    + "        jdbcDriver: 'com.mysql.jdbc.Driver',\n"
                    + "        jdbcUrl:'jdbc:mysql://localhost:3306/cbs_db',\n"
                    + "        jdbcUser: 'root',\n"
                    + "        jdbcPassword: '12345678'\n"
                    + "      }\n"
                    + "    }\n"
                    + "  ]\n"
                    + "}");

      Connection connection =
          DriverManager.getConnection("jdbc:calcite:", info);
      // must print "directory ... not found" to stdout, but not fail
      Statement statement = connection.createStatement();
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
//      String sql = "select * from t1 where col1 = 1 and col2 = col1";
      String sql = "select col1 , col2 from t1 where col1 = 1 or col1 = 2";
//      String sql = "select col1 , col2 from t1 where col1 = 1 and col1 = col2";
      ResultSet resultSet =
          statement.executeQuery(sql);

      ResultSet tables =
          connection.getMetaData().getTables(null, null, null, null);

      final StringBuilder buf = new StringBuilder();
      while (resultSet.next()) {
        int n = resultSet.getMetaData().getColumnCount();
        for (int i = 1; i <= n; i++) {
          buf.append(i > 1 ? "; " : "")
              .append(resultSet.getMetaData().getColumnLabel(i))
              .append("=")
              .append(resultSet.getObject(i));
        }
        System.out.println(buf.toString());
        buf.setLength(0);
      }
      resultSet.close();
      statement.close();
      connection.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
