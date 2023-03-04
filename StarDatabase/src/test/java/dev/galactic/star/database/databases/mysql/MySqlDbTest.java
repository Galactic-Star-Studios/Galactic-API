/*
 * Copyright 2022 Galactic Star Studios
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.galactic.star.database.databases.mysql;

import dev.galactic.star.database.databases.mysql.data.MySqlDatabase;
import dev.galactic.star.database.databases.mysql.data.TableNoAnnotationTest;
import dev.galactic.star.database.databases.mysql.data.TableTest;
import dev.galactic.star.database.databases.mysql.data.TableTest2;
import dev.galactic.star.database.databases.mysql.data.database.DbParentTest;
import dev.galactic.star.database.impl.exceptions.InvalidConnectionException;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(OrderAnnotation.class)
class MySqlDbTest {
    private static Connection connection;
    private static MySqlDb db;
    private static MySqlDatabase dbUtil;
    private String[] databases = new String[]{"test", "test_db", "test_db2", "test_db_3"};

    @Test
    @Order(1)
    void connect() {
        try {
            db = new MySqlDb("cimeyclust.com", 6009, "mysql", "root", "very_safe_password1234")
                    .connect();
            assertTrue(db.isConnected());
            connection = db.getConnection();
            dbUtil = db.getDatabaseMgr();

            dbUtil.deleteDatabases("test", "test_db", "test_db2", "test_db__3");
            db.createTables(new DbParentTest(), new TableTest(), new TableTest2());
        } catch (InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @Order(2)
    void createDatabase() {
        dbUtil.createDatabases("test", "test_db", " test_db2", "test_db_3");
    }

    @Test
    @Order(3)
    void switchDatabase() {
        dbUtil.switchDatabase("test_db2");
        try {
            assertEquals(connection.getCatalog(), "test_db2");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        dbUtil.switchDatabase("test_db");
    }

    @Test
    @Order(4)
    void setConnection() {
        assertThrows(RuntimeException.class, () ->
                db.setConnection(null));
    }

    @Test
    @Order(5)
    void isInvalid() {
        assertTrue(MySqlDb.isInvalid(null));
    }

    @Test
    @Order(6)
    void isConnected() {
        assertTrue(db.isConnected());
    }

    @Test
    @Order(7)
    void setHost() {
        assertDoesNotThrow(() -> db.setConnection(connection));
    }

    @Test
    @Order(8)
    void setPort() {
        assertThrows(RuntimeException.class, () ->
                db.setPort(3306));
    }

    @Test
    @Order(9)
    void setDatabaseName() {
        assertDoesNotThrow(() ->
                db.setDatabaseName("test_db1"));
    }

    @Test
    @Order(10)
    void setUsername() {
        assertThrows(RuntimeException.class, () ->
                db.setUsername("test"));
    }

    @Test
    @Order(11)
    void setPassword() {
        assertThrows(RuntimeException.class, () ->
                db.setHost("test_password1234"));
    }

    @Test
    @Order(12)
    void setParameters() {
        assertThrows(RuntimeException.class, () ->
                db.setParameters("some parameters"));
    }

    @Test
    @Order(14)
    void createTables() {
        //Checks to see if the table that has no @Table annotation throws an exception
        assertThrows(RuntimeException.class, () ->
                db.createTables(new TableNoAnnotationTest()));

        //Checks whether the table exists, and then creates it.
        db.createTables(new DbParentTest(), new TableTest(), new TableTest2());
        dbUtil.createDatabases("test_db");
        assertTrue(dbUtil.databaseExists("test_db"));

        String[] tables = {"table_test", "test_tbl1", "test_tbl2", "test_tbl3"};
        assertTrue(() -> {
            int tableCount = 0;
            int columnCount = 0;
            for (String tbl : db.getTableMgr()
                    .retrieveTables()
                    .stream()
                    .filter(e -> Arrays.asList(tables).contains(e))
                    .collect(Collectors.toList())) {
                columnCount += (long) db.getTableMgr()
                        .retrieveColumns(tbl)
                        .size();

                tableCount++;
            }
            return (tableCount + columnCount) == (16);
        });

        //Checks whether the table exists and then creates it from the table class
        assertTrue(db.getTableMgr().tableExists("table_test"));
    }

    @Test
    @Order(13)
    void insert() {
        assertThrows(
                RuntimeException.class,
                () -> db.insert("fake_db", new String[]{"fake_column"}, new Object[]{100})
        );

        assertThrows(
                RuntimeException.class,
                () -> db.insert("test_tbl1", new String[]{"count", "user_names", "password"}
                        , new Object[]{1, "user1", "pass1"})
        );

        Stream.of(new String[]{"table_test", "test_tbl1", "test_tbl2", "test_tbl3"})
                .forEach(e -> {
                    db.insert(e, new String[]{"count", "user_names", "passwords"}, new Object[]{1, "user", "pass"});
                });
        //TODO FIND AND CHECK WHETHER IT WAS INSERTED

    }

    @Test
    @Order(15)
    void testGetDatabases() {
        List<String> dbs = dbUtil.retrieveDatabases();
        assertTrue(dbs.containsAll(Arrays.asList(databases)));
    }

    @Test
    @Order(16)
    void databaseExists() {
        List<String> dbs = dbUtil.retrieveDatabases();
        assertTrue(dbs.containsAll(Arrays.asList(databases)));
    }

    @Test
    @Order(17)
    void deleteDatabase() {
        List<String> dbs = dbUtil.retrieveDatabases();
        assertTrue(dbs.containsAll(Arrays.asList(databases)));
        dbUtil.deleteDatabases("test", "test_db", "test_db2", "test_db__3");
        dbs = dbUtil.retrieveDatabases();
        assertFalse(dbs.containsAll(Arrays.asList(databases)));
    }

    @Test
    @Order(18)
    void close() {
        try {
            db.close();
        } catch (InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
        assertFalse(db.isConnected());
        assertTrue(MySqlDb.isInvalid(connection));
    }
}
