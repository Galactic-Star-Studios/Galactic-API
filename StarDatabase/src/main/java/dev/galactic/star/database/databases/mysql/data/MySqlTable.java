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

package dev.galactic.star.database.databases.mysql.data;

import dev.galactic.star.database.databases.mysql.MySqlDb;
import dev.galactic.star.database.impl.exceptions.InvalidConnectionException;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The class that handles altering tables.
 */
public class MySqlTable {
    private final MySqlDb dbInstance;

    /**
     * Main constructor.
     *
     * @param instance MySqlDb instance, so it can access the fields/methods in the class.
     */
    public MySqlTable(MySqlDb instance) {
        this.dbInstance = instance;
    }

    /**
     * Checks whether the column exists or not.
     *
     * @param tableName  Name of the table to check the columns for.
     * @param columnName Column name to check whether it exists.
     * @return True or false.
     */
    public boolean columnExists(String tableName, String columnName) {
        return this.retrieveColumns(tableName).contains(columnName);
    }

    /**
     * Drops the column in the table specified.
     *
     * @param tableName  Name of the table that houses the column.
     * @param columnName Name of the column to drop.
     * @return MySqlTable instance.
     */
    public MySqlTable deleteColumn(String tableName, String columnName) {
        try (PreparedStatement stmt = this.dbInstance.getConnection().prepareStatement("ALTER TABLE " + tableName +
                " DROP COLUMN " + columnName + ";")) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * 'Renames the table with the given name to a new name.
     *
     * @param oldTableName Name of the table to rename.
     * @param newTableName Name to rename the table to.
     * @return MySqlTable current instance.
     */
    public MySqlTable renameTable(String oldTableName, String newTableName) {
        try (PreparedStatement stmt =
                     this.dbInstance.getConnection().prepareStatement("RENAME TABLE " + oldTableName + " TO " + newTableName + ";")) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * Renames a column in the given table to a new name.
     *
     * @param tableName     Name of the table that contains the column you want to rename.
     * @param oldColumnName Name of the column you want to rename.
     * @param newColumnName The name you want to rename the column to.
     * @return MySqlTable current instance.
     */
    public MySqlTable renameColumn(String tableName, String oldColumnName, String newColumnName) {
        try (PreparedStatement stmt = this.dbInstance.getConnection().prepareStatement("ALTER TABLE " + tableName +
                " RENAME COLUMN " + oldColumnName + " TO " + newColumnName + ";")) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * Checks whether the table exists or not.
     *
     * @param tableName Name of the table to check.
     * @return True or false.
     */
    public boolean tableExists(String tableName) {
        return this.retrieveTables().contains(tableName);
    }

    /**
     * Deletes the table with the names specified.
     *
     * @param tableNames Tables to delete.
     * @return MySqlTable current instance.
     */
    public MySqlTable deleteTables(String... tableNames) {
        String tables = Arrays.toString(tableNames)
                .replace("[", "")
                .replace("]", "");
        try (PreparedStatement stmt =
                     this.dbInstance.getConnection().prepareStatement("DROP TABLE IF EXISTS" + tables + ";")) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * Unlocks all tables.
     *
     * @return MySqlTable current instance.
     */
    public MySqlTable unlockTables() {
        try (PreparedStatement stmt = this.dbInstance.getConnection().prepareStatement("UNLOCK TABLES;")) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * Locks the table in a read or write state.
     *
     * @param tableName Name of the table to lock.
     * @param readOnly  Whether to lock it as read only. True to lock it to the read only state, false for a write
     *                  only state.
     * @return MySqlTable current instance;
     */
    public MySqlTable lockTable(String tableName, boolean readOnly) {
        String query;
        if (readOnly) {
            query = "LOCK TABLE " + tableName + " READ;";
        } else {
            query = "LOCK TABLE " + tableName + " WRITE;";
        }
        try (PreparedStatement stmt = this.dbInstance.getConnection().prepareStatement(query)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * Gets the tables in the database.
     *
     * @return List&lt;String&gt; of the table names.
     */
    public List<String> retrieveTables() {
        List<String> tableNames = new ArrayList<>();

        Connection connection = dbInstance.getConnection();
        if (MySqlDb.isInvalid(connection)) {
            try {
                throw new InvalidConnectionException("Connection is invalid.");
            } catch (InvalidConnectionException e) {
                throw new RuntimeException(e);
            }
        }

        try (PreparedStatement stmt = connection.prepareStatement("SHOW TABLES;");
             ResultSet resultSet = stmt.executeQuery()) {
            while (resultSet.next()) {
                int columnsCount = resultSet.getMetaData().getColumnCount();
                for (int i = 1; i <= columnsCount; i++) {
                    tableNames.add(resultSet.getObject(i).toString());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return tableNames;
    }

    /**
     * Gets the columns in the table that is specified.
     *
     * @param tableName Name of the table you want to get the columns of.
     * @return List&lt;String&gt; of the column names.
     */
    public List<String> retrieveColumns(String tableName) {
        List<String> columns = new ArrayList<>();

        Connection connection = dbInstance.getConnection();
        if (MySqlDb.isInvalid(connection)) {
            try {
                throw new InvalidConnectionException("Connection is invalid.");
            } catch (InvalidConnectionException e) {
                throw new RuntimeException(e);
            }
        }

        if (this.retrieveTables().contains(tableName)) {
            try (PreparedStatement stmt = connection.prepareStatement(" SELECT * FROM " + tableName + ";");
                 ResultSet resultSet = stmt.executeQuery()) {
                ResultSetMetaData resultSetMeta = resultSet.getMetaData();
                while (resultSet.next()) {
                    int columnsCount = resultSet.getMetaData().getColumnCount();
                    for (int i = 1; i <= columnsCount; i++) {
                        columns.add(resultSetMeta.getColumnName(i));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return columns;
    }

    /**
     * Returns an instance of MySqlDb.
     *
     * @return MySqlDb instance.
     * @see MySqlDb
     */
    public MySqlDb instance() {
        return this.dbInstance;
    }
}
