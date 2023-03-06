/*
 * Copyright 2022-2022 Galactic Star Studios
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

package dev.galactic.star.database.impl;

import dev.galactic.star.database.databases.MySqlDatabase;
import dev.galactic.star.database.impl.objects.Table;

import java.io.InvalidClassException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The main Class used to interact with the different databases.
 *
 * @author PrismoidNW
 */
public abstract class StarDatabase {

    public final Set<Table> tables = new HashSet<>();
    /**
     * The database connection object.
     */
    private Connection connection;

    /**
     * Whether it is in debug mode.
     */
    private boolean debug;

    /**
     * The type of supported database at the moment. It takes an enum with several values.
     *
     * @param dbType Returns a new instance of the database corresponding with the type set. It inherits The
     *               StarDatabase class.
     * @return StarDatabase
     */
    public static StarDatabase type(DatabaseType dbType) {
        if (dbType == DatabaseType.MYSQL) {
            return new MySqlDatabase();
        }
        try {
            throw new Exception(
                    "Please report this to the developers by opening an issue. Error: forgot to add case " +
                            "statement" +
                            " in switch.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks whether the connection is not null and is not closed.
     *
     * @return boolean(true / false) whether the connection is not null AND not closed already.
     * @throws SQLException The exception thrown by the isClosed method of the Java Connection class.
     */
    public boolean isOpen() throws SQLException {
        return connection != null && !connection.isClosed();
    }

    /**
     * The abstract method that is called after calling the type method.
     *
     * @param username     The username to the login of the database.
     * @param password     The password to the login of the database. Recommended to have a strong, long password.
     * @param host         The url, domain, or IP of the database itself.
     * @param tableName    The name that will identify the table it is to change.
     * @param port         The port that goes along with the host url when connecting.
     * @param extraQueries The extra queries one may want to add.
     * @return A new instance of the class corresponding with the type you set in the beginning.
     */
    public abstract StarDatabase connect(String username, String password, String host, String tableName, int port,
                                         String extraQueries);

    /**
     * The abstract method that alters the tableName's name, tableName field, add or delete existing columns.
     *
     * @param tableName     nameOfTheTable
     * @param thingToChange The updated version of the tableName you want to update. You must get the instance of the
     *                      tableName and then change it to your desires.
     * @param objects       List of arguments depending on the thing to change.
     */
    public abstract void alterTable(String tableName, AlterTableType thingToChange, Object... objects);

    /**
     * The abstract method that inserts multiple pieces of data into a multiple columns.
     *
     * @param tableName       Name of the table where the column belongs to.
     * @param columns         The list of columns that data is going to be inserted into.
     * @param objectsToInsert The list of objects or data to be inserted.
     * @return Instance of StarDatabase so that it can be chained.
     */
    public abstract StarDatabase insert(String tableName, String[] columns, Object[] objectsToInsert);

    /**
     * Gets the table object by its name.
     *
     * @param tableName Name of the table.
     * @return Table object.
     */
    public Table getTableByName(String tableName) {
        List<Table> tableStream = this.getTables()
                .stream()
                .filter(e -> e.getName()
                        .equals(tableName))
                .collect(Collectors.toList());
        if (tableStream.size() == 0) {
            throw new IllegalArgumentException("No table found with the name: \"" + tableName + "\"");
        } else {
            return tableStream.get(0);
        }
    }

    /**
     * The abstract method that is called after connecting to create a database.
     *
     * @param tables the list of tables to create.
     * @throws SQLException if you don't have any tables to create.
     */
    public abstract void createTables(Table... tables) throws SQLException;

    /**
     * The abstract method that is called after connecting to create a database.
     *
     * @param tables the list of classes, annotated by the DatabaseTable annotation.
     * @throws SQLException if you don't have any tables to create.
     */
    public abstract void createTables(Class<?>... tables) throws SQLException, InvalidClassException;


    /**
     * Returns the connection of the database.
     *
     * @return Connection.
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * This is a protected method that sets the connections for child classes.
     *
     * @param connection The connection to set.
     */
    protected void setConnection(Connection connection) {
        this.connection = connection;
    }

    /**
     * This method allows to check the debug mode from the current Database.
     *
     * @return A boolean, which is true if debug mode is enabled and false if not.
     */
    public boolean isDebug() {
        return this.debug;
    }

    /**
     * This method allows to set the debug mode to true or false.
     *
     * @param debug if true, error messages with their stacktrace, internal infos and warnings will be printed
     *              out completely. if false errors and warnings will be distinct to their core information.
     */
    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    /**
     * Disconnects the connection if not already disconnected.
     *
     * @throws SQLException when either it is null or already closed.
     */
    public void disconnect() throws SQLException {
        if (this.isDebug()) {
            System.out.println("Attempting to disconnect...");
        }
        if (connection == null) {
            throw new SQLException("Connection is \"null\".");
        } else if (!connection.isClosed()) {
            throw new SQLException("Connection is already closed.");
        }
        connection.close();
        if (this.isDebug()) {
            System.out.println("Disconnected");
        }
    }

    public Set<Table> getTables() {
        return tables;
    }

    /**
     * The types of changeable content in the table.
     *
     * @author PrismoidNW
     */
    public enum AlterTableType {
        ADD_COLUMN,
        MODIFY_COLUMN,
        DROP_COLUMN,
        RENAME_TABLE
    }
}
