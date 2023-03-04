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
import dev.galactic.star.database.databases.mysql.data.MySqlTable;
import dev.galactic.star.database.databases.mysql.data.MySqlUser;
import dev.galactic.star.database.impl.annotations.Database;
import dev.galactic.star.database.impl.annotations.Table;
import dev.galactic.star.database.impl.annotations.TableColumn;
import dev.galactic.star.database.impl.exceptions.AnnotationNotFoundException;
import dev.galactic.star.database.impl.exceptions.InvalidConnectionException;

import java.lang.reflect.Field;
import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * The database API used to interact with a MySQL database.
 */

public class MySqlDb {
    private Connection connection;
    private String host;
    private int port;
    private String databaseName;
    private String username;
    private String password;
    private String parameters;


    /**
     * A constructor for the MySQL database that includes any extra queries you want to use.
     *
     * @param host         Host of the server.
     * @param port         Port of the database that is running on.
     * @param databaseName Name of the database in the database you want to interact with. Can be null or blank if you
     *                     don't want to interact just yet.
     * @param username     Username of the login details.
     * @param password     Password of the login details.
     * @param parameters   Any other attributes and values you want to use in the connection. Checkout
     *                     <a href="https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html" target="_blank">MySQL Dev website</a> for more info on the different parameters.
     */
    public MySqlDb(String host, int port, String databaseName, String username, String password, String parameters) {
        this.host = host;
        this.port = port;
        this.databaseName = databaseName;
        this.username = username;
        this.password = password;
        this.parameters = parameters;
    }

    /**
     * The base constructor for the MySQL database that includes.
     *
     * @param host         Host of the server.
     * @param port         Port of MySQL that is running on.
     * @param databaseName Name of the database in the MySQL database you want to interact with. Can be null or blank
     *                     if you
     *                     don't want to interact just yet.
     * @param username     Username of the login details.
     * @param password     Password of the login details.
     */
    public MySqlDb(String host, int port, String databaseName, String username, String password) {
        this(host, port, databaseName, username, password, null);
    }

    /**
     * A constructor that takes in an already existing connection.
     *
     * @param connection The Java connection object.
     *                   <a href="https://www.javatpoint.com/example-to-connect-to-the-mysql-database" target="_blank">Here are some examples.</a>
     */
    public MySqlDb(Connection connection) {
        this.connection = connection;
    }

    /**
     * Checks whether the connection is null or isn't connected (Invalid).
     *
     * @param connection Java Connection object.
     * @return true or false.
     * @see Connection
     */
    public static boolean isInvalid(Connection connection) {
        try {
            return connection == null || connection.isClosed();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns an instance of MySqlUser to interact with users.
     *
     * @return MySqlUser instance.
     * @see MySqlUser
     */
    public MySqlUser getUserUtilClass() {
        return new MySqlUser(this);
    }

    /**
     * Returns an instance of MySqlDatabase to interact with the databases.
     *
     * @return MySqlDatabase instance.
     * @see MySqlDatabase
     */
    public MySqlDatabase getDatabaseMgr() {
        return new MySqlDatabase(this);
    }

    /**
     * Checks whether there is a connection already established to the database.
     *
     * @return true or false.
     */
    public boolean isConnected() {
        try {
            return !MySqlDb.isInvalid(this.connection) && !this.connection.isClosed();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The method that connects to MySQL.
     *
     * @return Current instance of the class.
     * @throws InvalidConnectionException If the program can't connect to MySQL.
     * @see MySqlDb
     */
    public MySqlDb connect() throws InvalidConnectionException {
        if (this.isConnected()) {
            throw new InvalidConnectionException("There is already a connection to the database.");
        }
        boolean parametersBlank = this.parameters == null || this.parameters.isEmpty() || parameters.matches("\\s");
        boolean databaseBlank = this.databaseName == null || this.databaseName.isEmpty() || databaseName.matches("\\s");
        String databaseQuery = databaseBlank ? "" : "/" + databaseName;
        String parameters = parametersBlank ? "" : "?" + this.parameters;
        String parameterString = databaseQuery + parameters;
        try {
            this.connection = DriverManager.getConnection(String.format("jdbc:mysql://%s:%d%s", host, port,
                    parameterString
            ), this.username, this.password);
        } catch (SQLException e) {
            throw new InvalidConnectionException("Can't connect to the database. Please check details and try again: "
                    + e.getMessage());
        }
        return this;
    }

    /**
     * Alters the table.
     *
     * @return MySqlTable instance.
     * @see MySqlTable
     */
    public MySqlTable getTableMgr() {
        return new MySqlTable(this);
    }

    /**
     * @param objects A varargs of objects that have the
     * @return True if successful, else false.
     */
    public MySqlDb createTables(Object... objects) {
        for (Object o : objects) {
            Class<?> c = o.getClass();
            if (c.isAnnotationPresent(Database.class)) {
                this.createTableFromDbAnnotation(o);
            } else if (c.isAnnotationPresent(Table.class)) {
                this.createTableFromTableAnnotation(o);
            } else {
                try {
                    throw new AnnotationNotFoundException("There should either be a @Database or @Table annotation in" +
                            " the object.");
                } catch (AnnotationNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return this;
    }

    private String createModifyColumnQuery(TableColumn colAnnotation, Object object,
                                           String tableName, Field field) throws IllegalAccessException {
        MySqlTable sqlTable = this.getTableMgr();
        StringBuilder builder = new StringBuilder();
        if (sqlTable.tableExists(tableName) && sqlTable.columnExists(tableName, colAnnotation.name())) {
            builder.append(" MODIFY ")
                    .append(colAnnotation.name())
                    .append(" ")
                    .append(field.get(object))
                    .append("(")
                    .append(colAnnotation.maxDisplayed())
                    .append(") ")
                    .append(colAnnotation.notNull() ? "NOT NULL " : "")
                    .append(colAnnotation.autoIncrement() ? "AUTO_INCREMENT " : "")
                    .append(colAnnotation.primaryKey() ? "PRIMARY KEY " : " ")
                    .append(colAnnotation.foreignKey())
                    .append(",");
        }
        return builder.toString();
    }

    private void createTableFromTableAnnotation(Object object) {
        try {
            Class<?> c = object.getClass();
            if (!c.isAnnotationPresent(Table.class)) {
                throw new InvalidParameterException("Table class doesn't have @Table annotation. It must have one.");
            }

            Table tbl = c.getAnnotation(Table.class);
            StringBuilder b = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tbl.table_name() + "(");
            for (Field f : c.getDeclaredFields()) {
                f.setAccessible(true);
                if (!f.isAnnotationPresent(TableColumn.class)) {
                    throw new InvalidParameterException("Table class doesn't have @TableColumn annotation. It must " +
                            "have " +
                            "one.");
                }
                TableColumn col = f.getAnnotation(TableColumn.class);
                if (!col.autoCreate()) {
                    continue;
                }
                this.createTableQueryBuilder(col, object, b, f);
                //If the table exists already and there are new columns, it adds the columns using the ALTER TABLE
                // query.
                this.createAlterTableQuery(col, object, b, tbl.table_name(), f);
            }
            String query;
            if (b.substring(0, 6).equals("ALTER ")) {
                query = b.replace(b.length() - 2, b.length(), ";").toString();
            } else {
                query = b.replace(b.length() - 2, b.length(), ");").toString();
            }

            try (PreparedStatement stmt = this.connection.prepareStatement(query)) {
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void createAlterTableQuery(TableColumn colAnnotation, Object object, StringBuilder builder,
                                       String tableName, Field field) throws IllegalAccessException {
        MySqlTable sqlTable = this.getTableMgr();
        if (sqlTable.tableExists(tableName)) {
            builder.delete(0, builder.length());

            builder.append("ALTER TABLE ")
                    .append(tableName);
            if (!sqlTable.columnExists(tableName, colAnnotation.name())) {
                field.setAccessible(true);
                builder.append(" ADD ")
                        .append(colAnnotation.name())
                        .append(" ")
                        .append(field.get(object))
                        .append("(")
                        .append(colAnnotation.maxDisplayed())
                        .append(") ")
                        .append(colAnnotation.notNull() ? "NOT NULL " : "")
                        .append(colAnnotation.autoIncrement() ? "AUTO_INCREMENT " : "")
                        .append(colAnnotation.primaryKey() ? "PRIMARY KEY " : " ")
                        .append(colAnnotation.foreignKey())
                        .append(",");
            } else {
                builder.append(this.createModifyColumnQuery(colAnnotation, object, tableName, field));
            }
            field.setAccessible(false);
        }
    }

    private void createTableQueryBuilder(TableColumn colAnnotation, Object object, StringBuilder builder,
                                         Field field) throws IllegalAccessException {
        builder.append(colAnnotation.name())
                .append(" ")
                .append(field.get(object))
                .append("(")
                .append(colAnnotation.maxDisplayed())
                .append(") ")
                .append(colAnnotation.notNull() ? "NOT NULL " : "")
                .append(colAnnotation.autoIncrement() ? "AUTO_INCREMENT " : "")
                .append(colAnnotation.primaryKey() ? "PRIMARY KEY" : "")
                .append(" ")
                .append(colAnnotation.foreignKey())
                .append(",");
    }

    private void createTableFromDbAnnotation(Object object) {
        try {
            MySqlDatabase utilityClass = this.getDatabaseMgr();
            Class<?> c = object.getClass();
            Database db = c.getAnnotation(Database.class);
            if (db.create_database()) {
                utilityClass.createDatabases(db.name());
            }
            if (!this.getDatabaseMgr().databaseExists(db.name())) {
                throw new IllegalArgumentException("That database doesn't exist. Please make sure it does.");
            }
            if (db.switchToDb()) {
                utilityClass.switchDatabase(db.name());
            }
            // Loops through all fields that are annotated with @Table
            for (Field field1 : c.getDeclaredFields()) {
                field1.setAccessible(true);
                if (!field1.isAnnotationPresent(Table.class)) continue;
                Table tbl = field1.getAnnotation(Table.class);
                StringBuilder b = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tbl.table_name() + "(");
                for (Field field2 : field1.get(object).getClass().getDeclaredFields()) {
                    Object o2 = field1.get(object);
                    field2.setAccessible(true);
                    if (!field2.isAnnotationPresent(TableColumn.class)) {
                        throw new InvalidParameterException("Table class doesn't have @TableColumn annotation. It " +
                                "must have one.");
                    }
                    TableColumn col = field2.getAnnotation(TableColumn.class);
                    if (!col.autoCreate()) {
                        continue;
                    }
                    this.createTableQueryBuilder(col, o2, b, field2);
                    this.createAlterTableQuery(col, o2, b, tbl.table_name(), field2);
                    field2.setAccessible(false);
                }
                field1.setAccessible(false);
                String query = null;
                if (b.substring(0, 6).equals("ALTER ")) {
                    query = b.replace(b.length() - 2, b.length(), ";").toString();
                } else {
                    query = b.replace(b.length() - 2, b.length(), ");").toString();
                }
                try (PreparedStatement stmt = this.connection.prepareStatement(query)) {
                    stmt.executeUpdate();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Method for inserting values into a MySQL table.
     *
     * @param table   Table name.
     * @param columns List of the column names.
     * @param values  List of the objects you want to insert into the columns specified.
     * @return Current class instance.
     * @see MySqlDb
     */
    public MySqlDb insert(String table, String[] columns, Object[] values) {

        String columnQuery = Arrays.toString(columns)
                .replace("[", "(")
                .replace("]", ")");
        String valueQuery = Arrays.toString(values)
                .replace("[", "('")
                .replace("]", "')")
                .replace(", ", "','");
        try (PreparedStatement stmt = this.connection.prepareStatement("INSERT INTO " + table + columnQuery + " " +
                "VALUES " + valueQuery + ";")) {
            if (MySqlDb.isInvalid(this.connection)) {
                throw new InvalidConnectionException("Connection is invalid.");
            }
            stmt.executeUpdate();
        } catch (SQLException | InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * Method for updating values of a MySQL table.
     *
     * @param table            Table name.
     * @param columns          List of the column names.
     * @param values           List of the objects you want to update.
     * @param comparableColumn The column name to compare.
     * @param comparableValue  The value in the column to compare.
     * @return Current class instance.
     * @see MySqlDb
     */
    public MySqlDb update(String table, String[] columns, Object[] values, String comparableColumn,
                          String comparableValue) {

        StringBuilder setQuery = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            if (i == columns.length - 1) {
                setQuery.append(columns[i] + " = " + "'" + values[i].toString() + "'");
                continue;
            }
            setQuery.append(columns[i] + " = " + "'" + values[i].toString() + "', ");
        }
        try (PreparedStatement stmt = this.connection.prepareStatement("UPDATE " + table + " SET " + setQuery +
                " WHERE " + comparableColumn + " = " + comparableValue + ";")) {
            if (MySqlDb.isInvalid(this.connection)) {
                throw new InvalidConnectionException("Connection is invalid.");
            }
            stmt.executeUpdate();
        } catch (SQLException | InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * Getter for connection.
     *
     * @return Java Connection object.
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Setter for the connection of the database.
     *
     * @param connection Java Connection object.
     */
    public void setConnection(Connection connection) {
        try {
            if (MySqlDb.isInvalid(connection)) {
                throw new InvalidConnectionException("Can't change the connection when connection is invalid");
            }
            this.connection = connection;
        } catch (InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Closes the connection to the database.
     *
     * @return Current instance of MySqlDb.
     * @throws InvalidConnectionException When the connection object is null or already closed.
     * @see MySqlDb
     */
    public MySqlDb close() throws InvalidConnectionException {
        try {
            if (MySqlDb.isInvalid(this.connection)) {
                throw new InvalidConnectionException("Connection is invalid.");
            } else {
                this.connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * Getter for connection.
     *
     * @return Java Connection object.
     */
    public String getHost() {
        return host;
    }

    /**
     * Setter for the database host.
     *
     * @param host String host.
     */
    public void setHost(String host) {
        try {
            if (MySqlDb.isInvalid(this.connection) || this.isConnected()) {
                throw new InvalidConnectionException("Can't change the database host when connection is null or " +
                        "disconnected. Please disconnect and try again.");
            }
            this.host = host;
        } catch (InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Getter for the MySQL port its running on.
     *
     * @return Port of MySQL.
     */
    public int getPort() {
        return port;
    }

    /**
     * Setter for the port of the database.
     *
     * @param port Integer port.
     */
    public void setPort(int port) {
        try {
            if (MySqlDb.isInvalid(this.connection) || this.isConnected()) {
                throw new InvalidConnectionException("Can't change the port when connection is null or disconnected. "
                        + "Please disconnect and try again.");
            }
            this.port = port;
        } catch (InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Getter for database name.
     *
     * @return database name. It is null if one wasn't specified.
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Setter for database name.
     *
     * @param databaseName database name to set.
     */
    public void setDatabaseName(String databaseName) {
        try {
            if (MySqlDb.isInvalid(this.connection)) {
                throw new InvalidConnectionException("Can't change the database name when connection is null or " +
                        "disconnected. Please disconnect and try again.");
            }
            this.databaseName = databaseName;
        } catch (InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Getter for MySQL username.
     *
     * @return Username as a String.
     */
    public String getUsername() {
        return username;
    }

    /**
     * Setter for the login username.
     *
     * @param username Username as a String.
     */
    public void setUsername(String username) {
        try {
            if (MySqlDb.isInvalid(this.connection) || this.isConnected()) {
                throw new InvalidConnectionException("Can't change the username when connection is null or " +
                        "disconnected. Please disconnect and try again.");
            }
            this.username = username;
        } catch (InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Getter for the MySQL password.
     *
     * @return Password as a String.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Setter for the login password.
     *
     * @param password Password to set.
     */
    public void setPassword(String password) {
        try {
            if (MySqlDb.isInvalid(this.connection) || this.isConnected()) {
                throw new InvalidConnectionException("Can't change the password when connection is null or " +
                        "disconnected. Please disconnect and try again.");
            }
            this.password = password;
        } catch (InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Getter for connection parameters.
     *
     * @return Parameters used in the connection.
     */
    public String getParameters() {
        return parameters;
    }

    /**
     * Setter for the connection parameters.
     *
     * @param parameters String parameters list.
     */
    public void setParameters(String parameters) {
        try {
            if (MySqlDb.isInvalid(this.connection) || this.isConnected()) {
                throw new InvalidConnectionException("Can't change the username when connection is null or " +
                        "connected/disconnected. Please disconnect and try again.");
            }
            this.parameters = parameters;
        } catch (InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
    }
}
