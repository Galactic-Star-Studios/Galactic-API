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
import dev.galactic.star.database.impl.annotations.Database;
import dev.galactic.star.database.impl.exceptions.InvalidConnectionException;

import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * The database utility class.
 */
public class MySqlDatabase {
    private final MySqlDb dbInstance;
    private final Connection connection;
    private String databaseName;

    /**
     * Main constructor.
     *
     * @param dbInstance MySqlDb instance.
     * @see MySqlDb
     */
    public MySqlDatabase(MySqlDb dbInstance) {
        this.dbInstance = dbInstance;
        this.connection = dbInstance.getConnection();
        this.databaseName = dbInstance.getDatabaseName();
    }

    /**
     * Switches to the database specified.
     *
     * @param databaseName The name of the database to switch to.
     * @return Current instance of MySqlDb.
     * @see MySqlDb
     */
    public MySqlDatabase switchDatabase(String databaseName) {
        try {
            if (MySqlDb.isInvalid(this.connection)) {
                throw new InvalidConnectionException("Connection is invalid.");
            }
            connection.setCatalog(databaseName);
            this.databaseName = databaseName;
            this.dbInstance.setDatabaseName(databaseName);
        } catch (SQLException | InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * Creates a database if you have the permission.
     *
     * @param names Names of the databases to create.
     * @return Current instance of MySqlDb.
     * @see MySqlDb
     */
    public MySqlDatabase createDatabases(String... names) {
        for (String name : names) {
            try (PreparedStatement stmt =
                         this.connection.prepareStatement("CREATE DATABASE IF NOT EXISTS " + name + ";")) {
                if (MySqlDb.isInvalid(this.connection)) {
                    throw new InvalidConnectionException("Connection is invalid.");
                }
                stmt.executeUpdate();
            } catch (SQLException | InvalidConnectionException e) {
                throw new RuntimeException(e);
            }
        }
        return this;
    }

    /**
     * Creates from the object specified.
     *
     * @param classes Classes that has the @Database annotation.
     * @return Current instance.
     * @see Database
     * @see MySqlDb
     */
    public MySqlDatabase createDatabases(Class<?>... classes) {
        for (Class<?> clazz : classes) {
            if (!clazz.isAnnotationPresent(Database.class)) {
                throw new InvalidParameterException("That object doesn't have a Database annotation.");
            }
            this.createDatabases(clazz.getAnnotation(Database.class).name());
        }
        return this;
    }

    /**
     * Returns an array of the names of databases that exist.
     *
     * @return List&lt;String&gt; of the database names.
     */
    public List<String> retrieveDatabases() {
        return this.retrieveDatabases(null);
    }

    /**
     * Returns an array of the names of databases that exist, but with a certain pattern.
     *
     * @param pattern The pattern to filter out the databases that only match it. Will update this as soon as I can
     *                find a source with all the patterns.
     * @return List&lt;String&gt; of the database names.
     */
    public List<String> retrieveDatabases(String pattern) {
        List<String> databaseNames = new ArrayList<>();
        String patternQuery = pattern == null || pattern.isEmpty() ? ";" : " WHERE " + pattern + ";";
        if (MySqlDb.isInvalid(this.connection)) {
            try {
                throw new InvalidConnectionException("Connection is invalid.");
            } catch (InvalidConnectionException e) {
                throw new RuntimeException(e);
            }
        }
        try (PreparedStatement stmt = this.connection.prepareStatement("SHOW DATABASES" + patternQuery);
             ResultSet resultSet = stmt.executeQuery()) {
            while (resultSet.next()) {
                int columnsCount = resultSet.getMetaData().getColumnCount();
                for (int i = 1; i <= columnsCount; i++) {
                    databaseNames.add(resultSet.getObject(i).toString());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return databaseNames;
    }

    /**
     * Checks whether the database with the given name exists already.
     *
     * @param name Name to check for.
     * @return True or false.
     */
    public boolean databaseExists(String name) {
        return this.retrieveDatabases().contains(name);
    }

    /**
     * Deletes, or drops, the database with the name specified.
     *
     * @param names Names of the databases to delete.
     * @return Current instance of MySqlDb.
     * @see MySqlDb
     */
    public MySqlDatabase deleteDatabases(String... names) {
        for (String name : names) {
            try (PreparedStatement stmt = this.connection.prepareStatement("DROP DATABASE IF EXISTS " + name + ";"
            )) {
                if (MySqlDb.isInvalid(this.connection)) {
                    throw new InvalidConnectionException("Connection is invalid.");
                }
                stmt.executeUpdate();
            } catch (SQLException | InvalidConnectionException e) {
                throw new RuntimeException(e);
            }
        }
        return this;
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
