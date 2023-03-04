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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The user utility class.
 */
public class MySqlUser {
    private final MySqlDb dbInstance;
    private final Connection connection;

    /**
     * Main constructor.
     *
     * @param db MySqlDb instance.
     * @see MySqlDb
     */
    public MySqlUser(MySqlDb db) {
        this.dbInstance = db;
        this.connection = db.getConnection();
    }

    /**
     * @param username Username of the user.
     * @param password Password of the user.
     * @return Current instance of MySqlDb.
     * @see MySqlDb
     */
    public MySqlUser createUser(String username, String password) {
        return this.createUser("", username, password);
    }

    /**
     * @param hostname The host to access it from. Put null or blank string if you want to be able to access it from
     *                 any host on the server.
     * @param username Username of the user.
     * @param password Password of the user.
     * @return Current instance of MySqlDb.
     * @see MySqlDb
     */
    public MySqlUser createUser(String hostname, String username, String password) {
        String userQuery = hostname == null || hostname.isEmpty() ? "'" + username + "'@'%'" :
                "'" + username + "'@'" + hostname + "'";
        try (PreparedStatement stmt = this.connection.prepareStatement("CREATE USER " + userQuery + " IDENTIFIED BY " +
                "'" + password + "';")) {
            if (MySqlDb.isInvalid(this.connection)) {
                throw new InvalidConnectionException("Connection is invalid.");
            }
            System.out.println("CREATE USER " + userQuery + " IDENTIFIED BY " +
                    "'" + password + "';");
            stmt.execute();
        } catch (SQLException | InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * Deletes the user.
     *
     * @param accountNames A String varargs
     * @param host         The host name that the user is tied to. galactic-star.dev would be the host name. So it
     *                     would end
     *                     up like john@galactic-star.dev
     * @return Current instance of MySqlDb.
     * @see MySqlDb
     */
    public MySqlUser deleteUser(String host, String... accountNames) {
        if (accountNames.length == 0) {
            throw new IllegalArgumentException("There needs to be at least one user in the varargs.");
        }
        String userHost = host == null || host.isEmpty() ? "%" : host;
        String usersQuery = Arrays.toString(accountNames)
                .replace("[", "'")
                .replace("]", "'@'" + userHost + "';")
                .replace(", ", "'@'" + userHost + "',");
        try (PreparedStatement stmt = this.connection.prepareStatement("DROP USER " + usersQuery)) {
            if (MySqlDb.isInvalid(this.connection)) {
                throw new InvalidConnectionException("Connection is invalid.");
            }
            // stmt.executeUpdate();
        } catch (SQLException | InvalidConnectionException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * Changes a specific user's password. You don't need to specify a host because it defaults to % which kinda
     * means it is universal
     *
     * @param username    The username of the user who you want to change the password.
     * @param newPassword the new password.
     * @return Current instance of MySqlDb.
     * @see MySqlDb
     */
    public MySqlUser changeUserPassword(String username, String newPassword) {
        return this.changeUserPassword("%", username, newPassword);
    }

    /**
     * Changes a specific user's password.
     *
     * @param host        The hostname that appears after the @ in a query. Such as root@galactic-star.dev
     * @param username    The username of the user who you want to change the password.
     * @param newPassword the new password.
     * @return Current instance of MySqlDb.
     * @see MySqlDb
     */
    public MySqlUser changeUserPassword(String host, String username, String newPassword) {
        String user = "'" + username + "'@'" + host + "'";

        try (PreparedStatement stmt =
                     this.connection.prepareStatement("ALTER USER " + user + " IDENTIFIED BY '" + newPassword + "';")) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
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

    /**
     * Returns an array of the names of users that exist, but with a certain pattern.
     *
     * @return List&lt;String&gt; of the database names.
     */
    public List<String> retrieveUsers() {
        List<String> databaseNames = new ArrayList<>();
        if (MySqlDb.isInvalid(this.connection)) {
            try {
                throw new InvalidConnectionException("Connection is invalid.");
            } catch (InvalidConnectionException e) {
                throw new RuntimeException(e);
            }
        }
        try (PreparedStatement stmt = this.connection.prepareStatement("SELECT user FROM user;");
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
}
