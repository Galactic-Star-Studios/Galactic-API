package dev.galactic.star.database;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class StarDatabase {

    private static Connection connection;

    public StarDatabase(DatabaseType type) {
        this(connection);
    }

    public StarDatabase(Connection connection) {
        StarDatabase.connection = connection;
    }

    public boolean isOpen() throws SQLException {
        return connection != null && !connection.isClosed();
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        StarDatabase.connection = connection;
    }

    protected abstract StarDatabase connect();

    public void disconnect() throws SQLException {
        if (connection == null) {
            throw new SQLException("Connection is \"null\".");
        } else if (!connection.isClosed()) {
            throw new SQLException("Connection is already closed.");
        } else {
            connection.close();
        }
    }
}
