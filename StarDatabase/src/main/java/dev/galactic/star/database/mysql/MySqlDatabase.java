package dev.galactic.star.database.mysql;

import dev.galactic.star.database.impl.StarDatabase;
import dev.galactic.star.database.impl.mapping.annotations.DatabaseTable;
import dev.galactic.star.database.impl.objects.Column;
import dev.galactic.star.database.impl.objects.Table;

import java.io.InvalidClassException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map.Entry;

/**
 * @author PrismoidNW
 */
public class MySqlDatabase extends StarDatabase {
    @Override
    public StarDatabase connect(String username, String password, String host, String tableName, int port,
                                String extraQueries) {
        try {
            String connectQuery = "jdbc:mysql://" + host + ":" + port + "/" + tableName + (extraQueries.isEmpty() ?
                    "" : "?" + extraQueries);
            this.setConnection(DriverManager.getConnection(connectQuery, username, password));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public void createTables(Table... tables) throws SQLException {
        if (tables.length == 0) {
            throw new SQLException("You can't have a varargs with nothing in it.");
        } else {
            for (Table table : tables) {
                StringBuilder mainSb = new StringBuilder("CREATE TABLE IF NOT EXISTS " + table.getName() + "(");
                for (Entry<String, Column> cl : table.getColumns().entrySet()) {
                    Column column = cl.getValue();
                    String key = cl.getKey();
                    mainSb.append(key)
                            .append(" ")
                            .append(column.getFieldType().name())
                            .append("(")
                            .append(column.getMaxSize())
                            .append(")")
                            .append(!column.canBeNull() ? " NOT NULL" : "")
                            .append(column.autoIncrements() ? " AUTO_INCREMENT" : "")
                            .append(", ");
                }
                boolean isEmpty = table.getPrimaryKey().isEmpty();
                if (isEmpty) {
                    mainSb.delete(mainSb.length() - 2, mainSb.length());
                }
                mainSb.append(!isEmpty ? "PRIMARY KEY(" + table.getPrimaryKey() + "));" : ");");
                PreparedStatement ps = this.getConnection().prepareStatement(mainSb.toString());
                ps.execute();
                ps.close();
            }
        }
    }

    @Override
    public void createTables(Class<?>... tables) throws SQLException {
        if (tables.length == 0) {
            throw new SQLException("You can't have a varargs with nothing in it.");
        } else {
            for (Class<?> table : tables) {
                StringBuilder mainSb = new StringBuilder("CREATE TABLE IF NOT EXISTS " + table.getName() + "(");


                /*for (Entry<String, Column> cl : table.getColumns().entrySet()) {
                    Column column = cl.getValue();
                    String key = cl.getKey();
                    mainSb.append(key)
                            .append(" ")
                            .append(column.getFieldType().name())
                            .append("(")
                            .append(column.getMaxSize())
                            .append(")")
                            .append(!column.canBeNull() ? " NOT NULL" : "")
                            .append(column.autoIncrements() ? " AUTO_INCREMENT" : "")
                            .append(", ");
                }
                boolean isEmpty = table.getPrimaryKey().isEmpty();
                if (isEmpty) {
                    mainSb.delete(mainSb.length() - 2, mainSb.length());
                }
                mainSb.append(!isEmpty ? "PRIMARY KEY(" + table.getPrimaryKey() + "));" : ");");*/
                PreparedStatement ps = this.getConnection().prepareStatement(mainSb.toString());
                ps.execute();
                ps.close();
            }
        }
    }

    private void generateCreationString(Class<?> table) throws InvalidClassException {
        if (!table.isAnnotationPresent(DatabaseTable.class)) {
            throw new InvalidClassException(
                    "The class " + table.getName() + " doesn't annotate from DatabaseTable. " +
                            "Read more and see how to fix this here: "
            );
        }
    }
}