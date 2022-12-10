
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

package dev.galactic.star.database.databases;

import dev.galactic.star.database.impl.StarDatabase;
import dev.galactic.star.database.impl.mapping.annotations.DatabaseField;
import dev.galactic.star.database.impl.mapping.annotations.DatabaseTable;
import dev.galactic.star.database.impl.objects.Column;
import dev.galactic.star.database.impl.objects.Table;

import java.io.InvalidClassException;
import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map.Entry;

/**
 * The class that implements MySQL's own syntax.
 * @author PrismoidNW
 */
public class MySqlDatabase extends StarDatabase {
    @Override
    public StarDatabase connect(String username, String password, String host, String database, int port,
                                String extraQueries) throws RuntimeException {
        try {
            String connectQuery = "jdbc:mysql://" + host + ":" + port + "/" + database + (extraQueries.isEmpty() ?
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
    public void createTables(Class<?>... tables) throws SQLException, InvalidClassException {
        if (tables.length == 0) {
            throw new SQLException("You can't have a varargs with nothing in it.");
        } else {
            for (Class<?> table : tables) {
                StringBuilder query = generateCreationString(table);

                boolean isEmpty = table.getAnnotation(DatabaseTable.class).primaryKeyField().equals("");
                if (isEmpty && table.getDeclaredFields().length > 0) query.delete(query.length() - 2, query.length());
                query.append(!isEmpty ? "PRIMARY KEY(" + table.getAnnotation(DatabaseTable.class).primaryKeyField() + "));" : ");");
                System.out.println(query);
                PreparedStatement ps = this.getConnection().prepareStatement(query.toString());
                ps.execute();
                ps.close();
            }
        }
    }

    private StringBuilder generateCreationString(Class<?> table) throws InvalidClassException {
        if (!table.isAnnotationPresent(DatabaseTable.class)) {
            throw new InvalidClassException(
                    "The class " + table.getName() + " doesn't annotate from DatabaseTable. " +
                            "Read more and see how to fix this here: "
            );
        }

        String tableName = table.getSimpleName();
        if (!table.getAnnotation(DatabaseTable.class).tableName().equals("")) {
            tableName = table.getAnnotation(DatabaseTable.class).tableName();
        }
        StringBuilder query = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");
        for (Field column : table.getDeclaredFields()) {
            if (column.isAnnotationPresent(DatabaseField.class)) {
                String name = column.getAnnotation(DatabaseField.class).name();
                if (name.equals("")) name = column.getName().toLowerCase();
                int maxSize = column.getAnnotation(DatabaseField.class).maxSize();
                if (maxSize > column.getAnnotation(DatabaseField.class).fieldType().getDefaultLength()) {
                    maxSize = column.getAnnotation(DatabaseField.class).fieldType().getDefaultLength();
                }
                query
                        .append(name)
                        .append(" ")
                        .append(column.getAnnotation(DatabaseField.class).fieldType().getName())
                        .append("(")
                        .append(maxSize)
                        .append(")")
                        .append(!column.getAnnotation(DatabaseField.class).canBeNull() ? " NOT NULL" : "")
                        .append(column.getAnnotation(DatabaseField.class).autoIncrements() ? " AUTO_INCREMENT" : "")
                        .append(", ");
            }
        }
        return query;
    }
}
