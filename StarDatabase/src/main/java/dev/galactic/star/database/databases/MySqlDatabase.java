
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
import dev.galactic.star.database.impl.exceptions.WrongParameterException;
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
 *
 * @author PrismoidNW
 */
public class MySqlDatabase extends StarDatabase {
    /**
     * The abstract method that is called after calling the type method.
     *
     * @param username     The username to the login of the database.
     * @param password     The password to the login of the database. Recommended to have a strong, long password.
     * @param host         The url, domain, or IP of the database itself.
     * @param tableName    The name that will identify the table it is to change.
     * @param port         The port that goes along with the host url when connecting.
     * @param extraQueries The extra queries one may want to add.
     * @return StarDatabase instance so connecting can be done in one line
     */
    @Override
    public StarDatabase connect(String username, String password, String host, String tableName, int port,
                                String extraQueries) {
        if (this.isDebug()) {
            System.out.println("Connecting...");
        }
        try {
            String connectQuery = "jdbc:mysql://" + host + ":" + port + "/" + tableName + (extraQueries.isEmpty() ?
                    "" : "?" + extraQueries);
            this.setConnection(DriverManager.getConnection(connectQuery, username, password));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if (this.isDebug()) {
            System.out.println("Connected!");
        }
        return this;
    }

    /**
     * Alters the tableName's name, tableName field, add or delete existing columns.
     *
     * @param tableName     nameOfTheTable
     * @param thingToChange The updated version of the tableName you want to update. You must get the instance of the
     *                      tableName and then change it to your desires.
     * @param objects       List of arguments depending on the thing to change.
     */
    @Override
    public void alterTable(String tableName, AlterTableType thingToChange, Object... objects) {
        if (this.isDebug()) {
            System.out.println("Attempting to alter table \"" + tableName + "\"");
        }

        if (objects.length == 0) {
            try {
                throw new WrongParameterException(
                        "Wrong amount of parameters for the \"alterTable\" method. Varargs length equals 0.");
            } catch (WrongParameterException e) {
                throw new RuntimeException(e);
            }
        }

        StringBuilder builder = new StringBuilder("ALTER TABLE " + tableName + " ");

        switch (thingToChange) {
            case ADD_COLUMN: {
                for (Object c : objects) {
                    Column column = (Column) c;
                    builder.append("ADD ")
                            .append(column.getColumnDefinition())
                            .append(",");
                }

                if (builder.charAt(builder.length() - 1) == ',') {
                    builder.deleteCharAt(builder.length() - 1);
                }
                builder.append(";");
                if (this.isDebug()) {
                    System.out.println(builder);
                }
                this.executeUpdate(builder.toString());
                break;
            }
            case DROP_COLUMN: {
                for (Object c : objects) {
                    String columnNames = (String) c;
                    builder.append("DROP COLUMN ")
                            .append(columnNames)
                            .append(";");
                    if (this.isDebug()) {
                        System.out.println("Query String: " + builder);
                    }
                    this.executeUpdate(builder.toString());
                    builder = new StringBuilder("ALTER TABLE " + tableName + " ");
                }
                break;
            }
            case MODIFY_COLUMN: {
                for (Object s : objects) {
                    String[] set = (String[]) s;
                    String definition = this.getTableByName(tableName)
                            .getColumnByName(set[0])
                            .getColumnDefinition()
                            .replace(set[0] + " ", "");
                    builder.append("CHANGE COLUMN ")
                            .append(set[0])
                            .append(" ")
                            .append(set[1])
                            .append(" ")
                            .append(set.length == 3 ? set[2] : definition)
                            .append(";");

                    if (this.isDebug()) {
                        System.out.println("Query String: " + builder);
                    }
                    this.executeUpdate(builder.toString());
                    builder = new StringBuilder("ALTER TABLE " + tableName + " ");
                }
                break;
            }
            case RENAME_TABLE: {
                builder.append("RENAME TO ")
                        .append(objects[0]);
                this.getTableByName(tableName).setName((String) objects[0]);
                if (this.isDebug()) {
                    System.out.println("Query String: " + builder);
                }
                this.executeUpdate(builder.toString());
                break;
            }
        }
        if (this.isDebug()) {
            System.out.println("Executed SQL Query");
        }
    }

    /**
     * The method that inserts multiple pieces of data into a multiple columns.
     *
     * @param tableName       Name of the table where the column belongs to.
     * @param columns         The list of columns that data is going to be inserted into.
     * @param objectsToInsert The list of objects or data to be inserted.
     * @return Instance of StarDatabase so that it can be chained.
     */
    @Override
    public StarDatabase insert(String tableName, String[] columns, Object[] objectsToInsert) {
        if (columns.length != objectsToInsert.length) {
            try {
                throw new WrongParameterException("The size of the columns needs to be the size of objects");
            } catch (WrongParameterException e) {
                throw new RuntimeException(e);
            }
        }
        if (this.isDebug()) {
            System.out.println("Attempting to insert into the table \"" + tableName + "\"");
        }
        String tables = Arrays.toString(columns).replace("[", "").replace("]", "");
        String values = Arrays.toString(objectsToInsert).replace("[", "'").replace(", ", "', '").replace("]", "'");
        String query = "INSERT INTO " + tableName + "(" + tables + ") VALUES (" + values + ");";
        if (this.isDebug()) {
            System.out.println("Query String: " + query);
        }
        this.executeUpdate(query);
        if (this.isDebug()) {
            System.out.println("Done inserting into the table \"" + tableName + "\"");
        }
        return this;
    }

    /**
     * Used just so code can have less duplication.
     *
     * @param query SQL query String.
     */
    private void executeUpdate(String query) {
        try (PreparedStatement statement = this.getConnection().prepareStatement(query)) {
            statement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The inherited method that is called after connecting to create a database.
     *
     * @param tables the list of tables to create.
     * @throws SQLException if you don't have any tables to create.
     */
    @Override
    public void createTables(Table... tables) throws SQLException {
        if (this.isDebug()) {
            System.out.println("...");
        }
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
                this.getTables().add(table);
            }
            if (this.isDebug()) {
                System.out.println("Created tables.");
            }
        }
    }

    /**
     * The inherited method that is called after connecting to create a database.
     *
     * @param tables the list of classes, annotated by the DatabaseTable annotation.
     * @throws SQLException if you don't have any tables to create.
     */
    @Override
    public void createTables(Class<?>... tables) throws SQLException, InvalidClassException {
        if (tables.length == 0) {
            throw new SQLException("You can't have a varargs with nothing in it.");
        } else {
            for (Class<?> table : tables) {
                StringBuilder query = generateCreationString(table);

                boolean isEmpty = table.getAnnotation(DatabaseTable.class).primaryKeyField().equals("");
                if (isEmpty) query.delete(query.length() - 2, query.length());
                query.append(!isEmpty ?
                        "PRIMARY KEY(" + table.getAnnotation(DatabaseTable.class).primaryKeyField() + "));" : ");");
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

        String tableName = table.getName();
        if (!table.getAnnotation(DatabaseTable.class).tableName().equals("")) {
            tableName = table.getAnnotation(DatabaseTable.class).tableName();
        }
        StringBuilder query = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + "(");
        for (Field column : table.getFields()) {
            if (column.isAnnotationPresent(DatabaseField.class)) {
                String name = column.getAnnotation(DatabaseField.class).name();
                if (name.equals("")) name = column.getName().toLowerCase();
                query.append(name)
                        .append(" ")
                        .append(column.getAnnotation(DatabaseField.class).fieldType().getName())
                        .append("(")
                        .append(column.getAnnotation(DatabaseField.class).maxSize())
                        .append(")")
                        .append(!column.getAnnotation(DatabaseField.class).canBeNull() ? " NOT NULL" : "")
                        .append(column.getAnnotation(DatabaseField.class).autoIncrements() ? " AUTO_INCREMENT" : "")
                        .append(", ");
            }
        }
        return query;
    }
}
