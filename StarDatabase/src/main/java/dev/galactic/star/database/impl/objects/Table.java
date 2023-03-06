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

package dev.galactic.star.database.impl.objects;

import java.util.HashMap;

/**
 * The class that has all the metadata and information relating to the Database table.
 * @author PrismoidNW
 */
public class Table {
    private final HashMap<String, Column> columns = new HashMap<>();
    private String name;
    private String primaryKey = "";

    /**
     * The default constructor used to initialize the name.
     *
     * @param tableName Table name.
     */

    public Table(String tableName) {
        this.name = tableName;
    }


    /**
     * Returns a column by its name.
     *
     * @param columnName Name of the column.
     * @return Column instance.
     */
    public Column getColumnByName(String columnName) {
        Column column = this.columns.get(columnName);

        if (column == null) {
            throw new IllegalArgumentException("No table found with the columnName: \"" + columnName + "\"");
        } else {
            return column;
        }
    }

    /**
     * The default constructor used to initialize the name and primary key.
     *
     * @param tableName  Table name.
     * @param primaryKey Primary key.
     */
    public Table(String tableName, String primaryKey) {
        this(tableName);
        this.primaryKey = primaryKey;
    }

    /**
     * Takes a var args of the Column class.
     *
     * @param columnToAdd are the columns you want to add to the table.
     * @return Table so that it can be used for method chaining.
     */
    public Table addColumn(Column... columnToAdd) {
        for (Column column : columnToAdd) {
            this.columns.put(column.getName(), column);
        }
        return this;
    }

    /**
     * Gets a list of all the columns in the current table.
     *
     * @return A hashmap of the name with the object of the Column class.
     */
    public HashMap<String, Column> getColumns() {
        return columns;
    }

    /**
     * Gets the name of the table.
     *
     * @return a String of the name of the table.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the primary key of the table.
     *
     * @return The primary key. By default, the primary key is a blank String unless set so it returns "" if not set.
     * Else returns the primary key.
     */

    public String getPrimaryKey() {
        return primaryKey;
    }

    /**
     * Sets the primary key and updates it.
     *
     * @param primaryKey of the table.
     * @return Instance of the class for method chaining.
     */
    public Table setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    /**
     * Set the table's name to something different.
     * @param name New table name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns a human-readable String for debugging purposes. It prints the table, its values, and its columns.
     *
     * @return String of the table contents.
     */
    @Override
    public String toString() {
        StringBuilder columnBuilder = new StringBuilder();
        columns.forEach((i, j) -> columnBuilder.append(i)
                .append(":")
                .append(j.toString()));
        return "Table{" +
                "columns=" + columnBuilder +
                ", name='" + name + '\'' +
                ", primaryKey='" + primaryKey + '\'' +
                '}';
    }
}
