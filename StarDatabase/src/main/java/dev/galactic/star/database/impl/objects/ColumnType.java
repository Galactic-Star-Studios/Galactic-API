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

import java.sql.Time;
import java.sql.Date;

/**
 * The enum with all the supported Column types the database can use.
 * @author PrismoidNW & Verox001
 */
public enum ColumnType {
    /**
     * Integer datatype of the database.
     */
    INT("INT", Integer.class, 11),
    /**
     * Float datatype of the database.
     */
    FLOAT("FLOAT", Float.class, 0),
    /**
     * Bit datatype of the database.
     */
    BIT("BIT", Short.class, 0),
    /**
     * Boolean datatype of the database.
     */
    BOOL("BOOL", Boolean.class, 0),
    /**
     * Date datatype of the database.
     */
    DATE("DATE", java.util.Date.class, 0),
    /**
     * Time datatype of the database.
     */
    TIME("TIME", Time.class, 0),
    /**
     * DateTime datatype of the database.
     */
    DATE_TIME("DATETIME", Date.class, 0),
    /**
     * Character datatype of the database.
     */
    CHAR("CHAR", Character.class, 1),
    /**
     * Varchar datatype of the database.
     */
    VARCHAR("VARCHAR", String.class, 255),
    /**
     * Text datatype of the database.
     */
    TEXT("TEXT", String.class, 65535),
    /**
     * Binary datatype of the database.
     */
    BINARY("BINARY", Byte.class, 0);

    /**
     * The datatype's name.
     */
    private final String name;
    private final Class<?> type;
    private final int defaultLength;

    /**
     * The constructor used to set the name of the datatype to be used in the queries.
     *
     * @param name String
     */
    ColumnType(String name, Class<?> type, int defaultLength) {
        this.name = name;
        this.type = type;
        this.defaultLength = defaultLength;
    }

    /**
     * Gets the datatype's name.
     *
     * @return String
     */
    public String getName() {
        return name;
    }

    public Class<?> getType() {
        return type;
    }

    public int getDefaultLength() {
        return defaultLength;
    }
}
