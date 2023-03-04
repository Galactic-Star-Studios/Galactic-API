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

package dev.galactic.star.database.impl.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The annotation to mark a column in a database.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface TableColumn {
    /**
     * The name of the column.
     *
     * @return Column name.
     */
    String name();

    /**
     * The max number of characters that is displayed when selected.
     *
     * @return Integer value.
     */
    int maxDisplayed();


    /**
     * Whether to automatically create the column on creation of the table.
     *
     * @return True or false.
     */
    boolean autoCreate() default true;

    /**
     * The primary key for each table to be unique.
     *
     * @return True or false..
     */
    boolean primaryKey() default false;

    /**
     * Whether it sh
     *
     * @return Foreign key.
     */
    String foreignKey() default "";

    /**
     * Whether it should auto increment on each insert or not. Only works on integer types.
     *
     * @return True or false.
     */
    boolean autoIncrement() default false;

    /**
     * Whether it can be null or not.
     *
     * @return True or false.
     */
    boolean notNull() default false;

    /**
     * Provides a default string for the column.
     *
     * @return Default value as String
     */
    String defaultString() default "";
}
