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

package dev.galactic.star.database.impl.mapping.annotations;

import dev.galactic.star.database.impl.objects.ColumnType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares whether the field is a parameter.
 * @author Verox001
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DatabaseField {
    /**
     * the name of the database field.
     * @return String.
     */
    String name() default "";

    /**
     * Whether this field should be auto incremented on Insertion
     * @return Boolean
     */
    boolean autoIncrements() default false;

    /**
     * Whether values can be null.
     * @return Boolean.
     */
    boolean canBeNull() default false;

    /**
     * Determines the maximum length of the field.
     * Mostly used for complex datatypes like VARCHAR, TEXT, ...
     * @return int
     */
    int maxSize();

    /**
     * Determines the field type, the field should have in the database.
     * @return ColumnType
     */
    ColumnType fieldType();
}
