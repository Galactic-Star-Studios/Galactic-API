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

package dev.galactic.star.database.databases.mysql.data.database;

import dev.galactic.star.database.impl.annotations.Database;
import dev.galactic.star.database.impl.annotations.Table;

@Database(name = "test_db")
public class DbParentTest {
    @Table(table_name = "test_tbl1")
    TableTest1 tbl1 = new TableTest1();

    @Table(table_name = "test_tbl2")
    TableTest2 tbl2 = new TableTest2();

    @Table(table_name = "test_tbl3")
    TableTest3 tbl3 = new TableTest3();
}
