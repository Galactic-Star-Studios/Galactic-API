package dev.galactic.star.database;

import dev.galactic.star.database.databases.MySqlDatabase;
import dev.galactic.star.database.impl.mapping.annotations.DatabaseField;
import dev.galactic.star.database.impl.mapping.annotations.DatabaseTable;
import dev.galactic.star.database.impl.objects.ColumnType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InvalidClassException;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DatabaseTable(primaryKeyField = "someRandomString")
class TestTable {
    @DatabaseField(maxSize = 100, fieldType = ColumnType.VARCHAR)
    private String someRandomString;
}


public class MySQLDatabaseTest {
    MySqlDatabase db;

    @Test
    @BeforeEach
    void createConnection() {
        MySqlDatabase db = new MySqlDatabase();
        // Expects Error, because username is incorrect.
        assertThrows(RuntimeException.class, () -> db.connect(
                "WrongUsername",
                "pywimC5itJotyoot;",
                "cc1.cimeyclust.com",
                "web12db_Tests",
                3306,
                ""
        ));

        // Create actual connection
        db.connect(
                "web12db_TAdmin",
                "pywimC5itJotyoot;",
                "cc1.cimeyclust.com",
                "web12db_Tests",
                3306,
                ""
        );
        assertNotNull(db.getConnection());
        this.db = db;
    }

    @Test
    void createTable() throws SQLException, InvalidClassException {
        this.db.createTables(TestTable.class);
    }
}
