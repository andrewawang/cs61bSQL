package db61b;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class DatabaseTest {

    @Test
    public void testDatabase() {
        Database database = new Database();
        assertEquals(null, database.get("table"));

        String[] test = {"a", "b", "c"};
        Table table = new Table(test);
        String[] values = {"aa", "fe", "3"};
        table.add(values);
        table.add(new String[] {"fee", "s2", "4"});
        database.put("table", table);

        assertEquals(table, database.get("table"));

    }
}
