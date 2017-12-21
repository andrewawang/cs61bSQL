package db61b;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TableTest {

    @Test
    public void testTable() {
        String[] test = {"a", "b", "c"};
        Table table = new Table(test);

        assertEquals(3, table.columns());
        assertEquals("a", table.getTitle(0));
        assertEquals("b", table.getTitle(1));
        assertEquals("c", table.getTitle(2));

        assertEquals(0, table.findColumn("a"));
        assertEquals(1, table.findColumn("b"));
        assertEquals(2, table.findColumn("c"));

        assertEquals(0, table.size());
        String[] values = {"aa", "fe", "3"};
        table.add(values);

        assertEquals(1, table.size());
        assertEquals("fe", table.get(0, 1));
        assertEquals("aa", table.get(0, 0));

        table.add(new String[] {"fee", "s2", "4"});
        assertEquals(2, table.size());
        assertEquals("4", table.get(1, 2));
        assertEquals("s2", table.get(1, 1));

        table.writeTable("table");
        Table table3 = Table.readTable("table");
        assertEquals(table.get(0, 1), table3.get(0,  1));

    }

    String[] test2 = {"1", "2", "3"};
    Table table2 = new Table(test2);

    String[] test1 = {"1", "2", "3", "4"};
    Table table1 = new Table(test1);

    @Test
    public void testColumns() {
        assertEquals(3, table2.columns());
        assertEquals(4, table1.columns());
    }
    @Test
    public void testGetTitle() {
        assertEquals("1", table2.getTitle(0));
        assertEquals("2", table2.getTitle(1));
        assertEquals("3", table2.getTitle(2));

        assertEquals("1", table1.getTitle(0));
        assertEquals("2", table1.getTitle(1));
        assertEquals("3", table1.getTitle(2));
        assertEquals("4", table1.getTitle(3));
    }
    @Test
    public void testFindColumn() {
        assertEquals(0, table2.findColumn("1"));
        assertEquals(1, table2.findColumn("2"));
        assertEquals(2, table2.findColumn("3"));
    }
    @Test
    public void testSize() {
        assertEquals(0, table2.size());
    }
    @Test
    public void testAdd() {
        String[] values = {"ab", "fee", "3"};
        table2.add(values);
        assertEquals(1, table2.size());
        assertEquals("fee", table2.get(0, 1));
        assertEquals("ab", table2.get(0, 0));

        table2.add(new String[] {"fe", "s22", "4"});
        assertEquals(2, table2.size());
        assertEquals("4", table2.get(1, 2));
        assertEquals("s22", table2.get(1, 1));
    }
}
