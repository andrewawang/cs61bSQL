/*
// This is a SUGGESTED skeleton for a class that parses and executes database
// statements.  Be sure to read the STRATEGY section, and ask us if you have any
// questions about it.  You can throw this away if you want, but it is a good
// idea to try to understand it first.  Our solution adds or changes about 50
// lines in this skeleton.

// Comments that start with "//" are intended to be removed from your
// solutions.
*/
package db61b;


import java.io.PrintStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import static db61b.Utils.*;

/** An object that reads and interprets a sequence of commands from an
 *  input source.
 *  @author andrew*/
class CommandInterpreter {

    /* STRATEGY.
     *
     *   This interpreter parses commands using a technique called
     * "recursive descent." The idea is simple: we convert the BNF grammar,
     * as given in the specification document, into a program.
     *
     * First, we break up the input into "tokens": strings that correspond
     * to the "base case" symbols used in the BNF grammar.  These are
     * keywords, such as "select" or "create"; punctuation and relation
     * symbols such as ";", ",", ">="; and other names (of columns or tables).
     * All whitespace and comments get discarded in this process, so that the
     * rest of the program can deal just with things mentioned in the BNF.
     * The class Tokenizer performs this breaking-up task, known as
     * "tokenizing" or "lexical analysis."
     *
     * The rest of the parser consists of a set of functions that call each
     * other (possibly recursively, although that isn't needed for this
     * particular grammar) to operate on the sequence of tokens, one function
     * for each BNF rule. Consider a rule such as
     *
     *    <create statement> ::= create table <table name> <table definition> ;
     *
     * We can treat this as a definition for a function named (say)
     * createStatement.  The purpose of this function is to consume the
     * tokens for one create statement from the remaining token sequence,
     * to perform the required actions, and to return the resulting value,
     * if any (a create statement has no value, just side-effects, but a
     * select clause is supposed to produce a table, according to the spec.)
     *
     * The body of createStatement is dictated by the right-hand side of the
     * rule.  For each token (like create), we check that the next item in
     * the token stream is "create" (and report an error otherwise), and then
     * advance to the next token.  For a metavariable, like <table definition>,
     * we consume the tokens for <table definition>, and do whatever is
     * appropriate with the resulting value.  We do so by calling the
     * tableDefinition function, which is constructed (as is createStatement)
     * to do exactly this.
     *
     * Thus, the body of createStatement would look like this (_input is
     * the sequence of tokens):
     *
     *    _input.next("create");
     *    _input.next("table");
     *    String name = name();
     *    Table table = tableDefinition();
     *    _input.next(";");
     *
     * plus other code that operates on name and table to perform the function
     * of the create statement.  The .next method of Tokenizer is set up to
     * throw an exception (DBException) if the next token does not match its
     * argument.  Thus, any syntax error will cause an exception, which your
     * program can catch to do error reporting.
     *
     * This leaves the issue of what to do with rules that have alternatives
     * (the "|" symbol in the BNF grammar).  Fortunately, our grammar has
     * been written with this problem in mind.  When there are multiple
     * alternatives, you can always tell which to pick based on the next
     * unconsumed token.  For example, <table definition> has two alternative
     * right-hand sides, one of which starts with "(", and one with "as".
     * So all you have to do is test:
     *
     *     if (_input.nextIs("(")) {
     *         _input.next("(");
     *         // code to process "<column name>,  )"
     *     } else {
     *         // code to process "as <select clause>"
     *     }
     *
     * As a convenience, you can also write this as
     *
     *     if (_input.nextIf("(")) {
     *         // code to process "<column name>,  )"
     *     } else {
     *         // code to process "as <select clause>"
     *     }
     *
     * combining the calls to .nextIs and .next.
     *
     * You can handle the list of <column name>s in the preceding in a number
     * of ways, but personally, I suggest a simple loop:
     *
     *     ... = columnName();
     *     while (_input.nextIs(",")) {
     *         _input.next(",");
     *         ... = columnName();
     *     }
     *
     * or if you prefer even greater concision:
     *
     *     ... = columnName();
     *     while (_input.nextIf(",")) {
     *         ... = columnName();
     *     }
     *
     * (You'll have to figure out what do with the names you accumulate, of
     * course).
     */


    /** A new CommandInterpreter executing commands read from INP, writing
     *  prompts on PROMPTER, if it is non-null. */
    CommandInterpreter(Scanner inp, PrintStream prompter) {
        _input = new Tokenizer(inp, prompter);
        _database = new Database();
    }

    /** Parse and execute one statement from the token stream.  Return true
     *  iff the command is something other than quit or exit. */
    boolean statement() {
        switch (_input.peek()) {
        case "create":
            createStatement();
            break;
        case "load":
            loadStatement();
            break;
        case "exit": case "quit":
            exitStatement();
            return false;
        case "*EOF*":
            return false;
        case "insert":
            insertStatement();
            break;
        case "print":
            printStatement();
            break;
        case "select":
            selectStatement();
            break;
        case "store":
            storeStatement();
            break;
        default:
            throw error("unrecognizable command");
        }
        return true;
    }

    /** Parse and execute a create statement from the token stream. */
    void createStatement() {
        _input.next("create");
        _input.next("table");
        String name = name();
        Table table = tableDefinition();

        _database.put(name, table);
        _input.next(";");

    }

    /** Parse and execute an exit or quit statement. Actually does nothing
     *  except check syntax, since statement() handles the actual exiting. */
    void exitStatement() {
        if (!_input.nextIf("quit")) {
            _input.next("exit");
        }
        _input.next(";");
    }

    /** Parse and execute an insert statement from the token stream. */
    void insertStatement() {
        _input.next("insert");
        _input.next("into");
        Table table = tableName();
        _input.next("values");
        int cols = table.columns();

        String[] values = new String[cols];

        int k;
        while (true) {


            _input.next("(");

            k = 0;


            values[k] = literal();

            k += 1;

            while (_input.nextIf(",")) {

                values[k] = literal();
                k++;

            }

            _input.next(")");
            table.add(values);

            values = new String[cols];

            if (!_input.nextIf(",")) {
                break;
            }

        }
        _input.next(";");
    }

    /** Parse and execute a load statement from the token stream. */
    void loadStatement() {

        _input.next("load");
        String name = name();

        Table table = Table.readTable(name);
        _database.put(name, table);
        System.out.println("Loaded " + name + ".db");


        _input.next(";");
    }

    /** Parse and execute a store statement from the token stream. */
    void storeStatement() {
        _input.next("store");
        String name = _input.peek();
        Table table = tableName();

        table.writeTable(name);

        System.out.printf("Stored %s.db%n", name);
        _input.next(";");
    }

    /** Parse and execute a print statement from the token stream. */
    void printStatement() {

        _input.next("print");

        String name = name();
        Table table = _database.get(name);
        if (table == null) {
            throw error("unknown table: %s", name);
        }
        _input.next(";");
        System.out.print("Contents of ");
        System.out.println(name + ":");
        table.print();
    }

    /** Parse and execute a select statement from the token stream. */
    void selectStatement() {
        Table table = selectClause();
        System.out.println("Search results:");
        table.print();
        _input.next(";");
    }

    /** Parse and execute a table definition, returning the specified
     *  table. */
    Table tableDefinition() {
        Table table;
        if (_input.nextIf("(")) {

            ArrayList<String> columnTitles = new ArrayList<>();
            columnTitles.add(name());

            while (_input.nextIf(",")) {
                columnTitles.add(columnName());
            }

            _input.next(")");

            table = new Table(columnTitles);
        } else {

            _input.next("as");

            table = selectClause();
        }
        return table;
    }

    /** Parse and execute a select clause from the token stream, returning the
     *  resulting table. */
    Table selectClause() {
        _input.next("select");
        ArrayList<String> columnNames = new ArrayList<>();
        columnNamesHelper(columnNames);

        _input.next("from");
        ArrayList<Table> tables = new ArrayList<>();
        tablesHelper(tables);

        ArrayList<String> whereColumn = new ArrayList<>();
        ArrayList<String> whereValue = new ArrayList<>();
        ArrayList<String> whereCondition = new ArrayList<>();
        HashMap<String, Boolean> isValueNotCol = new HashMap<>();
        if (_input.nextIf("where")) {
            whereHelper(whereColumn, whereValue, whereCondition, isValueNotCol);
        }

        Table returnTable = new Table(columnNames);

        if (tables.size() == 1) {
            ArrayList<Integer> columnIndexes = new ArrayList<>();
            Table currentTable = tables.get(0);
            for (String name : columnNames) {
                columnIndexes.add(currentTable.findColumn(name));
            }
            if (whereColumn.size() == 0) {
                noCondSelectHelper(currentTable, columnIndexes, returnTable);
            } else {
                singleTableSelectCondHelper(whereColumn,
                        currentTable,
                        whereValue,
                        isValueNotCol,
                        whereCondition,
                        returnTable,
                        columnIndexes);

            }
        } else {
            twoTableSelectHelper(columnNames,
                    tables,
                    whereColumn,
                    whereValue,
                    whereCondition,
                    isValueNotCol,
                    returnTable);
        }
        return returnTable;
    }

    /**Helper for two table selection.
     *
     * @param columnNames col names
     * @param tables tables
     * @param whereColumn where  column
     * @param whereValue where value
     * @param whereCondition conditions
     * @param isValueNotCol whether value or col
     * @param returnTable returning table
     */
    void twoTableSelectHelper(ArrayList<String> columnNames,
                              ArrayList<Table> tables,
                              ArrayList<String> whereColumn,
                              ArrayList<String> whereValue,
                              ArrayList<String> whereCondition,
                              HashMap<String, Boolean> isValueNotCol,
                              Table returnTable) {
        Table table1 = tables.get(0);
        Table table2 = tables.get(1);
        Table tempTable = tempTableHelper(table1, table2);

        ArrayList<Integer> columnIndexes = new ArrayList<>();

        for (String name : columnNames) {
            columnIndexes.add(tempTable.findColumn(name));
        }
        if (whereColumn.size() == 0) {
            noCondSelectHelper(tempTable, columnIndexes, returnTable);
        } else {
            singleTableSelectCondHelper(whereColumn,
                    tempTable,
                    whereValue,
                    isValueNotCol,
                    whereCondition,
                    returnTable,
                    columnIndexes);
        }

    }

    /**
     * inner join table helper.
     * @param table1 table 1 to be joined
     * @param table2 2nd table
     * @return
     */
    Table tempTableHelper(Table table1, Table table2) {
        Table tempTable;
        String[] table1Cols = table1.getColumnNames();
        String[] table2Cols = table2.getColumnNames();
        HashMap<String, Boolean> tempTableCols = new HashMap<>();
        int rowLength = 0;
        for (String col : table1Cols) {
            tempTableCols.put(col, true);
            rowLength++;
        }
        for (String tab2Col : table2Cols) {
            if (table1.findColumn(tab2Col) != -1) {
                tempTableCols.put(tab2Col, false);
            } else {
                tempTableCols.put(tab2Col, true);
                rowLength++;
            }
        }
        String[] tempColNames = new String[rowLength];
        int start = 0;
        for (int i = 0; i < table1Cols.length; i++) {
            tempColNames[i] = table1Cols[i];
            start = i;
        }
        start += 1;
        for (int i = 0; i < table2Cols.length; i++) {
            if (tempTableCols.get(table2Cols[i])) {
                tempColNames[start] = table2Cols[i];
                start++;
            }
        }
        tempTable = new Table(tempColNames);
        String[] row = new String[rowLength];

        addingHelper(table1,
                table2,
                table1Cols,
                tempTableCols,
                row,
                table2Cols,
                tempTable);

        return tempTable;
    }

    /**
     * adding helper for mult select.
     * @param table1 first table
     * @param table2 second table
     * @param table1Cols cols of tabl1
     * @param tempTableCols joined table's cols
     * @param row each row to be inserted
     * @param table2Cols tabl2's cols
     * @param tempTable returning temp table
     */
    void addingHelper(Table table1,
                      Table table2,
                      String[] table1Cols,
                      HashMap<String, Boolean> tempTableCols,
                      String[] row,
                      String[] table2Cols,
                      Table tempTable) {
        Boolean adding = true;
        for (int curRow1 = 0; curRow1 < table1.size(); curRow1++) {
            for (int curRow2 = 0; curRow2 < table2.size(); curRow2++) {
                for (String tab1Col : table1Cols) {
                    if (!tempTableCols.get(tab1Col)) {
                        if (!table1.get(curRow1, table1.findColumn(tab1Col))
                                .equals(table2.get(curRow2,
                                        table2.findColumn(tab1Col)))) {
                            adding = false;
                            break;
                        }
                    }
                }
                if (adding) {
                    int start = 0;
                    for (int i = 0; i < table1Cols.length; i++) {
                        row[i] = table1.get(curRow1, i);
                        start = i;
                    }
                    start++;
                    for (int i = 0; i < table2Cols.length; i++) {
                        if (tempTableCols.get(table2Cols[i])) {
                            row[start] = table2.get(curRow2, i);
                            start++;
                        }
                    }
                    tempTable.add(row);
                }
                row = new String[row.length];
                adding = true;
            }
        }
    }

    /**
     * scrapes col names.
     * @param columnNames col names
     */
    void columnNamesHelper(ArrayList columnNames) {
        columnNames.add(columnName());
        while (_input.nextIf(",")) {
            columnNames.add(name());
        }
    }

    /**
     * scrapes tables from selection.
     * @param tables table names
     */
    void tablesHelper(ArrayList tables) {
        tables.add(tableName());
        while (_input.nextIf(",")) {
            tables.add(tableName());
        }
    }

    /**
     * helper for where.
     * @param whereColumn col
     * @param whereValue value
     * @param whereCondition condition
     * @param isValueNotCol if value or col
     */
    void whereHelper(ArrayList whereColumn,
                     ArrayList whereValue,
                     ArrayList whereCondition,
                     HashMap isValueNotCol) {
        whereColumn.add(columnName());
        whereCondition.add(condition());
        _input.flush();
        if (_input.nextIf(mkPatn("'.*"))) {
            _input.rewind();
            String value = literal();
            whereValue.add(value);
            isValueNotCol.put(value, true);
        } else {
            _input.rewind();
            String col = name();
            whereValue.add(col);
            isValueNotCol.put(col, false);
        }
        while (_input.nextIf("and")) {
            whereColumn.add(columnName());
            whereCondition.add(condition());
            _input.flush();
            if (_input.nextIf(mkPatn("'.*"))) {
                _input.rewind();
                String value = literal();
                whereValue.add(value);
                isValueNotCol.put(value, true);
            } else {
                _input.rewind();
                String col = name();
                whereValue.add(col);
                isValueNotCol.put(col, false);
            }
        }
    }

    /**
     * helper for no conditions select.
     * @param currentTable table selecting from
     * @param columnIndexes columns selecting
     * @param returnTable table adding
     */
    void noCondSelectHelper(Table currentTable,
                            ArrayList columnIndexes,
                            Table returnTable) {
        String[] row = new String[columnIndexes.size()];
        for (int curRow = 0; curRow < currentTable.size(); curRow++) {
            for (int i = 0; i < row.length; i++) {
                row[i] = currentTable.get(curRow, (int) columnIndexes.get(i));
            }
            returnTable.add(row);
            row = new String[columnIndexes.size()];
        }
    }

    /**
     * helper for single table selects.
     * @param whereColumn col
     * @param currentTable table
     * @param whereValue value
     * @param isValueNotCol if value or col
     * @param whereCondition cond
     * @param returnTable adding table
     * @param columnIndexes indexes to add
     */
    void singleTableSelectCondHelper(ArrayList<String> whereColumn,
                                     Table currentTable,
                                     ArrayList<String> whereValue,
                                     HashMap<String, Boolean> isValueNotCol,
                                     ArrayList<String> whereCondition,
                                     Table returnTable,
                                     ArrayList<Integer> columnIndexes) {
        String[] row = new String[columnIndexes.size()];
        ArrayList<Integer> whereColumnIndexes = new ArrayList<>();
        for (String name : whereColumn) {
            whereColumnIndexes.add(currentTable.findColumn(name));
        }
        Boolean adding = true;
        String currWhereItem;
        for (int curRow = 0; curRow < currentTable.size(); curRow++) {
            for (int i = 0; i < whereColumnIndexes.size(); i++) {
                currWhereItem = currentTable.get(curRow,
                        whereColumnIndexes.get(i));
                if (isValueNotCol.get(whereValue.get(i))) {
                    if (!conditionTrue(currWhereItem,
                            whereValue.get(i),
                            whereCondition.get(i))) {
                        adding = false;
                        break;
                    }
                } else {
                    String colItem = currentTable.get(curRow,
                            currentTable.findColumn(whereValue.get(i)));
                    if (!conditionTrue(currWhereItem,
                            colItem,
                            whereCondition.get(i))) {
                        adding = false;
                        break;
                    }
                }
            }
            if (adding) {
                for (int i = 0; i < row.length; i++) {
                    row[i] = currentTable.get(curRow, columnIndexes.get(i));
                }
                returnTable.add(row);
                row = new String[columnIndexes.size()];
            }
            adding = true;
        }
    }

    /**
     * returns condition relation.
     * @param a string
     * @param b string
     * @param condition condition
     * @return bool if condition holds
     */
    public Boolean conditionTrue(String a, String b, String condition) {

        a = a.trim();
        b = b.trim();
        int comparison = a.compareTo(b);
        return ((comparison == 0
                && (condition.equals("=")
                || condition.equals(">=")
                || condition.equals("<=")))
                || (comparison > 0
                && (condition.equals(">")
                || condition.equals(">=")
                || condition.equals("!=")))
                || (comparison < 0
                && (condition.equals("<")
                || condition.equals("<=")
                || condition.equals("!="))));
    }

    /** Parse and return a valid name (identifier) from the token stream. */
    String name() {
        return _input.next(Tokenizer.IDENTIFIER);
    }

    /** Parse and return a condition. */
    String condition() {
        return _input.next(Tokenizer.RELATION);
    }

    /** Parse and return a valid column name from the token stream. Column
     *  names are simply names; we use a different method name to clarify
     *  the intent of the code. */
    String columnName() {
        return name();
    }

    /** Parse a valid table name from the token stream, and return the Table
     *  that it designates, which must be loaded. */
    Table tableName() {
        String name = name();
        Table table = _database.get(name);
        if (table == null) {
            throw error("unknown table: %s", name);
        }
        return table;
    }

    /** Parse a literal and return the string it represents (i.e., without
     *  single quotes). */
    String literal() {
        String lit = _input.next(Tokenizer.LITERAL);
        return lit.substring(1, lit.length() - 1).trim();
    }

    /** Parse and return a list of Conditions that apply to TABLES from the
     *  token stream.  This denotes the conjunction (`and') of zero
     *  or more Conditions. */
    ArrayList<Condition> conditionClause(Table... tables) {
        return null;
    }

    /** Parse and return a Condition that applies to TABLES from the
     *  token stream. */
    Condition condition(Table... tables) {
        return null;
    }

    /** Advance the input past the next semicolon. */
    void skipCommand() {
        while (true) {
            try {
                while (!_input.nextIf(";") && !_input.nextIf("*EOF*")) {
                    _input.next();
                }
                return;
            } catch (DBException excp) {
                /* No action */
            }
        }
    }

    /** The command input source. */
    private Tokenizer _input;
    /** Database containing all tables. */
    private Database _database;
}
