package heapdb;

import heapdb.query.*;
import heapdb.query.EqCondition;

/**
 * Driver file for Instructor and Table.
 */

public class Main {
    public static void main(String args[]){


        // Initialize Schema
        Schema schemaTest = new Schema();
        schemaTest.addKeyIntType("ID");
        schemaTest.addVarCharType("name");
        schemaTest.addVarCharType("dept_name");
        schemaTest.addIntType("salary");

        // initialize test table and add test instructors
        LSMmemory instTest = new LSMmemory("dummyfile.txt", schemaTest);
        Tuple one = new Tuple(schemaTest,12121, "Kim", "Elect. Engr.", 65000);
        Tuple two = new Tuple(schemaTest,19803, "Wisneski", "Comp. Sci.", 46000);
        Tuple three = new Tuple(schemaTest,24734, "Bruns", "Comp. Sci.", 70000);
        Tuple four = new Tuple(schemaTest,55552, "Scott", "Math", 80000);
        Tuple five = new Tuple(schemaTest,12321, "Tao", "Comp. Sci.", 95000);

        instTest.insert(one);
        instTest.insert(two);
        instTest.insert(three);
        instTest.insert(four);
        instTest.insert(five);

        // output table
        System.out.println(instTest.toString());

        // test delete functionality
        Boolean deleteStatus = instTest.delete(12121);
        System.out.println("delete 12121:" + deleteStatus);
        System.out.println();

        System.out.println(instTest.toString());

        deleteStatus = instTest.delete(12121);
        System.out.println("delete 12121:" + deleteStatus);
        System.out.println();

        // test lookup functionality
        Tuple lookupTest = instTest.lookup(19803);
        System.out.println("lookup 19803:" + lookupTest.toString());
        System.out.println();

        lookupTest = instTest.lookup(12345);
        if(lookupTest == null){
            System.out.println("lookup 12345:null");
        }else{
            System.out.println("lookup 12345:" + lookupTest.toString());
        }
        System.out.println();

        // quick map test
        System.out.println(instTest);


        // test eval functionality
        ITable compSciDept = instTest.lookup("dept_name", "Comp. Sci.");
        System.out.println("lookup dept_name='Comp. Sci.'");
        System.out.println(compSciDept.toString());

        ITable instr55552 = instTest.lookup("ID", 55552);
        System.out.println("lookup ID=55552");
        System.out.println(instr55552.toString());

        ITable emptyTable = instTest.lookup("ID", 12345);
        System.out.println("lookup ID=12345");
        System.out.println(emptyTable.toString());

        // QUERY TESTING
        System.out.println("====QUERY TESTING=====");

        // Simple Eq
        Condition testCond = new EqCondition("dept_name", "Comp. Sci.");
        SelectQuery q = new SelectQuery(testCond);
        ITable result = q.eval(instTest);
        System.out.println("query: dept. name = Comp. Sci.");
        System.out.println(result.toString());

        // And Condition
        testCond = new AndCondition(new EqCondition("salary", 80000), new EqCondition("name", "Brian"));
        q = new SelectQuery(testCond);
        result = q.eval(instTest);
        System.out.println("query: salary = 80000 AND name = Brian");
        System.out.println(result.toString());

        //comb Query
        testCond = new OrCondition(
                new AndCondition(new EqCondition("salary", 80000), new EqCondition("name", "Scott")),
                new EqCondition("name", "Brian"));
        q = new SelectQuery(testCond);
        result = q.eval(instTest);
        System.out.println("query: ( salary = 80000 AND name = Scott ) OR name = Brian");
        System.out.println(result.toString());

        // JOIN TESTING
        System.out.println("====JOIN TESTING=====");

        Schema schemaJoinTestDept = new Schema();
        schemaJoinTestDept.addVarCharType("dept_name");
        schemaJoinTestDept.addKeyIntType("budget");

        LSMmemory joinTestInstr = new LSMmemory("dummyfileTwo.txt", schemaJoinTestDept);
        Tuple depOne = new Tuple(schemaJoinTestDept,"Elect. Engr.", 650000);
        Tuple depTwo = new Tuple(schemaJoinTestDept,"Comp. Sci.", 460000);
        Tuple depThree = new Tuple(schemaJoinTestDept,"Math", 700000);

        joinTestInstr.insert(depOne);
        joinTestInstr.insert(depTwo);
        joinTestInstr.insert(depThree);

        // output inst table
        System.out.println("Instructor Table: ");
        System.out.println(instTest.toString());

        // output dept table
        System.out.println("Department Table: ");
        System.out.println(joinTestInstr.toString());

        // output joined table
        System.out.println("Joined Table: ");
        ITable joinTest = SelectQuery.naturalJoin(instTest, joinTestInstr);
        System.out.println(joinTest.toString());

    }
}
