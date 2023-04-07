package heapdb.query;

import heapdb.ITable;
import heapdb.Schema;
import heapdb.Table;
import heapdb.Tuple;

import java.util.ArrayList;

/**
 * A simple select query of the form:
 * select column, column . . . from table where condition
 *
 * @author Glenn
 *
 */

public class SelectQuery  {

    private Condition cond;
    private String[] colNames;	   // a value of null means return all columns of the table

    /**
     * A query that contains both a where condition and a projection of columns
     * @param colNames are the columns to return
     * @param cond is the where clause
     */
    public SelectQuery(String[] colNames, Condition cond) {
        this.colNames = colNames;
        this.cond = cond;
    }

    /**
     * A query that contains a where condition.  All columns
     * of the Tuples are returned.
     * @param cond is the where clause
     */
    public SelectQuery(Condition cond) {
        this(null, cond);
    }


    public static ITable naturalJoin(ITable table1, ITable table2) {
        Schema resultSchema = table1.getSchema().naturaljoin(table2.getSchema());
        ITable resultTable = new Table(resultSchema);
        ArrayList<String> joinColumns = getJoinColumns(table1.getSchema(),table2.getSchema());

        for(Tuple t1 : table1){
            for(Tuple t2 : table2){
                boolean match = true;
                for(String s : joinColumns){
                    if(!t1.get(s).equals(t2.get(s))){
                        match = false;
                    }
                }
                if(match){
                    Tuple t = Tuple.joinTuple(resultSchema, t1, t2);
                    resultTable.insert(t);
                }
            }
        }

        return resultTable;
    }

    private static ArrayList<String> getJoinColumns(Schema s1, Schema s2){
        ArrayList<String> joinColumns = new ArrayList<>();

        for(int i = 0; i < s1.size(); i++){
            for(int j = 0; j < s2.size(); j++){
                if(s1.getName(i).equals(s2.getName(j))){
                    joinColumns.add(s1.getName(i));
                }
            }
        }

        return joinColumns;
    }

    public ITable eval(ITable table) {
        Schema resultSchema;
        if(colNames == null){
            resultSchema = table.getSchema();
        }
        else{
            resultSchema = table.getSchema().project(colNames);
        }
        Table resultTable = new Table(resultSchema);

        for(Tuple t: table){
            if(cond.eval(t)){
                resultTable.insert(t.project(resultSchema));
            }
        }
        return resultTable;
    }

    @Override
    public String toString() {
        String proj_columns;
        if (colNames != null) {
            proj_columns = String.join(",", colNames);
        } else {
            proj_columns = "*";
        }
        return "select " + proj_columns + " where " + cond;
    }

}
