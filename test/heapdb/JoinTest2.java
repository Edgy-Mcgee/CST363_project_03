package heapdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import heapdb.query.SelectQuery;

public class JoinTest2 {
    
    @Test
    public void testMultipleColumnJoin() {
        Schema s1 = new Schema();
        s1.addIntType("A");
        s1.addIntType("B");
        s1.addIntType("C");
        Schema s2 = new Schema();
        s2.addIntType("B");
        s2.addIntType("C");
        s2.addIntType("D");
        
        Table t1 = new Table(s1);
        Table t2 = new Table(s2);
        
        t1.insert(new Tuple(s1, 10, 20, 30));
        t1.insert(new Tuple(s1, 20, 25, 35));
        t1.insert(new Tuple(s1, 30, 35, 45));
        
        t2.insert(new Tuple(s2, 20, 30, 50));
        t2.insert(new Tuple(s2, 20, 30, 55));
        t2.insert(new Tuple(s2, 25, 35, 75));
        t2.insert(new Tuple(s2, 35, 40, 50));
        
        ITable t3 = SelectQuery.naturalJoin(t1,  t2);
        assertEquals(4, t3.getSchema().size());
        assertEquals(3, t3.size());
    }
    
    @Test
    public void noJoinColumns() {
        Schema s1 = new Schema();
        s1.addIntType("A");
        s1.addIntType("B");
        s1.addIntType("C");
        Schema s2 = new Schema();
        s2.addIntType("D");
        s2.addIntType("E");
        s2.addIntType("F");
        
        Table t1 = new Table(s1);
        Table t2 = new Table(s2);
        
        t1.insert(new Tuple(s1, 10, 20, 30));
        t1.insert(new Tuple(s1, 20, 25, 35));
        t1.insert(new Tuple(s1, 30, 35, 45));
        
        t2.insert(new Tuple(s2, 20, 30, 50));
        t2.insert(new Tuple(s2, 20, 30, 55));
        t2.insert(new Tuple(s2, 25, 35, 75));
        t2.insert(new Tuple(s2, 35, 40, 50));
        
        ITable t3 = SelectQuery.naturalJoin(t1,  t2);
        assertEquals(6, t3.getSchema().size());
        assertEquals(12, t3.size());
        
    }

}
