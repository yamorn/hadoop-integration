package base;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Created by louis on 14-11-9.
 */
@RunWith(JUnit4.class)
public class ClassTest {
    @Test
    public void test1(){
        Class clazz1=Boolean.class;
        Class clazz2=Boolean.class;
        assertTrue(clazz1 == clazz2);
    }
    @Test
    public void test2() {
        boolean result=List.class.isAssignableFrom(LinkedList.class);
        assertTrue(result);
    }
}
