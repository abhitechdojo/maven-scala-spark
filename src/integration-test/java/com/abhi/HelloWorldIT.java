package com.abhi;

import junit.framework.*;
import static org.junit.Assert.assertEquals;

public class HelloWorldIT {

    @Test
    public void test() {
        // make a rest call to webhdfs to get list of files
        val url = System.getProperty("webhdfscmd");
        assertEquals(url, "http://192.168.99.101:50070/webhdfs/v1/output?op=LISTSTATUS");
        // look for success flag
    }
}