package com.grpctest.mapreduce.worker;

import com.grpctest.mapreduce.driver.TaskServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import io.grpc.testing.GrpcServerRule;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class MapReduceWorkerTest {

    @Rule
    public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();

    private MapReduceWorker worker;

    @Before
    public void setUp() throws Exception {
        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcServerRule.getServiceRegistry().addService(new TaskServiceImpl(
                "C:\\Users\\USUARIO\\Documents\\grpc\\files\\files_test_map\\inputs_test",8 ,6));


        // Create a client channel and register for automatic graceful shutdown.
        ManagedChannel channel = grpcServerRule.getChannel();

        // Create a HelloWorldClient using the in-process channel;
       worker = new MapReduceWorker(channel);
    }
    @Test
    public void test_map_task() throws FileNotFoundException {

        HashMap<String, Integer> interMediateFileResult = new HashMap<>();
        HashMap<String,Integer> interMediateFileExpected = new HashMap<String, Integer>();
        interMediateFileExpected.put("the", 1);
        interMediateFileExpected.put("project", 1);
        interMediateFileExpected.put("gutenberg", 1);
        interMediateFileExpected.put("ebook", 1);

        worker.doMap("t1.txt", 0, 6,"C:\\Users\\USUARIO\\Documents\\grpc\\files\\files_test_map\\inputs_test\\" );

        File dirFile = new File("C:\\Users\\USUARIO\\Documents\\grpc\\files\\files_test_map\\intermediate\\mr-0-0");
        File myObj = new File(String.valueOf(dirFile));
        Scanner myReader = new Scanner(myObj);
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            String[] dataSplit = data.split("\\s+");
            interMediateFileResult.put(dataSplit[0], Integer.parseInt(dataSplit[1]));

        }
        assertTrue(interMediateFileExpected.equals(interMediateFileResult));
    }

    @Test
    public void test_reduce_task() throws FileNotFoundException {

        HashMap<String, Integer> interMediateFileResult = new HashMap<>();
        HashMap<String,Integer> interMediateFileExpected = new HashMap<String, Integer>();
        interMediateFileExpected.put("play", 1);
        interMediateFileExpected.put("a", 1);
        interMediateFileExpected.put("importance", 1);
        interMediateFileExpected.put("one", 1);
        interMediateFileExpected.put("sound", 1);
        interMediateFileExpected.put("for", 3);
        interMediateFileExpected.put("project", 1);
        interMediateFileExpected.put("is", 3);
        interMediateFileExpected.put("being", 1);
        interMediateFileExpected.put("accurately", 1);
        interMediateFileExpected.put("luxuriously", 1);
        interMediateFileExpected.put("room", 1);
        interMediateFileExpected.put("the", 4);
        interMediateFileExpected.put("can", 1);
        interMediateFileExpected.put("that", 1);
        interMediateFileExpected.put("piano", 1);
        interMediateFileExpected.put("ebook", 3);
        interMediateFileExpected.put("of", 2);
        interMediateFileExpected.put("earnest", 1);
        interMediateFileExpected.put("gutenberg", 1);

        worker.doReduce("mr-0-0.txt", 6, 0,"C:\\Users\\USUARIO\\Documents\\grpc\\files\\files_test_reduce\\inputs_test" );

        File dir = new File( "C:\\Users\\USUARIO\\Documents\\grpc\\files\\files_test_reduce\\out");
        File[] files = dir.listFiles((d, name) -> name.endsWith(0+".txt"));
        File outputFile= new File("C:\\Users\\USUARIO\\Documents\\grpc\\files\\files_test_reduce\\out\\"+0+".txt");
        Scanner lineReader = new Scanner(outputFile);
        while (lineReader.hasNextLine()) {
            String data = lineReader.nextLine();
            String[] dataSplit = data.split("\\s+");
            interMediateFileResult.put(dataSplit[0], Integer.parseInt(dataSplit[1]));

        }
        assertEquals(files.length, 1);
        assertTrue(interMediateFileExpected.equals(interMediateFileResult));
    }

    @Test
    public void cleanString() {
        String result = worker.cleanString("\"this is #a* &VEry dirty& String\" ");
        assertEquals(result, "this is a very dirty string");
    }
}