package com.grpctest.mapreduce.driver;

import com.proto.mapreduce.TaskAssigmentRequest;
import com.proto.mapreduce.TaskServiceGrpc;
import com.proto.mapreduce.taskResponse;

import org.junit.Rule;
import org.junit.Test;
import io.grpc.testing.GrpcServerRule;

import static org.junit.Assert.*;

public class MapReduceServerTest {

    @Rule
    public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();

    @Test
    public void test_get_task() {
        // Add the service to the in-process server.
        grpcServerRule.getServiceRegistry().addService(new TaskServiceImpl("C:\\Users\\USUARIO\\Documents\\grpc\\files\\test_driver\\inputs_test", 8, 6));

        TaskServiceGrpc.TaskServiceBlockingStub blockingStub =
                TaskServiceGrpc.newBlockingStub(grpcServerRule.getChannel());

        taskResponse response = blockingStub.taskresponse(TaskAssigmentRequest.newBuilder()
                .setTaskID("START")
                .setCommand(0)
                .build());
        assertTrue(response.getNTask() >-1);
        assertTrue(response.getFileName()!= "DONE");
    }

    @Test
    public void test_get_reduce_task(){
        // Add the service to the in-process server.
        grpcServerRule.getServiceRegistry().addService(new TaskServiceImpl("C:\\Users\\USUARIO\\Documents\\grpc\\files\\test_driver\\inputs_test", 8, 6));

        TaskServiceGrpc.TaskServiceBlockingStub blockingStub =
                TaskServiceGrpc.newBlockingStub(grpcServerRule.getChannel());

        taskResponse response = blockingStub.taskresponse(TaskAssigmentRequest.newBuilder()
                .setTaskID("0")
                .setCommand(1)
                .build());
        assertEquals(response.getCommand(), 1);
        assertEquals(response.getNTask(), 6);


    }

}