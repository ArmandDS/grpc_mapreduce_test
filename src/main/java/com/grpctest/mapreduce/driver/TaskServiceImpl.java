package com.grpctest.mapreduce.driver;

import com.grpctest.mapreduce.worker.MapReduceWorker;
import com.proto.mapreduce.TaskAssigmentRequest;
import com.proto.mapreduce.TaskServiceGrpc;
import com.proto.mapreduce.taskResponse;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;


public class TaskServiceImpl extends TaskServiceGrpc.TaskServiceImplBase {

    private HashMap<String, Integer> fileList = new HashMap<String,Integer>();
    private HashMap<String, Integer> fileMapList = new HashMap<String, Integer>();
    private Integer N_task, M_task;
    private HashMap<String, String> FilesStatus = new HashMap<String, String>();
    private HashMap<String, String> FilesMapStatus = new HashMap<String, String>();
    private static final Logger logger = Logger.getLogger(MapReduceWorker.class.getName());

    public TaskServiceImpl(String patchname, Integer N_task, Integer M_task){
        init(patchname, N_task, M_task);
    }


    @Override
    public void taskresponse(TaskAssigmentRequest request, StreamObserver<taskResponse> responseObserver) {

        Integer taskresponseID = -1;
        String taskFilename = null;
        String taskName = null;
        Integer taskCommand = 0;
        Integer N_task = 0;
        TaskAssigmentRequest taskAssigmentRequest = request;
        String taskRequestFilename = taskAssigmentRequest.getTaskID();
        Integer taskRequestCommand = taskAssigmentRequest.getCommand();

        if(taskRequestCommand == 0){
            taskName = "MAP";
            taskCommand = 0;
            N_task = this.M_task;
        }else{
            taskName = "REDUCE";
            taskCommand = 1;
            N_task = this.M_task;
        }

        if (!taskRequestFilename.equals("START")) {
            setStatus(taskRequestFilename, "COMPLETED", taskName);
        }
        taskFilename = getStatus(taskName);
        taskresponseID = getTaskID(taskFilename, taskName);


        if(taskFilename == null){
            taskName = "REDUCE";
            taskCommand = 1;
            taskFilename = getStatus(taskName);
            N_task = this.M_task;

            if (taskFilename != null) {
                setStatus(taskFilename, "WORKING", taskName);
                taskresponseID = getTaskID(taskFilename, taskName);
                logger.info("Assigning a "+ taskName + " task with ID: " + taskresponseID);

            }else {
                taskFilename = "DONE";
                taskresponseID = -1;
            }

        } else {
            setStatus(taskFilename, "WORKING", taskName);
            logger.info("Assigning a "+ taskName + " task with ID: " + taskresponseID);
        }


        taskResponse response = taskResponse.newBuilder()
                .setTaskID(taskresponseID)
                .setCommand(taskCommand)
                .setFileName(taskFilename)
                .setNTask(N_task)
                .build();

        //The Response
        responseObserver.onNext(response);

        //Complete the call
        responseObserver.onCompleted();

        if( taskFilename.equals("DONE") && (isworking() == null)){
            logger.info("All done, stopping server");
            try {
                Runtime.getRuntime().halt(0);
                throw new InterruptedException();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

    public void init(String pathname, Integer N_task, Integer M_task){
        File f = new File(pathname);
        this.N_task = N_task;
        this.M_task = M_task;

        CicularList cll = new CicularList();
        for(int i= 0; i < N_task; i++){
            cll.addNode(i);
        }
        // Populates the array with names of files and directories
        for (int i = 0; i < f.list().length; i++) {
            this.fileList.put(f.list()[i],cll.next());
            this.FilesStatus.put(f.list()[i], "TOASSIGN");

        }

        if(f.list().length >0){
            for (int i = 0; i < this.M_task; i++) {
                this.fileMapList.put((Integer.toString(i)), i);
                this.FilesMapStatus.put((Integer.toString(i)), "TOASSIGN");
            }
        }

     logger.info("Server started with N: "+ this.N_task + " " + "and M: "+  this.M_task);

    }


    public String getStatus(String taskName) {

            HashMap<String, String> FStatus = new HashMap<String, String>();
            if(taskName.equals("MAP")){
                FStatus = this.FilesStatus;
            }else{
                FStatus = this.FilesMapStatus;
            }
            String status = FStatus.entrySet().stream()
                    .filter(e -> e.getValue().equals( "TOASSIGN"))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);
            return status;
    }

    public String isworking() {

        String status = this.FilesMapStatus.entrySet().stream()
                .filter(e -> e.getValue().equals( "WORKING"))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
        return status;
    }

    public  Long getCountCompleted(String taskName){

        HashMap<String, String> FStatus = new HashMap<String, String>();
        if(taskName.equals("MAP")){
            FStatus = this.FilesStatus;
        }else{
            FStatus = this.FilesMapStatus;
        }
        Long completed = FStatus.entrySet().stream()
                .filter(e -> e.getValue().equals( "COMPLETED"))
                .map(Map.Entry::getKey)
                .count();
        return completed;
    }

    public Integer getTaskID(String task, String taskName) {
        Integer taskID;
        if(taskName.equals("MAP")){
            taskID = this.fileList.get(task);
        }else{
            taskID = this.fileMapList.get(task);
        }
        return taskID;
    }

    public void setStatus(String elem , String status, String taskName){
        if(taskName.equals("MAP")){
            this.FilesStatus.put(elem, status);
        }else{
            this.FilesMapStatus.put(elem, status);
        }
    }

    public class ShutdownException extends Exception {
        public ShutdownException(String errorMessage) {
            super(errorMessage);
        }
    }

}



