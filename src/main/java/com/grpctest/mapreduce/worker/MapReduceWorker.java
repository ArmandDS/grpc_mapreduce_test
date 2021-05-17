package com.grpctest.mapreduce.worker;

import com.proto.mapreduce.TaskAssigmentRequest;
import com.proto.mapreduce.TaskServiceGrpc;
import com.proto.mapreduce.taskResponse;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class MapReduceWorker {
    private static final Logger logger = Logger.getLogger(MapReduceWorker.class.getName());
    private static String dirInputs;

    private final TaskServiceGrpc.TaskServiceBlockingStub blockingStub;

    public MapReduceWorker(Channel channel) {
        blockingStub = TaskServiceGrpc.newBlockingStub(channel).withWaitForReady();

    }

    public static void main(String[] args) {
        String masterAddr;
        int masterPort;

        if (args.length<2) {
            logger.info("Not specifying  driver address and port, starting with default values");
            masterAddr = "localhost";
            masterPort = 50051;
        } else{
            masterAddr = args[0];
            masterPort = Integer.parseInt(args[1]);
        }
        ArgumentParser parser = ArgumentParsers.newArgumentParser("MapReduceWorker")
                .defaultHelp(true)
                .description("gRPC Worker for Map-Reduce task");

        // Required arguments
        parser.addArgument("--port")
                .help("Port on which to listen.")
                .setDefault(50051)
                .type(Integer.class);

        parser.addArgument("--add")
                .help("Ip address of the driver")
                .setDefault("localhost")
                .type(String.class);

        parser.addArgument("--files")
                .help("Folder where the files for map processing")
                .setDefault("/inputs")
                .type(String.class);

        try {
            Namespace ns = parser.parseArgs(args);
            masterAddr = (String) ns.get("add");
            masterPort = (int) ns.get("port");
            dirInputs = (String) ns.get("files");
            Path currentRelativePath = Paths.get("");
            String s = currentRelativePath.toAbsolutePath().toString();
            dirInputs = s + dirInputs;
            Path path = currentRelativePath.resolve(dirInputs);
            if (!Files.exists(path)){
                throw new IOException();
            }

        } catch (ArgumentParserException ex) {
            ex.getParser().handleError(ex);
        }catch (IOException ex){
            logger.severe("Cannot found the default files directory /inputs please specify inputs directory --files");
        }



        boolean flag = true;
        logger.info("Starting worker with driver on "+ masterAddr + " and port:" + masterPort);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(masterAddr, masterPort)
                .usePlaintext()
                .build();

        logger.info("Creating Stub");
        MapReduceWorker taskworker = new MapReduceWorker(channel);

        TaskAssigmentRequest taskresquest = TaskAssigmentRequest.newBuilder()
                .setTaskID("START")
                .setCommand(0)
                .build();

        taskResponse response =  taskworker.blockingStub.taskresponse(taskresquest);

        try{
            while(flag) {

                if(response.getFileName().equals("DONE")){
                    flag=false;
                    continue;
                }else if( response.getCommand() == 0){

                        logger.info("Received a Map Task with ID: "+  response.getTaskID());
                        doMap(response.getFileName(), response.getTaskID(), response.getNTask(), dirInputs);
                    }else {


                    logger.info("Received a Reduce Task with ID: "+  response.getTaskID());
                        doReduce(response.getFileName(), response.getNTask(), response.getTaskID(), dirInputs);
                    }

                 taskresquest = TaskAssigmentRequest.newBuilder()
                        .setTaskID(response.getFileName().toString())
                        .setCommand(response.getCommand())
                        .build();

                logger.info("Task: "+  response.getTaskID() +" done");

                response =  taskworker.blockingStub.taskresponse(taskresquest);




                waitBeforeNewTask(5000);

            }
            logger.info("Closing Channel");
            channel.shutdown();
        }catch (StatusRuntimeException ex){
            logger.info("Closing Channel");
            channel.shutdown();
        }
    }

    public static void doMap(String fileName, Integer taskID, Integer N_task, String dirInputs){

        try {
            long lineCount = Files.lines(Paths.get(dirInputs  + "\\"+ fileName)).count();
            int count = 0;
            lineCount = lineCount/N_task;
            if(lineCount < 1){
                lineCount =1;
            }
            int fileSplitNumber = 0;
            File myObj = new File(dirInputs + "\\"+fileName);
            Scanner myReader = new Scanner(myObj);
            String intermdiateDir = myObj.getParentFile().getParentFile() +"\\intermediate\\mr-";
            FileWriter fw = new FileWriter(intermdiateDir+ taskID+"-"+fileSplitNumber, true);
            BufferedWriter bw = new BufferedWriter(fw);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                data = cleanString(data);
                StringTokenizer st = new StringTokenizer(data);
                while (st.hasMoreTokens()) {
                    bw.write(st.nextToken() + " 1");
                    bw.newLine();
                }
                count ++;
                if (count == lineCount-1){
                    bw.close();
                    if(fileSplitNumber != (N_task-1)){
                        fileSplitNumber++;
                    }

                    fw = new FileWriter(intermdiateDir+ taskID+"-"+fileSplitNumber, true);
                    bw = new BufferedWriter(fw);
                    count= 0;
                }
            }
            myReader.close();
            bw.close();
        } catch (FileNotFoundException e) {
            logger.warning("An error occurred, file not found");
            e.printStackTrace();
        } catch (IOException e) {
            logger.warning("An error occurred.");
            e.printStackTrace();
        }
    }

    public static void doReduce(String fileName, Integer N_task, Integer taskID, String dirInputs){

        HashMap<String,Integer> wordCount = new HashMap<String, Integer>();
        try {
            String intermediateFiles = String.valueOf((new File(dirInputs)).getParentFile());
            File dir = new File( intermediateFiles + "\\intermediate");
            File[] files = dir.listFiles((d, name) -> name.endsWith( "-" + taskID));
            if(files.length >0) {
                FileWriter fw = new FileWriter(intermediateFiles+ "\\out\\" + taskID , true);
                BufferedWriter bw = new BufferedWriter(fw);
                for (File f : files) {
                    File myObj = new File(intermediateFiles + "\\intermediate\\" + f.getName());
                    long lineCount = Files.lines(Paths.get(intermediateFiles + "\\intermediate\\" + f.getName())).count();
                    Scanner myReader = new Scanner(myObj);

                    while (myReader.hasNextLine()) {
                        String data = myReader.nextLine();
                        String[] dataSplit = data.split("\\s+");
                        if (wordCount.containsKey(dataSplit[0])) {
                            wordCount.put(dataSplit[0], wordCount.get(dataSplit[0]) + 1);
                        } else {
                            wordCount.put(dataSplit[0], 1);
                        }

                    }
                    myReader.close();
                }
                for (HashMap.Entry<String, Integer> entry : wordCount.entrySet()) {
                    bw.write(entry.getKey() + " " + entry.getValue());
                    bw.newLine();
                }

                bw.close();
            }

        } catch (FileNotFoundException e) {
            logger.warning("An error occurred, file not found");
            e.printStackTrace();
        } catch (IOException e) {
            logger.warning("An error occurred.");
            e.printStackTrace();
        }
    }
     public static String cleanString(String text) {
         if (text != null) {
             text = text.replaceAll("\\s+", " ");
             text = text.replaceAll("[^a-zA-Z0-9_ ]", "");
             text = text.trim();
             text = text.toLowerCase();
             text = text.replaceAll("\\s+", " ");
         }
         return text;
     }

    private static void waitBeforeNewTask(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }




}
