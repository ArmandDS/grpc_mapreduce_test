package com.grpctest.mapreduce.driver;

import com.grpctest.mapreduce.worker.MapReduceWorker;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

public class MapReduceServer {

    private static final Logger logger = Logger.getLogger(MapReduceWorker.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException {

        String fileDir = null;
        int N_task = 4;
        int M_task = 2;

        ArgumentParser parser = ArgumentParsers.newArgumentParser("MapReduceServer")
                .defaultHelp(true)
                .description("gRPC driver for Map-Reduce task");

        // Required arguments
        parser.addArgument("--N")
                .help("Number of map task")
                .setDefault(4)
                .type(Integer.class);

        parser.addArgument("--M")
                .help("Number of reduce task")
                .setDefault(2)
                .type(Integer.class);

        parser.addArgument("--files")
                .help("Folder where the files for map processing")
                .setDefault("/inputs")
                .type(String.class);

        try {
            Namespace ns = parser.parseArgs(args);

            N_task = (int) ns.get("N");
            M_task = (int) ns.get("M");
            fileDir = (String) ns.get("files");

            Path currentRelativePath = Paths.get("");
            String s = currentRelativePath.toAbsolutePath().toString();
            fileDir = s + fileDir;
            Path path = currentRelativePath.resolve(fileDir);
            if (!Files.exists(path) ||  !make_directory(currentRelativePath, s+"/intermediate") || !make_directory(currentRelativePath, s+"/out")){
               throw new IOException();
           }


        } catch (ArgumentParserException ex) {
            logger.severe("error");
            ex.getParser().handleError(ex);
        }catch (IOException ex){
            logger.severe("Cannot found the default files directory, please specify inputs directory --files");
        }



         logger.info("Starting gRPC Server on localhost port 50051");

        TaskServiceImpl tsi = new TaskServiceImpl(fileDir, N_task, M_task);
        Server server= ServerBuilder.forPort(50051)
                .addService(tsi)
                .build();
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
             logger.warning("Receive ShutDown Request");
            server.shutdown();
            logger.info("Successfully stopped the server");
        }));
        server.awaitTermination();

    }

    private static boolean make_directory(Path pathname, String dirName){
        Path pathIntermediate = pathname.resolve(dirName);

        File newDir = new File(String.valueOf(pathIntermediate));
        System.out.println(String.valueOf(pathIntermediate));
        if (!newDir.exists()){
            newDir.mkdirs();
            pathIntermediate = pathname.resolve(dirName);
        }

        return Files.exists(pathIntermediate);
    }



}
