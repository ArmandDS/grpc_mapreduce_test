# gRPC MapReduce Implementation 

MapReduce in Java for word count, make with java, gradle and intellij

## Introduction

MapReduce in Java, with very basic driver and worker implementation.


# Project Structure

This has the following structure: 

* the src folder, is the main folder with all the code, it contains 2 folder:
    - main folder: contains the package folders with:
        - driver:
            - MapReduceServer.java: the java class for the driver
            - TaskServiceImpl.java: the gRPC service implementation for the driver
            - CircularList.java.: a java class for implementing a circular list for the task assigning
       - worker:
            - MapReduceworker.java: the java class for the gRPC worker, implemented the Map and reduce methods
     - test folder: contains a simple test for the driver and the worker, Please make sure to update   the folder tests as appropriate.
* build.gradle: the file for the gladle project build
* settings.gradle: the file for gradle config
* results: the folder with the generated result data when run with N=8, N=6.

# Build
The project uses Maven/gradle to manage build.

Please make sure to update tests as appropriate.

# Usage
 On the folder __executables__ are the .jar files for the driver and the worker, it requires a folder __/inputs__ with the data on the same directory.

### run the Driver from terminal
 ```bash
java -jar MRDriver-1.0-SNAPSHOP.jar
```
it run with default values for directory or N task and M task, for help run: 
 ```bash
java -jar MRDriver-1.0-SNAPSHOP.jar --help
```
by default it run on localhost on port 50051


### run the Worker from terminall
 ```bash
java -jar MRWorker-1.0-SNAPSHOP.jar
```
it run with default values for directory files and master address and port , for help run: 
 ```bash
java -jar MRWorker-1.0-SNAPSHOP.jar --help
```
by default it run on localhost on port 50051

# My Aproach

### The Driver
Using gRPC protocol, the program uses the gRPC server as driver and the gRPC clients as workers, when start the server it looks for the files directory
it create the folder intermediate and out if doesn't exist
Read the files to be proccessed and make a task lists with the number of map task and number of reduce task and mark each task as "TOASSING".
Wait for worker to connect.
On Each worker coneection it assign all the map tasks, until all are marked as completed, then assigne the reduce tasks.
When all is completed it send a signal to the worker for exit, and exit itself.
Function:

* schedule task
* assign task
* Send exit 

### The worker:
The Worker on other hand received the task to be made, a map or a reduce task, when it done it send a message to the driver asking for new task and to let know to the driver that former that is completed, when there are not task let it exit.
Function:

* Receive task
* Read in data
* Process data(map or reduce or anything)
* Output result

# Some images of the driver and worker running
The Driver started: 
![Image](https://github.com/ArmandDS/grpc_mapreduce_test/blob/main/images/driver_started.PNG)

The Worker started: 
![Image](https://github.com/ArmandDS/grpc_mapreduce_test/blob/main/images/worker_started.PNG)

The Driver and worker running:
![Image](https://github.com/ArmandDS/grpc_mapreduce_test/blob/main/images/driver_worker.PNG)

The unit test for driver:
![Image](https://github.com/ArmandDS/grpc_mapreduce_test/blob/main/images/test_driver.PNG)

The unit test for worker:
![Image](https://github.com/ArmandDS/grpc_mapreduce_test/blob/main/images/test_worker.PNG)


# TODOs:
* make worker failure recovery
* make some more test

# Result
* local test
* run master and all workers on localhost
* The __result__ folder has the output files containing the frequency of each work in the input, __with N= 6 and M = 4__

# Testing
some basic test are under folder test, please be aware of the data directory
