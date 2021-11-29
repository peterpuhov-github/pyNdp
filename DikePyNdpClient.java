/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dike.hdfs;

import java.net.HttpURLConnection;
import java.net.*;
import java.io.*;
import java.nio.charset.StandardCharsets;

import javax.xml.bind.DatatypeConverter;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.python.util.PythonInterpreter;

// import org.apache.commons.codec.binary.Hex; // For byte[] decoded = Hex.decodeHex("00A0BF");

public class DikePyNdpClient
{
      public static String pickle_worker_command() {
            String pickled_command = "";
            try {
            String[] command = { "python3", "/data/dike/client/spark_driver.py" };
            String[] envp = {"PYTHONPATH=/data"};

            Process p = Runtime.getRuntime().exec(command, envp);
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

            pickled_command = stdInput.readLine();
            System.out.println(pickled_command);

            String line;
            while ((line = stdError.readLine()) != null) {
                System.out.println(line);
            }
            p.waitFor();
            int exitStatus = p.exitValue();
            System.out.println(String.format("Process exited with status %d", exitStatus));
            } catch(Exception ex) {
                  ex.printStackTrace();
            }
            return pickled_command;
      }

      public static void main( String[] args )
      {
        String command = pickle_worker_command();
        System.out.println(String.format("command = %s", command ));

        //StartSocketServer();
        try {
        String cmdStr = command;
        String dag = "{\"Name\":\"DAG Projection\",\"NodeArray\":[{\"Name\":\"InputNode\",\"Type\":\"_INPUT\",\"RowGroup\":\"0\",\"File\":\"/tpch-test-parquet-1g/lineitem.parquet/part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet\"},{\"Name\":\"TpchQ14 Filter\",\"Type\":\"_FILTER\",\"FilterArray\":[{\"Expression\":\"IsNotNull\",\"Arg\":{\"ColumnReference\":\"l_shipdate\"}},{\"Expression\":\"GreaterThanOrEqual\",\"Left\":{\"ColumnReference\":\"l_shipdate\"},\"Right\":{\"Literal\":\"1995-09-01\"}},{\"Expression\":\"LessThan\",\"Left\":{\"ColumnReference\":\"l_shipdate\"},\"Right\":{\"Literal\":\"1995-10-01\"}}]},{\"Name\":\"TpchQ14 Project\",\"Type\":\"_PROJECTION\",\"ProjectionArray\":[\"l_partkey\",\"l_extendedprice\",\"l_discount\"]},{\"Name\":\"OutputNode\",\"Type\":\"_OUTPUT\",\"CompressionType\":\"ZSTD\",\"CompressionLevel\":\"2\"}]}";

        if(false){
        // System.out.println(dag);
        String fname = "tpch-test-parquet-1g/lineitem.parquet/part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet";
        String user = "peter";
        String url = "http://172.18.0.1:9860/" + fname + "?op=GETFILESTATUS&user.name=" + user;
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestProperty("ReadParam", dag);
        connection.setRequestMethod("GET");
        BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        JsonReader jsonReader = Json.createReader(reader);
        JsonObject jsonObj = jsonReader.readObject();

        jsonReader.close();
        reader.close();

        System.out.println(String.format("row_group_count  = %d", jsonObj.getInt("row_group_count") ));
        cmdStr = jsonObj.getString("spark_worker_command");
        System.out.println(String.format("cmdStr = %s", cmdStr ));
        }

        Thread t = StartServer(cmdStr, dag);
        // t.join();
        StartWorker();
        t.join();

        } catch (Exception ex) {
            System.out.println("Error occurred: ");
            ex.printStackTrace();            
        }

    }

    public static void StartWorker()
    {
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    String[] command = { "python3", "./src/main/java/org/dike/hdfs/worker.py" };
                    String[] envp = { "PYTHON_WORKER_FACTORY_PORT=21285",
                                      "PYTHON_WORKER_FACTORY_SECRET=secret"};
                                       //"PYTHONPATH=/data"};

                    Process p = Runtime.getRuntime().exec(command, envp);
                    BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                    String line;
                    while ((line = stdInput.readLine()) != null) {
                        System.out.println(line);
                    }
                    while ((line = stdError.readLine()) != null) {
                        System.out.println(line);
                    }
                    p.waitFor();
                    int exitStatus = p.exitValue();
                    System.out.println(String.format("Process exited with status %d", exitStatus));
                } catch(Exception ex) {
                  ex.printStackTrace();
                }
            }
        });
        t.start();
    }

    public static Thread StartServer(String cmdStr, final String dag)
    {
        final String cmd = cmdStr;
        Thread t = new Thread(new Runnable() {
            public void run() {
                  StartSocketServer(cmd, dag);
            }
        });
        t.start();
        return t;
    }

    public static void StartSocketServer(String cmdStr, String dag)
    {
        try {
            byte[] command = DatatypeConverter.parseHexBinary(cmdStr);

            ServerSocket serverSocket = new ServerSocket(21285, 1);
            System.out.println("Server is listening on port " + serverSocket.getLocalPort());

            Socket socket = serverSocket.accept();
            System.out.println("New client connected");
            String secret = "secret";
            byte [] buffer  = new byte[1024];

            // InputStream input = socket.getInputStream();
            DataInputStream input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            int len = input.readInt();
            for(int i = 0; i < len; i++){
                buffer[i] = (byte)input.read();
                //System.out.println(String.format("Reading buffer[%d]  = %d", i, buffer[i]));
            }
            System.out.println(new String(buffer));

            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            String resp = "ok";
            output.writeInt(resp.length());
            output.write(resp.getBytes());
            output.flush();
            int split_index = 0;
            output.writeInt(split_index);
            output.flush();
            String pythonVersion = "3.8";
            output.writeInt(pythonVersion.length());
            output.write(pythonVersion.getBytes());
            output.flush();
            boolean isBarrier = false;
            output.writeBoolean(isBarrier);
            output.flush();
            int boundPort = 0; // Unused if isBarrier is False
            output.writeInt(boundPort);
            output.flush();
            output.writeInt(secret.length());
            output.write(secret.getBytes());
            output.flush();

            int taskContext_stageId = 0;
            int taskContext_partitionId = 0;
            int taskContext_attemptNumber = 0;
            long taskContext_taskAttemptId = 0;
            output.writeInt(taskContext_stageId);
            output.writeInt(taskContext_partitionId);
            output.writeInt(taskContext_attemptNumber);
            output.writeLong(taskContext_taskAttemptId);
            output.flush();

            int additional_resources = 0;
            output.writeInt(additional_resources);
            output.flush();

            int local_properties = 0;
            output.writeInt(local_properties);
            output.flush();

            String spark_files_dir = "/spark_files_dir";
            output.writeInt(spark_files_dir.length());
            output.write(spark_files_dir.getBytes());
            output.flush();

            int num_python_includes = 1;
            output.writeInt(num_python_includes);
            output.flush();

            String filename = "/data";
            output.writeInt(filename.length());
            output.write(filename.getBytes());
            output.flush();



            boolean needs_broadcast_decryption_server = false;
            output.writeBoolean(isBarrier);
            int num_broadcast_variables = 0;
            output.writeInt(num_broadcast_variables);
            output.flush();

            int eval_type = 0; // PythonEvalType.NON_UDF
            output.writeInt(eval_type);
            output.flush();

/*
            // int command_int[] = new int[] {128, 3, 40, 99, 95, 95, 109, 97, 105, 110, 95, 95, 10, 110, 100, 112, 95, 114, 101, 97, 100, 101, 114, 10, 113, 0, 78, 99, 95, 95, 109, 97, 105, 110, 95, 95, 10, 68, 101, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 10, 113, 1, 41, 129, 113, 2, 99, 95, 95, 109, 97, 105, 110, 95, 95, 10, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 10, 113, 3, 41, 129, 113, 4, 116, 113, 5, 46};
            int command_int[] = new int[] {128, 5, 149, 73, 0, 0, 0, 0, 0, 0, 0, 40, 140, 8, 95, 95, 109, 97, 105, 110, 95, 95, 148, 140, 10, 110, 100, 112, 95, 114, 101, 97, 100, 101, 114, 148, 147, 148, 78, 104, 0, 140, 12, 68, 101, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 148, 147, 148, 41, 129, 148, 104, 0, 140, 10, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 148, 147, 148, 41, 129, 148, 116, 148, 46};
            byte command [] = new byte[command_int.length];
            for(int i = 0; i < command.length; i++){
                command[i] = (byte) (command_int[i] & 0xFF);
            }
*/
            System.out.println("Sending  NON_UDF to spark worker");
            output.writeInt(command.length);
            output.write(command);
            output.flush();

            output.writeInt(dag.length());
            output.write(dag.getBytes());
            output.flush();

/* From PySpark serializers
class SpecialLengths(object):
    END_OF_DATA_SECTION = -1
    PYTHON_EXCEPTION_THROWN = -2
    TIMING_DATA = -3
    END_OF_STREAM = -4
    NULL = -5
*/
            System.out.println("Sending  END_OF_STREAM to spark worker");
            int END_OF_STREAM = -4;
            output.writeInt(END_OF_STREAM);
            output.flush();

            do {
            len = input.readInt();
            if(len == -2){
                System.out.println("PYTHON_EXCEPTION_THROWN");
                len = input.readInt();
                for(int i = 0; i < len; i++){
                    buffer[i] = (byte)input.read();
                }
                System.out.println(new String(buffer));

            } else if (len == -3) {
                System.out.println("TIMING_DATA");
            } else {
                System.out.println(String.format("Reading %d bytes", len));
                // We expect to receive header
                long nCols = input.readLong();
                System.out.println("nCols : " + String.valueOf(nCols));
                int dataTypes [] = new int [(int)nCols];
                for( int i = 0 ; i < nCols && i < 32; i++) {
                    dataTypes[i] = (int)input.readLong();
                    System.out.println(String.valueOf(i) + " : " + String.valueOf(dataTypes[i]));
                }

                ColumVector [] columVector = new ColumVector [(int)nCols];
                int record_count = 0;
                int traceRecordCount = 0;
                int traceRecordMax = 10;
                int totalRecords = 0;

                for( int i = 0 ; i < nCols; i++) {
                    columVector[i] = new ColumVector(i, dataTypes[i]);
                }
                //while(true)
                {
                    try {
                        for( int i = 0 ; i < nCols; i++) {
                            columVector[i].readColumnZSTD(input);
                        }

                        if(traceRecordCount < traceRecordMax) {
                            for(int idx = 0; idx < columVector[0].record_count && traceRecordCount < traceRecordMax; idx++){
                                String record = "";
                                for( int i = 0 ; i < nCols; i++) {
                                    record += columVector[i].getString(idx) + ",";
                                }
                                System.out.println(record);
                                traceRecordCount++;
                            }
                        }

                        totalRecords += columVector[0].record_count;
                    }catch (Exception ex) {
                        System.out.println(ex);
                        break;
                    }
                }

            }
            } while( len > 0);


        } catch (Exception ex) {
            System.out.println("Error occurred: ");
            ex.printStackTrace();
        }
    }
}

// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikePyNdpClient