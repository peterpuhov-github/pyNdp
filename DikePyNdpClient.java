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

// import org.apache.commons.codec.binary.Hex; // For byte[] decoded = Hex.decodeHex("00A0BF");

public class DikePyNdpClient
{
    public static void main( String[] args )
    {
        //StartSocketServer();
        try {
        String dag = "{\"Name\":\"DAG Projection\",\"NodeArray\":[{\"Name\":\"InputNode\",\"Type\":\"_INPUT\",\"File\":\"/tpch-test-parquet/lineitem.parquet\"},{\"Name\":\"TpchQ14 Filter\",\"Type\":\"_FILTER\",\"FilterArray\":[{\"Expression\":\"IsNotNull\",\"Arg\":{\"ColumnReference\":\"l_shipdate\"}},{\"Expression\":\"GreaterThanOrEqual\",\"Left\":{\"ColumnReference\":\"l_shipdate\"},\"Right\":{\"Literal\":\"1995-09-01\"}},{\"Expression\":\"LessThan\",\"Left\":{\"ColumnReference\":\"l_shipdate\"},\"Right\":{\"Literal\":\"1995-10-01\"}}]},{\"Name\":\"TpchQ14 Project\",\"Type\":\"_PROJECTION\",\"ProjectionArray\":[\"l_partkey\",\"l_extendedprice\",\"l_discount\"]},{\"Name\":\"OutputNode\",\"Type\":\"_OUTPUT\",\"CompressionType\":\"ZSTD\",\"CompressionLevel\":\"2\"}]}";
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
        String cmdStr = jsonObj.getString("spark_worker_command");
        System.out.println(String.format("cmdStr = %s", cmdStr ));

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
                  Runtime.getRuntime().exec(command);
                } catch(IOException e) {
                  e.printStackTrace();
                }
            }
        });
        t.start();
    }

    public static void StartSocketServer(String cmdStr)
    {
        try {
          byte[] command = DatatypeConverter.parseHexBinary(cmdStr);
          ServerSocket serverSocket = new ServerSocket(21285, 1);
          System.out.println("Server is listening on port " + serverSocket.getLocalPort());

            while(true) {
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

            int num_python_includes = 0;
            output.writeInt(num_python_includes);
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
            output.writeInt(command.length);
            output.write(command);
            output.flush();

/* From PySpark serializers
class SpecialLengths(object):
    END_OF_DATA_SECTION = -1
    PYTHON_EXCEPTION_THROWN = -2
    TIMING_DATA = -3
    END_OF_STREAM = -4
    NULL = -5
*/
            int END_OF_STREAM = -4;
            output.writeInt(END_OF_STREAM);
            output.flush();
            } // while(true)

        } catch (Exception ex) {
            System.out.println("Error occurred: ");
            ex.printStackTrace();
        }
    }
}

// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikePyNdpClient