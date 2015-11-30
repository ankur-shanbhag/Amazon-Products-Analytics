package edu.neu.mr.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;

import org.codehaus.jackson.map.ObjectMapper;


public class JsonToCSVWithMetadata {
    public static void main(String args[]) throws IOException{
        if(args.length < 1)
            System.err.println("Filename is not specified");
        else if(args.length < 2){
            System.err.println("Output directory is not specified");
        }
        else {
            File f = new File(args[0]);
            BufferedReader bReader = new BufferedReader(new FileReader(args[0]));
            BufferedWriter bWriterData = new BufferedWriter(new FileWriter(args[1] + "//" + f.getName() + ".txt"));
            try {
                // json object mapper reads json file and puts data in JoinedData object
                ObjectMapper mapper = new ObjectMapper();
                String line = "";
                boolean wroteMetaData = false; // this is to handle first line (column names)
                StringBuilder sb = new StringBuilder();
                String fileWrite = "";
                while ((line = bReader.readLine()) != null) {
                    JoinedData jsonItem = mapper.readValue(line,JoinedData.class);
                    sb.setLength(0);
                    if(!wroteMetaData){
                        // using reflection in order to get name of the fields to write as ehader in csv
                        for (Field field : jsonItem.getClass().getDeclaredFields()) {
                            field.setAccessible(true); // if you want to modify private fields
                            sb.append(field.getName()).append(",");
                        }
                        fileWrite = sb.toString().substring(0,sb.toString().lastIndexOf(","));
                        wroteMetaData = true;
                        bWriterData.write(fileWrite);
                        bWriterData.newLine();
                        sb.setLength(0);
                    }

                    // sets the value of the field for that item in the  string builder
                    for (Field field : jsonItem.getClass().getDeclaredFields()) {
                        field.setAccessible(true); // if you want to modify private fields
                        sb.append(field.get(jsonItem)).append(",");
                    }
                    fileWrite = sb.toString().substring(0,sb.toString().lastIndexOf(",")-1);
                    bWriterData.write(fileWrite);
                    bWriterData.newLine();
                }
            }

            catch(Exception ex){
                System.err.println(ex.fillInStackTrace());
                System.err.println(ex.getMessage());
            }

            finally {
                bReader.close();
                bWriterData.close();
            }
        }

    }
}
