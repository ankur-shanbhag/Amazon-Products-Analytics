/**
 * Created by vjain on 11/25/15.
 */

import java.io.*;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.hadoop.examples.Join;
import org.codehaus.jackson.*;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


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
                ObjectMapper mapper = new ObjectMapper();
                String line = "";
                boolean wroteMetaData = false;
                StringBuilder sb = new StringBuilder();
                String fileWrite = "";
                while ((line = bReader.readLine()) != null) {
                    JoinedData jsonItem = mapper.readValue(line,JoinedData.class);
                    sb.setLength(0);
                    if(!wroteMetaData){
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
