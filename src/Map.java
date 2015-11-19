import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
	//private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
    private String pattern= "^[a-z][a-z0-9]*$";
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
    	//getting filenames
        InputSplit inputSplit = context.getInputSplit();
        String fileName = ((FileSplit) inputSplit).getPath().getName();
        
        // getting values 
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            String stringWord = word.toString().toLowerCase();
            
            //Matching
            if (stringWord.matches(pattern)){
            	//generating key-value pair
            	// instead of sending number we are sending the filename associated with the word.
                context.write(new Text(stringWord), new Text(fileName)); 
                //context.write(new Text(stringWord), one);
                }
            }
        }
    }
