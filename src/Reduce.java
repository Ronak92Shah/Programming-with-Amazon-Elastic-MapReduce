import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class Reduce extends Reducer<Text, Text, Text, Text> 
{
    
    public void reduce(Text txt, Iterable<Text> val, Context contxt)
    throws IOException, InterruptedException
    {
    	
    	int count = 0; //to count how many times word appears in the file
    	int total = 0;
    	boolean bolChk=false;

    	String file="";//to store filename we counting
    
    	String txtStr="{"; // includes output
    	
    	 for (Text value : val) 
    	 {

    	 if(!bolChk)
    	 {

    		 file=value.toString();

    		 bolChk=true;

    	 }

    	 if (file.equals(value.toString())){ // chk if similar

    		 count=count+1; //increase the count 
    		 total = total + 1;

    	 }
    	 else
    	 {

    		 txtStr+=file + "="+count +", "; // store in string
    		 
    	file=value.toString(); // as value is object so to convert in string.
    	
    	count=1;
    	total = total + 1;
    	 } 
    	 }
    	 txtStr+= file + "="+count +"} \n"; //output pattern
        contxt.write(txt, new Text(txtStr));	
        String tot = "" + total; // convert to string from int
        //as method write accepts (Text,Text) displaying it and converting to Text
        contxt.write(new Text ("Total occurrence of '" + txt + "':"), new Text (tot));
    }
}
