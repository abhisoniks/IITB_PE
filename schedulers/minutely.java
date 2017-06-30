
package schedulers;

import java.util.Timer;
import Data_puller.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class minutely {
    public static void main(String... x){
        Timer timer = new Timer();
        data_puller mTask = new data_puller();
        File f = new File("/home/abhisoni/NetBeansProjects/IITB_PE/src/files/archivalInfo_admin");
        //File f = new File("./src/files/archivalInfo_admin");
        int interval=0;
        try{
            BufferedReader br = new BufferedReader(new FileReader(f));
            String line,hostname,interface_name,key;
            int hostid=0,itemid=0;
            br.readLine();
            line = br.readLine();
            String[] str = line.split("\\s+");
            interval = Integer.parseInt(str[0]);
            System.out.println("interval==="+interval);
            str = null;
        }
        catch(IOException ex){
            System.out.println("Exception in reading fie");
            ex.printStackTrace();
        } 
        
        
        timer.scheduleAtFixedRate(mTask, 0, interval*60000); 
    }                
}
