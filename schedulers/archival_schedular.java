
package schedulers;
import java.util.Timer;
import archival.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class archival_schedular{
    public static void main(String... x){
        long interval=0;
        Timer timer = new Timer();
        statistics_Generator mTask = new statistics_Generator();
        File f = new File("./src/files/archivalInfo_admin");
            try{
                BufferedReader br = new BufferedReader(new FileReader(f));
                String line,hostname,interface_name,key;
                int hostid=0,itemid=0;
                br.readLine();
                br.readLine();
                line = br.readLine();
                String[] str = line.split("\\s+");
                interval = Integer.parseInt(str[1]);
                str = null;
            }
            catch(IOException ex){
                System.out.println("Exception in reading fie");
            }
            interval = interval * 3600;
            timer.scheduleAtFixedRate(mTask,0,interval*1000);
            f=null;    
    }   
}
