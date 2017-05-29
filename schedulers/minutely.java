
package schedulers;

import java.util.Timer;
import Data_puller.*;

public class minutely {
    public static void main(String... x){
        Timer timer = new Timer();
        data_puller mTask = new data_puller();
        timer.scheduleAtFixedRate(mTask, 0, 60000); 
    }                
}
