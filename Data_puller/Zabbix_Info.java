
package Data_puller;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import sql.*;

public class Zabbix_Info {
 
    public int gethostid(String hostname,Connection con2){      
         sqlutil2 su2 = new sqlutil2();
            int hostid=0;
            String query = "select hostid from hosts where host like ?";
            ArrayList params = new ArrayList();
            params.add(hostname);
            try{
          //         makeConnection(); 
                   ResultSet rs   = su2.selectQuery(query, params, con2);  
                  while(rs.next())
                        hostid = Integer.parseInt(rs.getString("hostid"));
                  rs = null;
                }catch(Exception ex){
                    System.out.println("Exception in retrieving Hostid");
                }
            query = null;
            params = null;
            return hostid;
    }
        
    public int getItemid(int hostid,String interface_name,String key,Connection con2){
         sqlutil2 su2 = new sqlutil2();
        //    intializecon();
            int itemid = 0;
            String query2 = "select itemid from items where hostid=? and name like ? and key_ like ?";
            ArrayList params = new ArrayList();    
            params.add(hostid);
            params.add(key);
            params.add(interface_name);
            try{        
                ResultSet rs = su2.selectQuery(query2, params,con2);
                while(rs.next())
                     itemid = Integer.parseInt(rs.getString("itemid"));
                rs=null;
            }catch(Exception ex){
                    System.out.println("Exception in retrieving itemid");
            }
            query2 = null;
            params = null;
            return itemid;
    }
        
    public ResultSet getValueAndClock(int itemid, long next,Connection con2){
         sqlutil2 su2 = new sqlutil2();
//   intializecon();
            ResultSet rs=null;
            String query4 = "select * from zabbix.history_uint where itemid = ? and  clock >= ? and clock < ? limit 1";
            ArrayList params = new ArrayList();
            params.add(itemid); params.add(next);
            long next2 = next+60;
            params.add(next2);
            try{
                rs = su2.selectQuery(query4, params,con2);    
            }catch(Exception ex){
                System.out.println("Exception in finding value of traffic and finding clock value");
            }
            query4 = null;
            params = null;
            return rs;
    }
}
