package Data_puller;
import java.util.*;
import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import DateFieldAdder.*;
import sql.*;

public class data_puller extends TimerTask{
    ArrayList params = new ArrayList();
    static long next = 1495297500; // multiple of 60   
    int flag2 = 0; // If this flag is 1 then there is a chance that zabbix server and raw_data server is not in sync
    int flag3=0;   // 
    long zabbixMax = 0l;
    sqlutil2 su2 = new sqlutil2();
    String itemList;
    HashMap<Integer,Integer> hm = new HashMap<Integer,Integer>();
    Integer[] items;
    
    public data_puller(){
        long rawMax = getRawMaxClock();
        if(rawMax==0){
            next = 1495297500;
        }else{
            rawMax -= (rawMax % 60);
            next = rawMax + 60;
        }
        
        zabbixMax = getZabbixMaxClock();
        if(zabbixMax - rawMax >= 60) flag3=1;
        itemList = getItemList();
    }
        
    public String getItemList(){
        ArrayList<Integer> temp = new ArrayList<Integer>();
        String res = "(";
        addHost su  = new addHost();
        Connection con = null;
        try{
            String query = "select itemid,hostid from items";
            ArrayList al = new ArrayList();
            con = su.getcon();
            ResultSet rs = su.selectQuery(query, al, con);
            int item=0;int host=0;
            int count=0;
            while(rs.next()){
                count++;
                res += rs.getString("itemid")+",";
                item = Integer.parseInt(rs.getString("itemid"));
                temp.add(item);
                host = Integer.parseInt(rs.getString("hostid"));
                hm.put(item, host);
            }
            res +="-1)";
        }catch(Exception ex){
            System.out.println("Exception in getting connection with addHost database");
            ex.printStackTrace();
            
        }
        
        items = new Integer[temp.size()];
        items = temp.toArray(items);
        Arrays.sort(items);
        return res;
    }
    
    public void run() {
        sqlutil2 su2 = new sqlutil2();
        sqlutil su = new sqlutil();
        Connection con=null; Connection con2=null;
            try{
                con2 = su2.getcon();
            }catch(Exception ex){
                 System.out.println("something wrong with data puller connection with zabbix");
                 ex.printStackTrace();
                 flag2=1;
                 return ;
            }
            try{
                con = su.getcon();
            }catch(Exception ex){
                System.out.println("something wrong with data puller connection with local server");
                flag2=1;
                ex.printStackTrace();
                return;
            }
            if(flag3==1){
                System.out.println(flag3+" flag3 block "+zabbixMax);
                if(bringSync(zabbixMax,con2,con)){
                    flag2=0;
                    flag3=0;
                }    
                return;
            } 
            if(flag2==1){
                    System.out.println("In flag2 block");
                    long zabbixMax = getZabbixMaxClock();
                    if(zabbixMax - next > 60){
                        if(bringSync(zabbixMax,con2,con)){
                            flag2=0;
                            flag3=0;
                        };
                        return;
                    } 
            }
            pullData(con,con2);
            try{
                con.close();
                con2.close();
            }catch(Exception ex){
                ex.printStackTrace();
                System.out.println("Close connection");
            }      
    } 
    
    public long getRawMaxClock(){
        String query = "select max(clock) as maxi from raw_data ";
        long res=0l;
        sqlutil su2 = new sqlutil();
        Connection con=null;
        try{
            con = su2.getcon();
            ResultSet rs = su2.selectQuery(query, params, con);
            if(rs.next()){
                  res = Long.parseLong(rs.getString("maxi"));
            }
            
        }
        catch(java.lang.NumberFormatException ex){
            return 0;
        }
        
        catch(Exception ex){
            System.out.println("Exception in getting maxClock from raw_data");
            ex.printStackTrace();
            return 0;
        }finally{
            try{
                con.close();
            }catch(Exception ex){
                
            }
            
        }
        return res;
    }
    
    public long getZabbixMaxClock(){
        String query = "select max(clock) as maxi from (select clock from history_uint where clock > ?) as T ";
        params.add(next);
        long res=0l;
        Connection con2=null;
        try{
            con2 = su2.getcon();
            ResultSet rs = su2.selectQuery(query, params, con2);
            if(rs.next())
                res = Long.parseLong(rs.getString("maxi"));
        }catch(Exception ex){
            System.out.println("Exception in getting maxClock from zabbix");
            ex.printStackTrace();
            return 0;
        }finally{
            try{
                con2.close();
            }catch(Exception ex){
                
            }
        }
        params.clear();
        return res;    
    }
    
    public boolean bringSync(long zabbixMax,Connection con2,Connection con){
          if(con ==null){
              try{
                  con = new sqlutil().getcon();
              }catch(Exception ex){
                  System.out.println("connection fail bring sync");
                  return false;
              }
          }
          if(zabbixMax==0) zabbixMax = getZabbixMaxClock();
          while(true){
              while(zabbixMax-next >= 60){
                pullData(con,con2);
                System.out.println("while bringsync  "+next+" "+zabbixMax);
            }
            zabbixMax = getZabbixMaxClock();
            if(zabbixMax  - next <=60) break;
            System.out.println("still not in sync  "+next+" "+zabbixMax);
          }  
            System.out.println("fully sync "+next+" "+zabbixMax);
            return true;
    }
    
    public void pullData(Connection con,Connection con2){
        String query = "select * from history_uint where clock >= ? and clock < ? and itemid in "+itemList+" order by itemid";
        params.add(next);params.add(next+60);
        System.out.println("query==>>"+query);
        try{
            ResultSet rs = su2.selectQuery(query, params, con2);
            int i = 0;
            DateFieldAdder adder = new DateFieldAdder();
            long value = 0;long clock =0;int hostid=0;int itemid = 0;
            while(rs.next()){
                itemid = Integer.parseInt(rs.getString("itemid"));
                System.out.println(itemid+" "+items[i]);
                if(items[i]==itemid){
                 //   System.out.println("equal");
                    value = Long.parseLong(rs.getString("value"));
                    clock = Long.parseLong(rs.getString("clock"));
                    hostid = hm.get(itemid);
                    adder.Dateinsertor(hostid,itemid,clock,value,con);
                }else{
                    while(items[i]!=itemid){
                   //     System.out.println("Unequal");
                        value = 0;
                        hostid = hm.get(items[i]);
                        clock = next;
                        i++;
                        adder.Dateinsertor(hostid,items[i],clock,value,con);
                    }
                   // System.out.println("Again Equal "+itemid+" "+items[i]);
                    value = Long.parseLong(rs.getString("value"));
                    clock = Long.parseLong(rs.getString("clock"));
                    hostid = hm.get(itemid);
                    adder.Dateinsertor(hostid,itemid,clock,value,con);
                    
                }
                i++;
            }
            adder.al=null;
        }catch(Exception ex){
            System.out.println("Exception in pullData");
            ex.printStackTrace();
        }
        params.clear();
        next+=60;
    }
    
    
    
    
    /*
    public void pullData(Connection con,Connection con2){
        long value=0;
        
        File f = new File("./src/files/hosts_file");
        try{
            BufferedReader br = new BufferedReader(new FileReader(f));
            String line,hostname,interface_name,key;
            int hostid=0,itemid=0;
            br.readLine();
            String[] str = null;
            String tmp = "%";
            String tmp2 = "%traffic%";
            while((line = br.readLine()) != null){
                long clock=0;
                str = line.split("\\s+");
                hostname = tmp+str[0]+tmp;
                interface_name = tmp+str[1]+tmp;
                key  = tmp+str[2]+tmp2;
                ResultSet rs=null;
                Zabbix_Info zi  = new Zabbix_Info();
                hostid = zi.gethostid(hostname,con2);
                itemid = zi.getItemid(hostid,interface_name,key,con2);
                rs     = zi.getValueAndClock(itemid,next,con2);
                try{
                    if(rs.next()){
                       value = Long.parseLong(rs.getString("value"));
                       clock = Long.parseLong(rs.getString("clock"));
                       if(clock==0){
                           System.out.println("clock is zero" +clock +" "+ next);
                           clock = next;
                       }
                          // clock = next;
                    }else{
                        System.out.println("No data found for this clock");
                        value=0; clock =next;
                    }       
                }catch(Exception ex){
                    System.out.println("Exception in retrieving value and clock from resultset");
                }
                DateFieldAdder adder = new DateFieldAdder();
                adder.Dateinsertor(hostid,itemid,clock,value,con);
                str = null;
            } // file reading complete
             next = next + 60;
        }catch(IOException ex){
            System.out.println("Exception in reading file");
            ex.printStackTrace();
        }
       // System.gc();
    }   */
}
         