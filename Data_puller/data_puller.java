package Data_puller;
import java.util.*;
import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import DateFieldAdder.*;
import sql.*;

public class data_puller extends TimerTask{
    ArrayList params = new ArrayList();
    static long next = 1497239340; // multiple of 60   
    int flag2 = 0; // If this flag is 1 then there is a chance that zabbix server and raw_data server is not in sync
    int flag3=0;   // 
    int fullySync_Flag=0;
    long zabbixMax = 0l;
    sqlutil2 su2 = new sqlutil2();
    sqlutil su = new sqlutil();
    String itemList;
    HashMap<Integer,Integer> hm = new HashMap<Integer,Integer>();
    Integer[] items;
    Connection con;  // local connection
    Connection con2; // zabbix  connection
    int count=0;
    
    
    public void makelocalConnection(){
        try{
            con = su.getcon();
        }catch(Exception ex){
            System.out.println("something wrong with data puller connection with local");
            ex.printStackTrace();
            return;
        }
    }
    
    public void makezabbixConnection(){
        try{
            con2 = su2.getcon();
        }catch(Exception ex){
            System.out.println("something wrong with data puller connection with zabbix");
            ex.printStackTrace();
            return;
        }
    }
    
    public data_puller(){
        makelocalConnection();
        makezabbixConnection();
        long rawMax = getRawMaxClock();
        if(rawMax==0){
            next = 1495297500;
        }else{
            rawMax -= (rawMax % 60);
            next = rawMax + 60;
        }
        itemList = getItemList();
        zabbixMax = getZabbixMaxClock();
        if(zabbixMax - rawMax >= 60) flag3=1;
   
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
            return null;
            
        }
        
        items = new Integer[temp.size()];
        items = temp.toArray(items);
        Arrays.sort(items);
        return res;
    }
    
    public void run() {
        System.out.println("In Run and next in this schedule is "+next);
        if(itemList==null){ 
            itemList = getItemList();
            if(itemList==null){System.out.println("Item List NUll"); return;}
        }
        zabbixMax = getZabbixMaxClock();
        if(zabbixMax==-2){flag3=1;return;}
        System.out.println(zabbixMax+" ## "+next);
        if(zabbixMax-next<60){
            System.out.println("waiting thread "+count);
            return;
        }else{
            fullySync_Flag=0;
        }  
        if(zabbixMax-next>120)flag3=1;
        try{
            if(con2==null || con2.isClosed())
                con2 = su2.getcon();
        }catch(Exception ex){
             System.out.println("something wrong with data puller connection with zabbix");
             ex.printStackTrace();
             flag2=1;
             return ;
        }
        
        try{
            if(con==null||con.isClosed())
                con = su.getcon();
        }catch(com.mysql.jdbc.exceptions.jdbc4.CommunicationsException ex){
            System.out.println("Communication Link failure");
            return;
        }catch(Exception ex){
            System.out.println("something wrong with data puller connection with local server");
            flag2=1;
            ex.printStackTrace();
            return;
        }
        
            
        if(flag3==1){
            if(bringSync(zabbixMax)){
                System.out.println("After sync");
                fullySync_Flag=1;
                flag2=0;
                flag3=0;
            }    
            return;
        } 
        if(flag2==1){
            System.out.println("In flag2 block");
            long zabbixMax = getZabbixMaxClock();
            if(zabbixMax - next > 60){
                if(bringSync(zabbixMax)){
                    flag2=0;
                    flag3=0;
                };
                return;
            } 
        }
        pullData();
    } 
    
    public long getRawMaxClock(){
        String query = "select max(clock) as maxi from raw_data ";
        long res=0l;
        sqlutil su = new sqlutil();
       // Connection con=null;
        try{
            if(con == null||con.isClosed())
                con = su2.getcon();
            ResultSet rs = su.selectQuery(query, params, con);
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
        }
        return res;
    }
    
    public long getZabbixMaxClock(){
        params.clear();
        String Zquery = "select max(clock) as maxi from (select clock from history_uint where clock > ? and itemid in "+itemList+") as T ";
        params.add(next);
        long res=0l;
        try{
            if(con2==null||con2.isClosed())
                con2 = su2.getcon();
            ResultSet rs = su2.selectQuery(Zquery, params, con2);
            if(rs==null)return -2;
            if(rs.next()){
                String temp = rs.getString("maxi");
                if(temp==null){
                    try{
                        
                    }catch(Exception ex){
                        System.out.println("Exception in sleep");
                    }
                    return -1;
                } 
                else res = Long.parseLong(rs.getString("maxi"));
            }else{
                System.out.println("Yes");
                return -1;
            }
                
        }catch(com.mysql.jdbc.exceptions.jdbc4.CommunicationsException ex){
            System.out.println("Communication Link failure");
            return -2;
        }
        catch(java.sql.SQLException sql_ex){
            System.out.println("param in getZabbixMax is "+params);
            sql_ex.printStackTrace();
        }
        catch(Exception ex){
            System.out.println("Exception in getting maxClock from zabbix");
            ex.printStackTrace();
            return 0;
        }
        params.clear();
        return res;    
    }
    
    public boolean bringSync(long zabbixMax){
        try{
            if(con == null||con.isClosed())
            con = new sqlutil().getcon();
        }catch(Exception ex){
            System.out.println("connection fail bring sync");
            return false;
        }
        
        if(zabbixMax==0) zabbixMax = getZabbixMaxClock();
        while(true){
            while(zabbixMax-next >= 60){
              pullData();
              System.out.println("while bringsync  "+next+" "+zabbixMax);
          }
          zabbixMax = getZabbixMaxClock();
          if(zabbixMax  - next <=60) break;
          System.out.println("still not in sync  "+next+" "+zabbixMax);
        }  
          System.out.println("fully sync "+next+" "+zabbixMax);
          return true;
    }
    
    public void pullData(){
        params.add(next);params.add(next+60);
        int hostid=0;int itemid = 0;
        System.out.println("pullData for clock "+next);
        String Vquery = "select * from history_uint where clock >= ? and clock < ? and itemid in "+itemList+" order by itemid";
        try{
            ResultSet rs = su2.selectQuery(Vquery, params, con2);
            int i = 0;
            DateFieldAdder adder = new DateFieldAdder();
            long value = 0;long clock =0;
            while(rs.next()){
                itemid = Integer.parseInt(rs.getString("itemid"));
                if(i<items.length&&items[i]==itemid){
                 //   System.out.println("equal");
                    value = Long.parseLong(rs.getString("value"));
                    clock = Long.parseLong(rs.getString("clock"));
                    hostid = hm.get(itemid);
                    adder.Dateinsertor(hostid,itemid,clock,value,con);
                }else{
                    while(i<items.length&&items[i]!=itemid){
                        value = 0;
                        hostid = hm.get(items[i]);
                        clock = next;
                        adder.Dateinsertor(hostid,items[i],clock,value,con);
                        i++;
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
        }catch(java.sql.SQLException sql_ex){
            System.out.println("Exception in pull Data and param" + params);
            sql_ex.printStackTrace();
        }
        catch(Exception ex){
            System.out.println("Exception in pullData" + itemid+" "+hostid+" "+next);
            ex.printStackTrace();
        }
        params.clear();
        next+=60;
    }  
}
         