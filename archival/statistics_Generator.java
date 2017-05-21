package archival;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;
import sql.*;
import Data_puller.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class statistics_Generator extends TimerTask {
    boolean flag =false;
    static HashMap<String,Integer> host = new HashMap<String,Integer>();
    static HashMap<Integer,Integer> items = new HashMap<Integer,Integer>();
    Connection local_connection = null;
    ArrayList params = new ArrayList();
    File f = new File("./src/files/archivalInfo_admin");
    public statistics_Generator(){ 
        System.out.println(">>>>");
        sqlutil su = new sqlutil();
        try{
               local_connection = su.getcon();
        }catch(Exception ex){
            System.out.println("something wrong with local db connection");
        }
    }
    
    static{
        addHost ah = new addHost();
        Connection con = null;
        try{
            
            con = ah.getcon();
            String query = "select * from hosts";
            ArrayList al = new ArrayList();
            ResultSet rs = ah.selectQuery(query, al, con);
            while(rs.next()){
                String hostName = rs.getString("host");
                int hostId = Integer.parseInt(rs.getString("hostid"));
               // System.out.println(hostName+" "+hostId);
                host.put(hostName,hostId);
            }
        }catch(Exception ex){
            System.out.println("Exception in making connection with addHost file");
        }finally{
            try{
                con.close();
            }catch(Exception ex){
            }
        }    
    }
    
    public void run(){
        if(flag==false){ flag = true; return;}
        System.out.println("In run");
        stats_generator(1);    
    }
    
    public void getItemList(){
        addHost su  = new addHost();
        Connection con = null;
        try{
            String query = "select itemid,hostid from items";
            ArrayList al = new ArrayList();
            con = su.getcon();
            ResultSet rs = su.selectQuery(query, al, con);
            int item=0;int host=0;
            while(rs.next()){
                item = Integer.parseInt(rs.getString("itemid"));
                host = Integer.parseInt(rs.getString("hostid"));
                items.put(item, host);
            }
            al = null;
        }catch(Exception ex){
            System.out.println("Exception in getting connection with addHost database");
            ex.printStackTrace();   
        }
    }
    
    
    void stats_generator(int level){   
        sqlutil2 su2 = new sqlutil2();
        sqlutil su = new sqlutil();
        if(local_connection==null){
            try{
               local_connection = su.getcon();
            }catch(Exception ex){
                System.out.println("something wrong with local db connection");
            return;
            }
        }
        
        try{
           
           BufferedReader br = new BufferedReader(new FileReader(f));
           String line,hostname,interface_name,key;
           int hostid=0, itemid=0;
           int count=0;
           String record_prime=null;String record = null;
           while((line = br.readLine()) != null && count<=level){
              if(count==level){
                  record_prime = line;
                  break;
              }else{
                  count++;
              }
           }
           if(count==level){
               String prime = br.readLine();
               if(prime==null) return;
               
               Iterator it = items.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry)it.next();
                    itemid = (int)pair.getKey();
                    hostid = (int)pair.getValue();
                    System.out.println(hostid+" pair "+itemid);
                    insertStats(record_prime,prime,hostid,itemid,level);
                    it.remove(); // avoids a ConcurrentModificationException
                }
                it = null;
                stats_generator(level+1);
                
           }     
        }catch(IOException ex){
           System.out.println("exception while reading archivalInfo_admin");
        }
        
    }
    
    
   
    
    /*
    void stats_generator(int level){   
        sqlutil2 su2 = new sqlutil2();
        Connection zabbix_connection=null;
        Connection local_connection = null;
        try{
               zabbix_connection = su2.getcon();
        }catch(Exception ex){
            System.out.println("something wrong with connection with zabbix db");
            return ;
        }
        sqlutil su = new sqlutil();
        try{
               local_connection = su.getcon();
        }catch(Exception ex){
            System.out.println("something wrong with local db connection");
            return;
        }

        try{
           File f = new File("./src/files/archivalInfo_admin");
           BufferedReader br = new BufferedReader(new FileReader(f));
           String line,hostname,interface_name,key;
           int hostid=0, itemid=0;
           int count=0;
           String record_prime=null;String record = null;
           while((line = br.readLine()) != null && count<=level){
              if(count==level){
                  record_prime = line;
                  break;
              }else{
                  count++;
              }
           }
           if(count==level){
               String prime = br.readLine();
               if(prime==null) return;
               File file = new File("./src/files/hosts_file");
               BufferedReader brr = new BufferedReader(new FileReader(file));
               brr.readLine();
               String[] str = null;
               String tmp = "%";
               String tmp2 = "%traffic%";
               while((line = brr.readLine()) != null){
                    str = line.split("\\s+");
                   // hostname = "%"+str[0]+"%";
                    hostname = str[0];
                 //   hostname = tmp+str[0]+tmp;
                    interface_name = tmp+str[1]+tmp;
                    key  = tmp+str[2]+tmp2;
                    Zabbix_Info zi = new Zabbix_Info();
                    hostid = host.get(hostname);
                   // hostid = zi.gethostid(hostname,zabbix_connection);
                    itemid = zi.getItemid(hostid,interface_name,key,zabbix_connection);
                    System.out.println(hostname+" "+hostid+" "+itemid);
                    insertStats(record_prime,prime,hostid,itemid,level);
                    hostname = null;
                    interface_name = null;
                }
                str = null;
                tmp=null;
                tmp2=null;
                brr.close();
                stats_generator(level+1);
                try{
                    zabbix_connection.close();
                    local_connection.close();
                }catch(Exception ex){
                    System.out.println("Exception in closing connection");
                }
                
           }     
        }catch(IOException ex){
           System.out.println("exception while reading archivalInfo_admin");
        }
        
    }*/
    
    public void insertStats(String record_prime,String record,int hostid,int itemid,int level){
        String[] str_prime = record_prime.split("\\s+");
        String[] str = record.split("\\s+");
        if(str.length!=5) return;
        if(str_prime.length!=5) return;
        int Gran_prime = Integer.parseInt(str_prime[0]);
        int period_prime = Integer.parseInt(str_prime[1]);
        String table_prime = str_prime[2];
        int Gran = Integer.parseInt(str[0]);
        int period = Integer.parseInt(str[1]);
        String table = str[2];
        long row_count = get_rowCount(table_prime, hostid, itemid);
        long p = (period_prime  *60/ Gran_prime) + Gran/Gran_prime;
        while(row_count>= p){
            if(row_count - (Gran/Gran_prime) > (period_prime  *60/ Gran_prime)  ){
                    ResultSet rs2 =  getOldestEntry(Gran/Gran_prime,table_prime,hostid,itemid);
                    String num_stats= getNumericStats(Gran/Gran_prime,table_prime,hostid,itemid,level);
                    nextlevelInsertion( num_stats , hostid,  itemid,table);
                    deleteRows(Gran/Gran_prime,table_prime,hostid,itemid);
                    row_count = row_count - ( Gran/Gran_prime );      
            }else break;
        }
        table_prime = null;
        table = null; str_prime = null;str = null;
    }
     
    public void nextlevelInsertion(String num_stats, int hostid, int itemid,String table){
            String[] new_str = num_stats.split("\\s+");
            long total  = Long.parseLong(new_str[0]);
            double avg  = Double.parseDouble(new_str[1]);
            long min    = Long.parseLong(new_str[2]);
            long max    = Long.parseLong(new_str[3]);
            int zeroCount   = Integer.parseInt(new_str[5]);
            long clock  = Long.parseLong(new_str[4]);
            String date = unixtimeConvertor(clock);
            ResultSet rs=null;
            sqlutil su2 = new sqlutil();
            String query = "insert into "+table+" (hostid,itemid,clock,Time,"
                    + "totalTraffic,avgTraffic,minTraffic,maxTraffic,zeroTraffic)values(?,?,?,?,?,?,?,?,?)";
           // ArrayList params = new ArrayList();
            params.add(hostid); params.add(itemid); params.add(clock);
            params.add(date);params.add(total);params.add(avg);params.add(min);params.add(max);
            params.add(zeroCount); Connection local_connection=null;
            try{
               local_connection = su2.getcon();
               int p = su2.ins_upd_del(query,params,local_connection);
               if(p>0) ;//System.out.println(p + "number of rows are inserted successfulyy for "+hostid+" "+itemid);
               else System.out.println("Insertion unsuccessfull for "+hostid+" "+itemid);
            }catch(Exception ex){
                System.out.println("exception while insertion at current level "+hostid+" "+itemid);
                ex.printStackTrace();
            }finally{
                try{
                    local_connection.close();
                }catch(Exception ex){
                    System.out.println(">>>");
                }
            }
            new_str = null;params.clear();

    }
    
    public long get_rowCount(String table,int hostid,int itemid){
            ResultSet rs=null;//Connection local_connection=null;
            sqlutil su2 = new sqlutil();
            String query = "select count(*) as row_count from "+table +" where hostid = ? and itemid = ?";
         //   ArrayList params = new ArrayList();
            params.add(hostid); params.add(itemid);
            long row_count=0;
            try{
                Connection local_connection = su2.getcon();
                rs = su2.selectQuery(query,params,local_connection);  
                while(rs.next())
                     row_count = Long.parseLong(rs.getString("row_count"));
                local_connection.close();
            }catch(Exception ex){
                System.out.println("Exception in retrieving row count in prime_table" + hostid+" "+itemid);
                ex.printStackTrace();
            }
            params.clear();query = null;
            return row_count;
    }
    
    public ResultSet getOldestEntry(long count,String table,int hostid,int itemid){
        
        ResultSet rs=null;
        sqlutil su2 = new sqlutil();
        String query = "select * from "+table+" where hostid = ? and itemid= ? order by clock asc limit "+count;
      //  ArrayList params = new ArrayList();
        params.add(hostid);params.add(itemid);
    //    Connection local_connection = null;
       if(local_connection == null){
           try{
            local_connection = su2.getcon();
           } catch(Exception ex){
            System.out.println("Exception in getOldestEntry");
           }         
       }
        try{
            rs = su2.selectQuery(query,params,local_connection);  
        }catch(Exception ex){
            System.out.println("Exception in retrieving oldest entry in prime_tablefor"+" "
                    +hostid+" "+itemid);
            ex.printStackTrace();
            return null;
        }
        finally{
            try{
                params.clear();
            }catch(Exception ex){
            
            }
        }
        return rs;
    }
    
    String getNumericStats(long num,String table,int hostid,int itemid,int level){
        ResultSet rs=null;
        String result=null;
        sqlutil su2 = new sqlutil();
        String query;
        System.out.println(level+" "+table);
        if(level==1)
           query = "select sum(value) as total,avg(value) as avge,max(value) as maxe,"
                + "min(value) as mine,min(clock) as minclock from (select * from "
                +table+" where hostid =? and itemid=? order by clock asc limit ?) temp ";
        else
           query = "select sum(totalTraffic) as total,avg(avgTraffic) as avge,min(minTraffic)"
                + " as mine, max(maxTraffic) as maxe, min(clock) as minclock from"
                + "  (select * from "+table+" where hostid =? and itemid=? order"
                + " by clock asc limit ?) temp ";
     //   ArrayList params = new ArrayList();
        params.add(hostid);params.add(itemid);params.add(num);
        String query2;
        if(level==1)
            query2 = "select count(*) as count from (select * from "+table+""
                    + " where hostid = ? and itemid= ? order by clock asc limit ? )"
                    + "temp where value=?";
        else 
            query2 = "select count(*) as count from (select * from "+table+" where"
                    + " hostid = ? and itemid= ? order by clock asc limit ? )temp "
                    + "where totalTraffic=?";
        try{
            if(local_connection==null)
                local_connection = su2.getcon();
            rs = su2.selectQuery(query,params,local_connection);
            while(rs.next()){
                String total = rs.getString("total");
                String avg = rs.getString("avge");
                String min = rs.getString("mine");
                String max = rs.getString("maxe");
                String minclock = rs.getString("minclock");
                result = total+" "+avg+" "+min+" "+max+" "+minclock;
            }
            params.add(0);
            rs = su2.selectQuery(query2,params,local_connection);
            while(rs.next()){
                String count = rs.getString("count");
                result=result+" "+count;
            }    
        }catch(Exception ex){
            System.out.println("Exception in retrieving statistics in prime_table" +hostid+" "+itemid);
            ex.printStackTrace();
        }finally{
            try{ params.clear();
            }catch(Exception ex){}
            
        }
        return result;
    }
    
    void deleteRows(int num,String table,int hostid,int itemid){
        ResultSet rs=null;
        sqlutil su2 = new sqlutil();
        String query = "DELETE  from  "+ table+" where hostid = ? and itemid = ? order by clock asc limit ?";
      //  ArrayList params = new ArrayList();
        params.add(hostid); params.add(itemid); params.add(num);
        
         try{
             if(local_connection==null)
                local_connection = su2.getcon();
            int p = su2.ins_upd_del(query,params,local_connection);
            if(p>0);// System.out.println(p + "number of rows are deleted successfulyy");
            else System.out.println("Deletion unsuccessfull for "+hostid+" "+itemid);
         }catch(Exception ex){
             System.out.println("exception while deleting rows from prime table "+hostid+" "+itemid);
             ex.printStackTrace();
         }
         params.clear();
    }
    
    String unixtimeConvertor(long unix_time){
        Date date1 = new Date((unix_time+19800)*1000L); // *1000 is to convert seconds to milliseconds
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
        sdf.setTimeZone(TimeZone.getTimeZone("GMT")); // give a timezone reference for formating (see comment at the bottom
        String time = sdf.format(date1);
        return time;
    }
}    
