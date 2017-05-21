
package DateFieldAdder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;
import sql.sqlutil;

public class DateFieldAdder {
    public ArrayList al = new ArrayList();
    public void Dateinsertor(int hostid,int itemid,long clock,long value,Connection con){
        Date date1 = new Date((clock+19800)*1000L);     // *1000 is to convert seconds to milliseconds
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
        sdf.setTimeZone(TimeZone.getTimeZone("GMT")); // give a timezone reference for formating (see comment at the bottom
        String time = sdf.format(date1);
        insertRawData(hostid,itemid,clock,value,time,con);
        time = null;
        sdf = null;

    }
  
    void insertRawData(int hostid,int itemid,long clock,long val,String time,Connection con){
        
        String query = "insert into raw_data(hostid,itemid,clock,Time,value )values(?,?,?,?,?)";
        al.add(hostid);al.add(itemid);al.add(clock);al.add(time); al.add(val);
        sqlutil su = new sqlutil();
        try{    
                //Connection con = su.getcon();
                int rs = su.ins_upd_del(query, al,con);
                if(rs > 0) ;//System.out.println("Inserted");
                else System.out.println("failed");
        }catch(Exception ex){
                System.out.println("Exception while inserting raw_data");
                ex.printStackTrace();
        }
        query = null;al.clear();
    }
}
