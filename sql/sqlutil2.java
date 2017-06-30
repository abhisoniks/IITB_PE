
package sql;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;

public class sqlutil2 {

    String Driver;
    String Username;
    String password;
    String url;
    Connection con;
    public void loadValues() throws IOException {
        InputStream is = this.getClass().getResourceAsStream("properties2.properties");
        java.util.Properties p = new java.util.Properties();
        p.load(is);
        Driver = p.getProperty("Driver");
        Username = p.getProperty("UserName");
        password = p.getProperty("password");
        url = p.getProperty("url");
    }

    public java.sql.Connection getcon() throws Exception {
        loadValues();
        Class.forName(Driver);
        con = DriverManager.getConnection(url,Username, password);
        return con;
    }

    public int ins_upd_del(String query, ArrayList al,Connection con) throws Exception {
        try{
           // con = getcon();
            PreparedStatement ps = con.prepareStatement(query);
            if (al.size() > 0 && al != null) {
                for (int i = 0; i < al.size(); i++) {
                    ps.setObject(i + 1, al.get(i));
                }
            }
            int row = ps.executeUpdate();
            return row;
        }
        catch(Exception ex){
            System.out.println("Exception found at sqlutil2 file in ins_upd_del function");
            ex.printStackTrace(System.out);
            return 0;
        }
        
    }
    public ResultSet selectQuery(String query,ArrayList al,Connection con)throws Exception{
        try{
            PreparedStatement ps=con.prepareStatement(query);
            if(al!=null&&al.size()>0)
            {
                for(int i=0;i<al.size();i++)
                    ps.setObject(i+1,al.get(i));
            }
            ResultSet rs=ps.executeQuery();
            ps = null;
            return rs;
        }
        catch(Exception ex){
            System.out.println("exception caught in sqlutil2 file in search function");
            return null;
        }
    }
}

    

