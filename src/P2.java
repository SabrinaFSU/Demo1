/**
 * Created by sixuan on 4/11/2016.
 */
//In a country X, the population is counted at postal code level, town/city level, county level, state level and country level.
//        Exact number of levels can vary.
//        The data is stored in a database of your choice. However the population values of those levels are not adding up.
//        So write a  Java program & corresponding SQL/tables for reading the population data and roll-up appropriately from
//        each level upto the country level and write it back to the database with corrected data for each level.


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;




public class P2 {




    public  void connect()
    {
        Connection c = null;
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:test.db");
            System.out.println("Connection to SQLite has been established.");
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }

    }




    public void createNewDatabase() {

        String url = "jdbc:sqlite:C://sqlite/db/tests.db";

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }



    public void createNewTable() {
        // SQLite connection string
        String url = "jdbc:sqlite:C://sqlite/db/tests.db";

        // SQL statement for creating a new table
        String sql = "CREATE TABLE IF NOT EXISTS P2" +
                "(CountryName String PRIMARY KEY     NOT NULL," +
                " PostalCodeLevel     float     NOT NULL, " +
                " TownCityLevel     float     NOT NULL, " +
                " CountyLevel       float     NOT NULL, " +
                " StateLevel        float     NOT NULL, " +
                " CountryLevel         float     )";
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            // create a new table
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }



    public void insert()
    {
        Connection c = null;
        Statement stmt = null;
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:test.db");
            c.setAutoCommit(false);
            System.out.println("Opened database successfully");

            stmt = c.createStatement();
            String sql = "INSERT INTO P2(CountryName,PostalCodeLevel,TownCityLevel,CountyLevel,StateLevel,CountryLevel)" +
                    "VALUES ('China',3.2,3.4,3.2,3.5, );";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO P2(CountryName,PostalCodeLevel,TownCityLevel,CountyLevel,StateLevel,CountryLevel)" +
                    "VALUES ('USA',0.9,1.2,1.3,0.8, );";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO P2(CountryName,PostalCodeLevel,TownCityLevel,CountyLevel,StateLevel,CountryLevel)" +
                    "VALUES ('Japan',0.6,0.5,0.4,0.5, );";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO P2(CountryName,PostalCodeLevel,TownCityLevel,CountyLevel,StateLevel,CountryLevel)" +
                    "VALUES ('India',3.8,2.5,3.5,3.3, );";
            stmt.executeUpdate(sql);

            stmt.close();
            c.commit();
            c.close();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
        System.out.println("Records created successfully");
    }






    public void select()
    {
        Connection c = null;
        Statement stmt = null;
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:test.db");
            c.setAutoCommit(false);
            System.out.println("Opened database successfully");

            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery( "SELECT * FROM P2;" );
            while ( rs.next() ) {
                String CountryName = rs.getString("CountryName");
                float PostalCodeLevel  = rs.getFloat("PostalCodeLevel");
                float TownCityLevel  = rs.getFloat("TownCityLevel");
                float CountyLevel = rs.getFloat("CountyLevel");
                float StateLevel  = rs.getFloat("StateLevel");
                float CountryLevel  = rs.getFloat("CountryLevel");
                System.out.println( "CountryName = " + CountryName );
                System.out.println( "PostalCodeLevel = " + PostalCodeLevel);
                System.out.println( "Town/cityLevel = " + TownCityLevel );
                System.out.println( "CountyLevel = " + CountyLevel );
                System.out.println( "StateLevel = " + StateLevel );
                System.out.println( "CountryLevel = " +CountryLevel );
                System.out.println();
            }
            rs.close();
            stmt.close();
            c.close();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
        System.out.println("Operation done successfully");
    }





//    public  void rollUp()
//    {
//        Connection c = null;
//        Statement stmt = null;
//        try {
//            Class.forName("org.sqlite.JDBC");
//            c = DriverManager.getConnection("jdbc:sqlite:test.db");
//            c.setAutoCommit(false);
//            System.out.println("Opened database successfully");
//
//            stmt = c.createStatement();
//            String sql = "UPDATE COMPANY set SALARY = 25000.00 where ID=1;";
//            stmt.executeUpdate(sql);
//            c.commit();
//
//            ResultSet rs = stmt.executeQuery( "SELECT * FROM P2;" );
//            while ( rs.next() ) {
//                float CountryLevel  = rs.getFloat("CountryLevel");
//                System.out.println( "CountryLevel = " +CountryLevel );
//                System.out.println();
//            }
//            rs.close();
//            stmt.close();
//            c.close();
//        } catch ( Exception e ) {
//            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
//            System.exit(0);
//        }
//        System.out.println("Operation done successfully");
//    }




    public static void main(String args[]){

    }
}
