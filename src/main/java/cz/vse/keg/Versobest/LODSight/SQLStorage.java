package cz.vse.keg.Versobest.LODSight;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.jena.atlas.iterator.RepeatApplyIterator;

import com.hp.hpl.jena.rdf.model.RDFNode;

public class SQLStorage {
	Connection conn;
	
	public SQLStorage(String server, String db, String username, String password) {
		try {
            // The newInstance() call is a work around for some
            // broken Java implementations

            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            // handle the error
        }
		
		try {
		    conn =
		       DriverManager.getConnection("jdbc:mysql://"+server+":3306/"+db+"?" +
		                                   "user="+username+"&"+"password="+password);

		    // Do something with the Connection
		} catch (SQLException ex) {
		    // handle any errors
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		}
	}
	
	private int generateID(String table, String column) {
		int id = -1;
		Statement setIdStatement;
		try {
			do {
				id = (int) Math.random()*100000000;
				setIdStatement = conn.createStatement();
				setIdStatement.execute("SELECT "+column+" FROM "+table+" WHERE "+column+" = "+id);
			} while(setIdStatement.getResultSet()!=null);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return id;
	}
	
	public void storeSummary(Summarizer sum){
		
		int sumid = generateID("Summary", "column");
		String sqlString = "INSERT INTO Summary(Dataset, Endpoint, SumID) VALUES(?,?,?)";
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = conn.prepareStatement(sqlString);
			preparedStatement.setString(1, sum.getGraph());
			preparedStatement.setString(2, sum.getEndpoint());
			preparedStatement.setInt(3, sumid);
			preparedStatement.executeUpdate();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
	}
	
	private void addEntity(RDFNode n){
		
	}
	
	
	
	public void storePath(Path path) {
		
	}
}
