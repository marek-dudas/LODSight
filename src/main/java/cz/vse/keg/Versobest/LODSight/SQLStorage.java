package cz.vse.keg.Versobest.LODSight;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.jena.atlas.iterator.RepeatApplyIterator;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.function.library.namespace;

public class SQLStorage implements PathDoneChecker {
	Connection conn;
	int sumid = -1;
	
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
	
	public void addSummary(Summarizer sum){
		
		sumid = generateID("Summary", "SumID");
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
	
	private int getPrefixID(String prefix){
		String sqlString = "SELECT PrefixID FROM Prefix WHERE URI = ?";
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = conn.prepareStatement(sqlString);
			preparedStatement.setString(1, prefix);
			preparedStatement.execute();
			java.sql.ResultSet result = preparedStatement.getResultSet();
			if(result != null && result.next()) {
				return result.getInt("PrefixID");				
			}
			else {
				int prefixID = generateID("Prefix", "PrefixID");
				preparedStatement = conn.prepareStatement("INSERT INTO Prefix (PrefixID, URI) VALUES (?,?)");
				preparedStatement.setInt(1, prefixID);
				preparedStatement.setString(2, prefix);
				preparedStatement.executeUpdate();
				return prefixID;
			}		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}
	
	private int getEntityID(RDFNode node){
		int prefixId = getPrefixID(node.asResource().getNameSpace());
		
		String sqlString = "SELECT EntityID FROM Entity WHERE EntityName = ? AND PrefixID = ?";
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = conn.prepareStatement(sqlString);
			preparedStatement.setString(1, node.asResource().getLocalName());
			preparedStatement.setInt(2, prefixId);
			preparedStatement.execute();
			java.sql.ResultSet result = preparedStatement.getResultSet();
			if(result != null && result.next()) {
				return result.getInt("EntityID");				
			}
			else {
				return addEntity(node);
			}		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}
	
	private int addEntity(RDFNode n){
		Integer entid = generateID("Entity", "EntityID");
		String sqlString = "INSERT INTO Entity(EntityID, EntityName, PrefixID) VALUES (?,?,?)";
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = conn.prepareStatement(sqlString);
			preparedStatement.setString(1, entid.toString());
			preparedStatement.setString(2, n.asResource().getLocalName());
			preparedStatement.setInt(3, getPrefixID(n.asResource().getNameSpace()));
			preparedStatement.executeUpdate();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return entid;
	}	
	
	public void storePath(Path path) {
		Integer pathid = generateID("Path", "PathID");
		String sqlString = "INSERT INTO Path(PathID, SumID, Frequency, PathHash) VALUES (?,?,?,?)";
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = conn.prepareStatement(sqlString);
			preparedStatement.setInt(1, pathid);
			preparedStatement.setInt(2, sumid);
			preparedStatement.setInt(3, path.getFreq());
			preparedStatement.setString(4, getPathHash(path));
			preparedStatement.executeUpdate();
			int tripletIndex = 0;
			for (Triplet t : path.getTriplets()) {
				storePathTriplet(t, pathid, tripletIndex);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	private void storePathTriplet(Triplet t, int pathid, int orderNum) {
		String sqlString = "INSERT INTO Triplet(PathID, OrderNum, Subject_EntityID, Predicate_EntityID, Object_EntityID) VALUES (?,?,?,?,?)";
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = conn.prepareStatement(sqlString);
			preparedStatement.setInt(1, pathid);
			preparedStatement.setInt(2, sumid);
			preparedStatement.setInt(3, getEntityID(t.s()));
			preparedStatement.setInt(4, getEntityID(t.p()));
			preparedStatement.setInt(5, getEntityID(t.o()));
			preparedStatement.executeUpdate();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	private String getPathHash(Path path) {
		String hash = "";
		boolean first = true;
		for (RDFNode n : path.getNodes()) {
			if(!first) hash += "-";
			first = false;
			hash += Integer.toString(getEntityID(n));
		}
		return hash;
	}

	@Override
	public boolean isPathChecked(Path path) {
		String sqlString = "SELECT * FROM Path WHERE PathHash = ?";
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = conn.prepareStatement(sqlString);
			preparedStatement.setString(1, getPathHash(path));
			preparedStatement.execute();
			java.sql.ResultSet result = preparedStatement.getResultSet();
			if(result != null && result.next()) {
				return true;				
			}
			else {
				return false;
			}		
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return false;
	}
}
