package cz.vse.keg.Versobest.LODSight;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class CSetFinder {
	private String endpoint;
	
	public CSetFinder(String endpoint)
	{
		this.endpoint = endpoint;
	}
	
	public CSet findForSubject(RDFNode s, CSetStorage storage) {
	
		Query query = QueryFactory.create(getSetQuery(s)) ;
		List<Triplet> triplets = new ArrayList<Triplet>();
		  QueryExecution qexec = null;
		  try {
			  qexec  = QueryExecutionFactory.sparqlService(endpoint, query);
		  }
		  catch(Error e) {
			  System.err.println("Error on creating query: "+query+" .... error: "+e.getMessage());
			  return null;
		  }
		    ResultSet results = qexec.execSelect() ;
		    for ( ; results.hasNext() ; )
		    {
		      QuerySolution soln = results.nextSolution() ;
		      RDFNode predicate = soln.get("?p") ;       // Get a result variable by name.
			  RDFNode objType = soln.get("?otype");
			  triplets.add(new Triplet(s, predicate, objType));
		      System.out.println(predicate.asResource().getNameSpace()+ " : "+ 
		    		  predicate.asResource().getLocalName());
		      
		    }
		  return new CSet(triplets);		 
	}
	
	private String getSetQuery(RDFNode subjectType){
		return "SELECT DISTINCT ?p, ?otype WHERE {"
				+ "?s a <"+subjectType+"> . "
				+ "?o a ?otype . "
				+ "?s ?p ?o . }";
	}
}
