package cz.vse.keg.Versobest.LODSight;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class PathFinder {
	
	private String endpoint;
	private String defaultGraph;
	
	private static String findPredicatesQuery = "SELECT DISTINCT ?p WHERE {?s ?p ?o .}";
	private static String findClassesQuery = "SELECT DISTINCT ?p WHERE {?s ?p ?o .}";
	
	private List<RDFNode> predicates;

	private boolean findPredicates() {		 
		  Query query = QueryFactory.create(findPredicatesQuery) ;
		  QueryExecution qexec = null;
		  try {
			  qexec  = QueryExecutionFactory.sparqlService(endpoint, query, defaultGraph);
		  }
		  catch(Error e) {
			  System.err.println("Error on creating query: "+query+" .... error: "+e.getMessage());
			  return false;
		  }
		    ResultSet results = qexec.execSelect() ;
		    for ( ; results.hasNext() ; )
		    {
		      QuerySolution soln = results.nextSolution() ;
		      RDFNode predicate = soln.get("?p") ;       // Get a result variable by name.
		      if(predicate.asResource().getURI()!="http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
		      {
			      predicates.add(predicate);
			      //System.out.println(predicate.asResource().getNameSpace()+ " : "+ 
			      //predicate.asResource().getLocalName() + " ... " + predicate.toString());
		      }
		    }
		  return true;		      
	}
	
	private boolean findClasses() {		 
		  Query query = QueryFactory.create(findPredicatesQuery) ;
		  QueryExecution qexec = null;
		  try {
			  qexec  = QueryExecutionFactory.sparqlService(endpoint, query, defaultGraph);
		  }
		  catch(Error e) {
			  System.err.println("Error on creating query: "+query+" .... error: "+e.getMessage());
			  return false;
		  }
		    ResultSet results = qexec.execSelect() ;
		    for ( ; results.hasNext() ; )
		    {
		      QuerySolution soln = results.nextSolution() ;
		      RDFNode predicate = soln.get("?p") ;       // Get a result variable by name.
		      if(predicate.asResource().getURI()!="http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
		      {
			      predicates.add(predicate);
			      //System.out.println(predicate.asResource().getNameSpace()+ " : "+ 
			      //predicate.asResource().getLocalName() + " ... " + predicate.toString());
		      }
		    }
		  return true;		      
	}
	
	public PathFinder(String sparqlEndpoint, String defGraph) {
		predicates = new ArrayList<RDFNode>();
		this.endpoint = sparqlEndpoint;
		this.defaultGraph = defGraph;
	}
	
	public void initPathFinding() {
		findPredicates();
	}
	
}
