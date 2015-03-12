package cz.vse.keg.Versobest.LODSight;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class PathFinderSimple {
	private String endpoint;
	private String defaultGraph;
	private List<Path> paths;
	
	private String findPathsQuery = 
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"
			+"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
			+"SELECT DISTINCT ?t1 ?t2 ?p (COUNT(*) AS ?total) \n"
			+"WHERE { \n"
			+"?s ?p ?o . \n"
			+"?s a ?t1 . \n"
			+"?o a ?t2 . \n"
			+"FILTER (?p != rdfs:subClassOf && ?p != rdf:type && ?p != rdfs:range && ?p != rdfs:domain) \n"
			+"} GROUP BY ?t1 ?t2 ?p";
	
	public PathFinderSimple(String sparqlEndpoint, String defGraph) {
		this.endpoint = sparqlEndpoint;
		this.defaultGraph = defGraph;
	}
	
	public void findPaths(PathDoneChecker pathChecker) {
		paths = new ArrayList<Path>();
		  Query query = QueryFactory.create(findPathsQuery) ;
		  System.out.println(query);
		  QueryExecution qexec = null;
		  try {
			  qexec  = QueryExecutionFactory.sparqlService(endpoint, query, defaultGraph);
		  }
		  catch(Error e) {
			  System.err.println("Error on creating query: "+query+" .... error: "+e.getMessage());
		  }
		  if(qexec!=null)
		  {
		    ResultSet results = qexec.execSelect() ;
		    for ( ; results.hasNext() ; )
		    {
		    	QuerySolution soln = results.nextSolution() ;
			  
			  	Path path = new Path(soln.get("?total").asLiteral().getInt());
				path.addNode(soln.get("?t1"));
				path.addNode(soln.get("?p"));
				path.addNode(soln.get("?t2"));				
		      
		      	paths.add(path);
				pathChecker.storePath(path);
				System.out.println(path);
		    }
		  }		      
	}
	
	public List<Path> getPaths() { return paths;}
	
}
