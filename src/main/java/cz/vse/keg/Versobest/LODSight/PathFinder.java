package cz.vse.keg.Versobest.LODSight;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.path.PathWriter;

public class PathFinder {
	
	private String endpoint;
	private String defaultGraph;
	
	private static String findPredicatesQuery = "SELECT DISTINCT ?p WHERE {?s ?p ?o .}";
	private static String findClassesQuery = "SELECT DISTINCT ?class WHERE {?s a ?class .}";
	
	private List<RDFNode> predicates;
	private List<RDFNode> classes;
	private List<Path> paths;
	private int predicateI, subjectI, objectI;

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
			      System.out.println(predicate.asResource().getNameSpace()+ " : "+ 
			        predicate.asResource().getLocalName() + " ... " + predicate.toString());
		      }
		    }
		  return true;		      
	}
	
	private boolean findClasses() {		 
		  Query query = QueryFactory.create(findClassesQuery) ;
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
		      RDFNode rdfsclass = soln.get("?class") ;       // Get a result variable by name.
			  classes.add(rdfsclass);
		      System.out.println(rdfsclass.asResource().getNameSpace()+ " : "+ 
		    		  rdfsclass.asResource().getLocalName() + " ... " + rdfsclass.toString());
		    }
		  return true;		      
	}
	
	public PathFinder(String sparqlEndpoint, String defGraph) {
		predicates = new ArrayList<RDFNode>();
		classes = new ArrayList<RDFNode>();
		this.endpoint = sparqlEndpoint;
		this.defaultGraph = defGraph;
	}
	
	public void initPathFinding() {
		findPredicates();
		findClasses();
		predicateI = subjectI = objectI = 0;
	}	
	
	private String getPathQuery(String t1, String p, String t2){
		return "SELECT (COUNT(DISTINCT ?o) AS ?total) WHERE {"
				+ "?s a <"+t1+"> . "
				+ "?o a <"+t2+"> . "
				+ "?s <"+p+"> ?o . }";
	}
	
	public void findPaths() {
		paths = new ArrayList<Path>();
		for(predicateI = 0; predicateI < predicates.size(); predicateI++) {
			for(subjectI = 0; subjectI < classes.size(); subjectI++) {
				for(objectI = 0; objectI < classes.size(); objectI++)
				{
					Query query = QueryFactory.create(getPathQuery(classes.get(subjectI).toString(), 
							predicates.get(predicateI).toString(), 
							classes.get(objectI).toString())) ;
					QueryExecution qexec = null;
					try {
						qexec  = QueryExecutionFactory.sparqlService(endpoint, query, defaultGraph);
					}
					catch(Error e) {
						  System.err.println("Error on creating query: "+query+" .... error: "+e.getMessage());
					}
					ResultSet results = qexec.execSelect();
					int count = 0;
					if(results.hasNext())
						count = results.next().get("?total").asLiteral().getInt();
					if(count>0) {
						Path path = new Path(count);
						path.addNode(classes.get(subjectI));
						path.addNode(predicates.get(predicateI));
						path.addNode(classes.get(objectI));
						paths.add(path);
					}
				}		
				System.out.println("Progress: "+predicateI*classes.size()+subjectI*classes.size()+objectI / classes.size()*classes.size()*predicates.size());
			}
			System.out.println("Progress: "+predicateI*classes.size()+subjectI*classes.size()+objectI / classes.size()*classes.size()*predicates.size());
		}
	
	}
	
}
