package cz.vse.keg.Versobest.LODSight;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class PathFinder {
	private int hackerPause = 300000;
	private String endpoint;
	private String defaultGraph;
	
	private static String findPredicatesQuery = ""
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
			+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
			+ "SELECT DISTINCT ?p" 
			+ " WHERE {"
			+ "?s ?p ?o ."
			+ "FILTER (?p != rdfs:subClassOf && ?p != rdf:type)" //v0.2 added FILTER"
			+ "}";
	private static String findClassesQuery = "SELECT DISTINCT ?class WHERE {?s a ?class .}";
	
	private List<RDFNode> predicates;
	private List<RDFNode> classes;
	private List<Path> paths;
	private int predicateI, subjectI, objectI;
	
	private int predicateLimit;
	
	public void setHackerPause(int pause) {
		hackerPause = pause;
	}

	private boolean findPredicates() {	
		String predQueryString = findPredicatesQuery;
		if(predicateLimit>0) predQueryString += " LIMIT "+predicateLimit;
		  Query query = QueryFactory.create(predQueryString) ;
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
		String queryString = findClassesQuery;
		if(predicateLimit>0) queryString += " LIMIT "+predicateLimit;
		  Query query = QueryFactory.create(queryString) ;
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
	
	public PathFinder(String sparqlEndpoint, String defGraph, int predicateLimit) {
		predicates = new ArrayList<RDFNode>();
		classes = new ArrayList<RDFNode>();
		this.endpoint = sparqlEndpoint;
		this.defaultGraph = defGraph;
		this.predicateLimit = (predicateLimit>0) ? predicateLimit : Integer.MAX_VALUE;
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
	
	public void findPaths(PathDoneChecker pathChecker) {
		paths = new ArrayList<Path>();
		for(predicateI = 0; predicateI < predicates.size() && predicateI<predicateLimit; predicateI++) {
			for(subjectI = 0; subjectI < classes.size(); subjectI++) {
				for(objectI = 0; objectI < classes.size(); objectI++)
				{
					Path path = new Path(0);
					path.addNode(classes.get(subjectI));
					path.addNode(predicates.get(predicateI));
					path.addNode(classes.get(objectI));
					int storedFrequency = pathChecker.getPathFrequency(path);
					if (storedFrequency>=0) 
					{
						path.setFreq(storedFrequency);
						if(storedFrequency>0) paths.add(path);
					}
					else {					
						Query query = QueryFactory.create(getPathQuery(classes.get(subjectI).toString(), 
								predicates.get(predicateI).toString(), 
								classes.get(objectI).toString())) ;
						QueryExecution qexec = null;
						ResultSet results = null;
						int count = -1;
						do {							
							try {
								qexec  = QueryExecutionFactory.sparqlService(endpoint, query, defaultGraph);
								results = qexec.execSelect();
								if(results!=null && results.hasNext())
									count = results.next().get("?total").asLiteral().getInt();
							}
							catch(Throwable e) {
								  System.err.println("Error on creating/running query: "+query+" .... error: "+e.getMessage());
								  try {
										System.out.println("waiting for " + hackerPause + "ms");
										Thread.sleep(hackerPause);
									} catch (InterruptedException interruptedException) {
										// TODO Auto-generated catch block
										interruptedException.printStackTrace();
									}
							}
						} while (count<0);
						
						if(count>0) {
							path.setFreq(count);
							paths.add(path);
						}
						pathChecker.storePath(path);
					}
				}
				System.out.println("Progress: "+(predicateI*classes.size()*classes.size()+subjectI*classes.size()+objectI)+" / "+((classes.size()*classes.size()*predicates.size())));
			}		
			
			//System.out.println("Progress: "+predicateI*classes.size()+subjectI*classes.size()+objectI / classes.size()*classes.size()*predicates.size());
		}
		
		Collections.sort(paths);
	
	}
	
	public List<Path> getPaths() { return paths;}
	
}
