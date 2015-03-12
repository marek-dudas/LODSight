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
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class CSetFinder {
	private String endpoint;
	private CSetStorage storage;
	private String graph;
	private int hackerPause = 3000;
	
	public void setHackerPause(int pause) {
		hackerPause = pause;
	}
	
	public CSetFinder(String endpoint, String graph, CSetStorage storage)
	{
		this.storage = storage;
		this.endpoint = endpoint;
		this.graph = graph;
	}
	
	public CSet findForSubject(RDFNode s) {
		System.out.println("searching for predicates for " + s.asResource().getLocalName());
		
		Query query = QueryFactory.create(getSetQuery(s)) ;
		List<Triplet> triplets = new ArrayList<Triplet>();
		  QueryExecution qexec = null;
		  try {
			  qexec  = QueryExecutionFactory.sparqlService(endpoint, query, graph);
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
			  RDFNode objType = soln.get("?otyped");
			  if(objType != null)
			  {
				  triplets.add(new Triplet(s, predicate, objType));
			      System.out.println("found predicate ... " + predicate.asResource().getNameSpace()+ " : "+ 
			    		  predicate.asResource().getLocalName());
			  }
		      
		    }
		  return new CSet(triplets);		 
	}
	
	public CSet findForSubjectSimple(RDFNode s) {
		System.out.println("searching for predicates for " + s.asResource().getLocalName());
		
		Query query = QueryFactory.create(getDatatypeQuery(s)) ;
		System.out.println(query);
		List<Triplet> triplets = new ArrayList<Triplet>();
		  QueryExecution qexec = null;
		  try {
			  qexec  = QueryExecutionFactory.sparqlService(endpoint, query, graph);
		  }
		  catch(Error e) {
			  System.err.println("Error on creating query: "+query+" .... error: "+e.getMessage());
			  return null;
		  }
		    ResultSet results = qexec.execSelect() ;
		    System.out.println(results);
		    for ( ; results.hasNext() ; )
		    {
		      QuerySolution soln = results.nextSolution() ;
		      RDFNode predicate = soln.get("?p") ;       // Get a result variable by name.
			  RDFNode objType = soln.get("?otype");
			  if(predicate != null)
			  {
				  //triplets.add(new Triplet(s, predicate, objType));
			      System.out.println("found predicate ... " + predicate.asResource().getNameSpace()+ " : "+ 
			    		  predicate.asResource().getLocalName());
			  }
		      
		    }
		    
		 CSet datatypeCSet = new CSet(triplets);
		 
		 query = QueryFactory.create(getLangstringQuery(s));
		 System.out.println(query);
		 qexec = null;
		  try {
			  qexec  = QueryExecutionFactory.sparqlService(endpoint, query, graph);
		  }
		  catch(Error e) {
			  System.err.println("Error on creating query: "+query+" .... error: "+e.getMessage());
			  return null;
		  }
		    results = qexec.execSelect() ;
		    for ( ; results.hasNext() ; )
		    {
		      QuerySolution soln = results.nextSolution() ;
		      RDFNode predicate = soln.get("?p") ;       
		      
			  if(! datatypeCSet.getPredicates().contains(predicate))
			  {
				  triplets.add(new Triplet(s, predicate, ResourceFactory.createResource("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")));
			      System.out.println("found predicate ... " + predicate.asResource().getNameSpace()+ " : "+ 
			    		  predicate.asResource().getLocalName());
			  }
		      
		    }
		    
		  return new CSet(triplets);		 
	}
	
	public void findSets(List<RDFNode> subjects) {
		for (RDFNode s : subjects) {
			if(!storage.cSetExists(s))
			{
				try {
					Thread.sleep(hackerPause);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				CSet cSet = findForSubject(s);
				if(cSet != null && cSet.getTriplets().size() > 0) this.storage.storeCSet(cSet);
				System.out.println("waiting for " + hackerPause + "ms");
			}
		}
	}
	
	private String getDatatypeQuery(RDFNode subjectType) {
		return "SELECT DISTINCT ?p ?o \n"
				+ "WHERE {\n" 
				+ "?s a <"+subjectType+"> . \n"
				+ "?s ?p ?o  \n"
				//+ "OPTIONAL {?o a ?otype} . \n"
				//+ "BIND(if(isIRI(?o), <http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>, if(lang(?o)=\"\", datatype(?o), <http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>)) AS ?otyped) \n"
				//+ "BIND(if(isLiteral(?o), datatype(?o), \"\") AS ?otype) .\n"
				//+ "FILTER(datatype(?o) != '') .\n"
				+ "FILTER(isLiteral(?o))\n"
				+ "}";
	}

	private String getLangstringQuery(RDFNode subjectType) {
		return "SELECT DISTINCT ?p WHERE { \n"
				+ "?s a <"+subjectType+"> . \n"
				+ "?s ?p ?o . \n"
				+ "filter(isLiteral(?o) && lang(?o) != '') }";
	}
	
	private String getSetQuery(RDFNode subjectType){
		return "SELECT DISTINCT ?p ?otyped WHERE { \n"
				+ "?s a <"+subjectType+"> . \n"
				//+ "?o a ?otype . " //v0.2 including datatypes, more complex type finding
				+ "?s ?p ?o . \n"
				+ "OPTIONAL {?o a ?otype} . \n"
				+ "BIND(if(isIRI(?o), ?otype, if(lang(?o)=\"\", datatype(?o), <http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>)) AS ?otyped) \n"
				//+ "FILTER(isLiteral(?o)) \n"
				//+ "BIND(if(lang(?o)=\"\", datatype(?o), <http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>) AS ?otyped) \n"
				+ "FILTER(bound(?otyped) && !bound(?otype)) \n"
				+ "FILTER(?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> && ?p != <http://www.w3.org/2000/01/rdf-schema#subClassOf>) \n"
				+ "}";
	}
}
