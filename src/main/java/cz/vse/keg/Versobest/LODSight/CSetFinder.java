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
	private CSetStorage storage;
	private String graph;
	private int hackerPause = 1000;
	
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
	
	public void findSets(List<RDFNode> subjects) {
		for (RDFNode s : subjects) {
			if(!storage.cSetExists(s))
			{
				CSet cSet = findForSubject(s);
				if(cSet != null && cSet.getTriplets().size() > 0) this.storage.storeCSet(cSet);
				System.out.println("waiting for " + hackerPause + "ms");
				try {
					Thread.sleep(hackerPause);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	private String getSetQuery(RDFNode subjectType){
		return "SELECT DISTINCT ?p ?otyped WHERE { \n"
				+ "?s a <"+subjectType+"> . \n"
				//+ "?o a ?otype . " //v0.2 including datatypes, more complex type finding
				+ "?s ?p ?o . \n"
				+ "OPTIONAL {?o a ?otype} . \n"
				+ "BIND(if(isIRI(?o), ?otype, if(lang(?o)=\"\", datatype(?o), <http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>)) AS ?otyped) \n"
				+ "FILTER(bound(?otyped)) \n"
				+ "FILTER(?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> && ?p != <http://www.w3.org/2000/01/rdf-schema#subClassOf>) \n"
				+ "}";
	}
}
