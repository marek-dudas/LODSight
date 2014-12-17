package cz.vse.keg.Versobest.LODSight;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.rdf.model.RDFNode;

public class Summarizer {
	int cSetLimit = 20;
	int predicateLimit;
	String endpoint, graph;
	public Summarizer(String endpoint, String graph, int predicateLimit){
		this.endpoint = endpoint;
		this.graph = graph;
		this.predicateLimit = predicateLimit;
	}
	
	String getEndpoint() { return endpoint;}
	String getGraph() { return graph; }
	
	public void summarizeDataset(int continueWithID) {

		SQLStorage storage = new SQLStorage("192.168.1.2", "lodsight", "lodsight", "loddva");
		if(continueWithID>=0) storage.continueWithSummary(continueWithID);
		else storage.addSummary(this);
    	PathFinder pathF = new PathFinder(endpoint, graph, predicateLimit);
    	pathF.initPathFinding();
    	System.out.println( "---------pathfinding started---------" );
    	pathF.findPaths(storage);
    	System.out.println( "---------pathfinding ended: "+ pathF.getPaths().size() + "paths found ---------" );
    	List<RDFNode> frequentClassesList = new ArrayList<RDFNode>();
    	for(int i=0; i<cSetLimit && i<pathF.getPaths().size(); i++) {
    		List<RDFNode> pathNodes = pathF.getPaths().get(i).getNodes();
    		for(int j=0; j<pathNodes.size(); j+=2) {
    			if(!frequentClassesList.contains(pathNodes.get(j))) frequentClassesList.add(pathNodes.get(j));
    		}
    	}
    	CSetFinder setFinder = new CSetFinder(endpoint, graph, storage);
    	setFinder.findSets(frequentClassesList);
	}
}
