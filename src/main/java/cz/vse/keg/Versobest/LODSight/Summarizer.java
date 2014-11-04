package cz.vse.keg.Versobest.LODSight;

public class Summarizer {
	String endpoint, graph;
	public Summarizer(String endpoint, String graph){
		this.endpoint = endpoint;
		this.graph = graph;
	}
	
	String getEndpoint() { return endpoint;}
	String getGraph() { return graph; }
	
	public void summarizeDataset(int continueWithID) {

    	PathFinder pathF = new PathFinder(args[0]);
    	pathF.initPathFinding();
    	System.out.println( "---------pathfinding started---------" );
    	//pathF.findPaths();
	}
}
