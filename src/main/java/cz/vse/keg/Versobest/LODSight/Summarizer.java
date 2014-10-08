package cz.vse.keg.Versobest.LODSight;

public class Summarizer {
	String endpoint, graph;
	public Summarizer(String endpoint, String graph){
		this.endpoint = endpoint;
		this.graph = graph;
	}
	
	String getEndpoint() { return endpoint;}
	String getGraph() { return graph; }
}
