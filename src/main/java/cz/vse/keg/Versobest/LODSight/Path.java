package cz.vse.keg.Versobest.LODSight;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.rdf.model.RDFNode;

public class Path implements Comparable<Path>{
	private List<RDFNode> nodes;
	private int frequency=0;
	
	public Path(int freq) {
		nodes = new ArrayList<RDFNode>();
		frequency = freq;
	}
	
	public void addNode(RDFNode n) {
		nodes.add(n);
	}
	
	public List<Triplet> getTriplets() {
		List<Triplet> triplets = new ArrayList<Triplet>();
		if(nodes.size() >= 3 && (nodes.size() - 3) % 2 == 0) {
			for(int i=0; i+2<nodes.size(); i+=2) {
				triplets.add(new Triplet(nodes.get(i),nodes.get(i+1),nodes.get(i+2)));
			}
		}
		return triplets;
	}
	
	public List<RDFNode> getNodes() { return nodes; }
	
	public String toString() {
		String s = nodes.get(0).asResource().getLocalName();
		for(int i=1; i<nodes.size(); i++) {
				s += ((i % 2 == 0)?"/":"\\")+nodes.get(i).asResource().getLocalName();
		}
		s += " : "+frequency;
		return s;
	}
	
	public void setFreq(int freq) {
		frequency  =freq;
	}
	
	public int getFreq() { return frequency; }

	@Override
	public int compareTo(Path o) {
		// TODO Auto-generated method stub
		if(o.frequency > frequency) return -1;
		else if(o.frequency == frequency) return 0;
		else return 1;
	}
	
}
