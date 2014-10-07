package cz.vse.keg.Versobest.LODSight;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.rdf.model.RDFNode;

public class Path {
	private List<RDFNode> nodes;
	private int frequency;
	
	public Path(int freq) {
		nodes = new ArrayList<RDFNode>();
		frequency = freq;
	}
	
	public void addNode(RDFNode n) {
		nodes.add(n);
	}
	
	public List<Triplet> getTriplets() {
		if(nodes.size() >= 3 && (nodes.size() - 3) % 2 == 0) {
			List<Triplet> triplets = new ArrayList<Triplet>();
			for(int i=0; i+2<nodes.size(); i+=2) {
				triplets.add(new Triplet(nodes.get(i),nodes.get(i+1),nodes.get(i+2)));
			}
			return triplets;
		}
		else return null;
	}
	
	public String toString() {
		String s = nodes.get(0).asResource().getLocalName();
		for(int i=1; i<nodes.size(); i++) {
				s += ((i % 2 == 0)?"/":"\\")+nodes.get(i).asResource().getLocalName();
		}
		s += " : "+frequency;
		return s;
	}
	
}
