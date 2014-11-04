package cz.vse.keg.Versobest.LODSight;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.rdf.model.RDFNode;

public class CSet {
	private List<RDFNode> predicates;
	private RDFNode forSubject;
	private List<Triplet> triplets;
	private int frequency = -1;
	
	public CSet(List<Triplet> t) {
		triplets = t;
		predicates = new ArrayList<RDFNode>();
		for (Triplet triplet : t) {
			forSubject = triplet.s();
			if(!predicates.contains(triplet.p())) predicates.add(triplet.p());
		}
	}
	
	public int getFrequency() { return frequency;}
	
	public List<RDFNode> getPredicates() { return predicates; }
	
	public List<Triplet> getTriplets() { return triplets; }
}
