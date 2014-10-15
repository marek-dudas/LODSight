package cz.vse.keg.Versobest.LODSight;

import com.hp.hpl.jena.rdf.model.RDFNode;

public class Triplet {
	private RDFNode t1,p,t2;
	
	public Triplet(RDFNode t1, RDFNode p, RDFNode t2)
	{
		this.t1 = t1; this.t2 = t2; this.p = p;
	}
	
	public RDFNode s() { return t1;}
	public RDFNode p() { return p;}
	public RDFNode o() { return t2;}
}
