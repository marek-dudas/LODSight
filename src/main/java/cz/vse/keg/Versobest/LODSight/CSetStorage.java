package cz.vse.keg.Versobest.LODSight;

import com.hp.hpl.jena.rdf.model.RDFNode;

public interface CSetStorage {
	public boolean cSetExists(RDFNode subject);
	public boolean storeCSet(CSet cSet);
}
