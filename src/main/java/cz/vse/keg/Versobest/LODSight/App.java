package cz.vse.keg.Versobest.LODSight;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "LODSight started" );
        
        if(args.length<2) System.out.println("Usage: lodsight <SPARQL Endpoint> <Default graph>");
        else {
        	System.out.println( "summarization started" );
        	PathFinder pathF = new PathFinder(args[0], args[1]);
        	pathF.initPathFinding();
        	System.out.println( "---------pathfinding started---------" );
        	pathF.findPaths();
        }
    }
}
