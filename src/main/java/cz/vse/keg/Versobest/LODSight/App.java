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
        
        if(args.length<1) System.out.println("Usage: lodsight <SPARQL Endpoint> [summary ID](optional) ");
        else {
        	System.out.println( "summarization started" );
        }
    }
}
