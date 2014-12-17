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
        
        if(args.length<1) System.out.println("Usage: lodsight <SPARQL Endpoint> [predicate limit](use 0 for no-limit) [graph](default:all) [summary ID](optional) ");
        else {
        	System.out.println( "summarization started" );
        	int predicateLimit = 0;
        	int summaryId = -1;
        	String endpoint = args[0];
        	String graph = "";
        	if(args.length>1) predicateLimit = Integer.parseInt(args[1]);
        	if(args.length>2) graph = args[2];
        	if(args.length>3) summaryId = Integer.parseInt(args[3]);
        	Summarizer summarizer = new Summarizer(endpoint, graph, predicateLimit);
        	summarizer.summarizeDataset(summaryId);
        }
    }
}
