package cz.vse.keg.Versobest.LODSight;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "LODSight started" );
        
        if(args.length<1) System.out.println("Usage: lodsight <config-file> <SPARQL Endpoint> [predicate limit](use 0 for no-limit) [graph](default:all) [summary ID](optional) ");
        else {
        	
        	Properties prop = new Properties();
        	InputStream input = null;
         
        	try {
         
        		input = new FileInputStream(args[0]);
         
        		// load a properties file
        		prop.load(input);
         
        	} catch (IOException ex) {
        		ex.printStackTrace();
        	} finally {
        		if (input != null) {
        			try {
        				input.close();
        			} catch (IOException e) {
        				e.printStackTrace();
        			}
        		}
        	}
        	
        	System.out.println( "summarization started" );
        	int predicateLimit = 0;
        	int summaryId = -1;
        	String endpoint = args[1];
        	String graph = "";
        	if(args.length>2) predicateLimit = Integer.parseInt(args[2]);
        	if(args.length>3 && !args[3].equals("all")) graph = args[3];
        	if(args.length>4) summaryId = Integer.parseInt(args[4]);
        	Summarizer summarizer = new Summarizer(endpoint, graph, predicateLimit);
        	summarizer.summarizeDataset(summaryId,prop.getProperty("server"), prop.getProperty("database"),
        			prop.getProperty("dbuser"),prop.getProperty("dbpassword"),Integer.parseInt(prop.getProperty("pathHackerPause")),
        			Integer.parseInt(prop.getProperty("csetHackerPause")));
        }
    }
}
