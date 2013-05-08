/*
 * BERNARD GUTERMANN (c) 2013
 */
package ch.epfl.advdb.milestone2;

/**
 * 
 * @author Bernard Gütermann
 *
 */
public class Main {
	final static int K = 4;
	final static int REDUCERS = 2;
	/**
	 * Main
	 * @param args <input folder> <output folder> 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		//INIT K-MEANS SEEDS
		System.out.println("######## SEEDS ########");		
		if(KSeeds.run(args, K)==-1) printAndQuit("K-Seeds");
		System.out.println("######## END SEEDS########");	
		System.out.println("######## STARTING K MEANS ########");	
		long convCount=0;
		int i = 0;
		while (convCount<K||i<10){
			System.out.println("######## ITERATION "+i+" ########");	
			convCount=KMeans.run(args, i, REDUCERS, K);
			if(convCount==-1) printAndQuit("KMeans iteration "+i);
			System.out.println("##### STATS for iteration "+i+" #####");
			System.out.println("Converged centroids :"+convCount);
			System.out.println("###################################");
			++i;
		}
	}

	private static void printAndQuit(String string) {
		System.out.println("ERROR : "+string);
		System.exit(1);
		
	}
	
}	
