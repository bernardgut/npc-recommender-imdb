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
	/** Number of clusters
	 */
	public final static int K = 88;
	/** Number of Reducers
	 */
	public final static int REDUCERS = 44;
	/** Number of Users in the V data set
	 */
	public final static int USERS = 478867;
	/** Number of dimensions in the V data set
	 */
	public final static int DIMENSIONS = 10;
	/** Threshold : a movie is deemed relevant if it has a rating above this
	 */
	public final static int T = 0;
	
	/**
	 * Main
	 * @param args <input train folder> <input test folder> <output> 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		//number of iterations
		int i = 0;
		//number of points that have converged after a given iteration
		long count = 0;
		System.out.println("################## TRAIN ################");
		//K-MEANS IMDB
		System.out.println("######## SEEDS ########");		
		if(KSeeds.runKpp(args, K, "IMDB")==-1) printAndQuit("K-Seeds");
		System.out.println("######## END SEEDS########");	
		System.out.println("######## STARTING K MEANS IMDB ########");	
		while (count<K && i<10){
			System.out.println("######## IMDB : ITERATION "+i+" ########");	
			count = KMeans.runIMDB(args, i, REDUCERS, K);
			if(count==-1) printAndQuit("KMeans imdb iteration "+i);
			System.out.println("####### STATS for iteration "+i+" #######");
			System.out.println("Converged centroids :"+count);
			System.out.println("###################################");
			++i;
		}
		System.out.println("######## END K MEANS IMDB ########");	

		//KMEAN NETFLIX
		i=0;
		System.out.println("######## V-Formatting ########");		
		if(VFormating.run(args, REDUCERS)==-1)	printAndQuit("K-Seeds");
		System.out.println("########## SEEDS ##########");		
		if(KSeeds.runKpp(args, K, "Netflix")==-1) printAndQuit("K-Seeds");
		System.out.println("######## STARTING K MEANS : NETFLIX ########");	
		count =0;
		while (count<K && i<10){
			System.out.println("######## NF ITERATION "+i+" ########");	
			count = KMeans.runNetflix(args, i, REDUCERS, K);
			if(count==-1) printAndQuit("KMeans netflix iteration "+i);
			System.out.println("##### STATS for iteration "+i+" #####");
			System.out.println("Converged centroids :"+count);
			System.out.println("###################################");
			++i;
		}
		System.out.println("######## END K MEANS NETFLIX ########");
		
		//MAPPING
		System.out.println("########## MAPPING ##########");		
		if(Maping.run(args, K)==-1) printAndQuit("Mapper");
		System.out.println("########################################");
		
		//TEST PHASE
		System.out.println("################## TEST ################");
		System.out.println("########## BUILD RECOMMANDATIONS ##########");
		if(Recommander.run(args, REDUCERS, K, USERS, DIMENSIONS, T)==-1)
			printAndQuit("Recommander");
//		System.out.println("########## COMPUTE FSCORE ##########");	
//		if(FScore.run(args, REDUCERS, T)==-1) printAndQuit("FScore");
//		System.out.println("########## AVERAGE FSCORE ##########");	
//		double m = Fetchers.fetchFScoreMean(args);
//		if (m==-1) printAndQuit("fetchFScore");
//		System.out.println("AFS = "+m);
		
	}

	/**
	 * quits with the given message
	 * @param string exit message
	 */
	private static void printAndQuit(String string) {
		System.out.println("ERROR : "+string);
		System.exit(1);
	}
}	
