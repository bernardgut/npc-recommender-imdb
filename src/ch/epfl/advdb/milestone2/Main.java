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
	final static int USERS = 190152;
	final static int DIMENSIONS = 10;
	/**
	 * Main
	 * @param args <input folder> <output folder> 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		//TRAIN PHASE
		System.out.println("################## TRAIN ################");
		int convCount=0;
		int i = 0;
		//KMEAN IMDB
		System.out.println("######## SEEDS ########");		
		if(KSeeds.runRandom(args, K, "IMDB")==-1) 
			printAndQuit("K-Seeds");
		System.out.println("######## END SEEDS########");	
		
		System.out.println("######## STARTING K MEANS IMDB ########");	
		while (convCount<K && i<10){
			System.out.println("######## IMDB : ITERATION "+i+" ########");	
			convCount=KMeans.runIMDB(args, i, REDUCERS, K);
			if(convCount==-1) printAndQuit("KMeans imdb iteration "+i);
			
			System.out.println("##### STATS for iteration "+i+" #####");
			System.out.println("Converged centroids :"+convCount);
			System.out.println("###################################");
			++i;
		}
		System.out.println("######## END K MEANS IMDB ########");	

		//KMEAN NETFLIX
		convCount=0;
		i=0;
		System.out.println("######## V-Formatting ########");		
		if(VFormating.run(args, REDUCERS)==-1) printAndQuit("K-Seeds");
		
		System.out.println("########## SEEDS ##########");		
		if(KSeeds.runRandom(args, K, "Netflix")==-1) 
			printAndQuit("K-Seeds");
		
		System.out.println("######## STARTING K MEANS : NETFLIX ########");	
		while (convCount<K && i<10){
			System.out.println("######## NF ITERATION "+i+" ########");	
			convCount=KMeans.runNetflix(args, i, REDUCERS, K);
			if(convCount==-1) printAndQuit("KMeans netflix iteration "+i);
			
			System.out.println("##### STATS for iteration "+i+" #####");
			System.out.println("Converged centroids :"+convCount);
			System.out.println("###################################");
			++i;
		}
		System.out.println("######## END K MEANS NETFLIX ########");
		
		//MAPPING
		System.out.println("########## MAPPING ##########");		
		if(Maping.run(args, K)==-1) 
			printAndQuit("Mapper");
		System.out.println("########################################");
		
		//TEST PHASE
		System.out.println("################## TEST ################");
		
		System.out.println("########## BUILD RECOMMANDATIONS ##########");
		if(Recommander.run(args, REDUCERS, K, USERS, DIMENSIONS)==-1)
			printAndQuit("Recommander");
		
		System.out.println("########## COMPUTE FSCORE ##########");	
		if(FScore.run(args, REDUCERS)==-1)
			printAndQuit("FScore");
	}

	private static void printAndQuit(String string) {
		System.out.println("ERROR : "+string);
		System.exit(1);
		
	}
}	
