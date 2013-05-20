package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ch.epfl.advdb.milestone2.io.FVector;
import ch.epfl.advdb.milestone2.io.FVectorIMDB;
import ch.epfl.advdb.milestone2.io.FVectorNetflix;

/**
 * Class containing functions to generate the K-Means seeds based on the 
 * input set of features
 * @author Bernard Gütermann
 *
 */
public class KSeeds {
	
	static String newLine = System.getProperty("line.separator"); 

	/**
	 * generate seeds using a custom version of K++ algorithm. The seeds are generated using 
	 * the following scheme : 
	 * 1. Take a random feature point and add it as seed
	 * 2. Take the point that has the largest cumulated distance with the set of points in the seeds
	 * 3. Add this point to the set of seed. 
	 * 4. execute step 2 and 3 until K seeds have been chosen
	 * @param args The args input table containing the path to the train set
	 * @param K The number of seeds to be generated
	 * @param type "IMDB" or "Netflix" so that the function handle genericity
	 * @return 0 if success, -1 otherwise
	 */
	public static int runKpp(String[] args, final int K, String type){
		Path ip;
		Path op;

		if(type.equals("IMDB")){
			ip=new Path(args[0]+"/features");
			op=new Path(args[2]+"/clusterIMDB0/part-0");
		}
		else if (type.equals("Netflix")){
			ip=new Path(args[2]+"/V0");
			op=new Path(args[2]+"/clusterNetflix0/part-0");
		}
		else return -1;
		
		Configuration c= new Configuration();
		FileSystem fs;
		//READ F
		ArrayList<FVector> f = new ArrayList<FVector>();
		FileStatus[] status;
		try {
			fs  = FileSystem.get(c);
			status = fs.listStatus(ip);
			for (int i=0;i<status.length;i++)
			{
				String fileName[] = status[i].getPath().toString().split("/");
				if (fileName[fileName.length-1].contains("features")||
						fileName[fileName.length-1].contains("part")) {
					BufferedReader br=new BufferedReader(
							new InputStreamReader(fs.open(status[i].getPath())));
					String line=br.readLine();
					while (line != null)
					{
						if(line.split(",").length>1){
							FVector v;
							if (type.equals("IMDB")){
								v = new FVectorIMDB(line);
							}else if (type.equals("Netflix")){
								v = new FVectorNetflix(line);
							}else return -1;
							if (!f.contains(v))
								f.add(v);
						}
						line = br.readLine();
					}
					br.close();
				}
			}
		} catch (IOException e) {
			System.err.println("KSeeds : unable to fetch features");
			e.printStackTrace();
			return -1;
		}  
		//KMeans++
		Random r = new Random();
		ArrayList<FVector> seeds = new ArrayList<FVector>();
		FVector v = f.get(r.nextInt(f.size()));
		seeds.add(v);
		for(int i=0;i<K-1;i++){
			//find the vector farthest away from the current set of taken vectors
			float maxDistance = 0;
			float totDistance = 0;
			FVector farthest = null;
			for (FVector p : f){
				totDistance = 0;
				for (FVector s : seeds)
					totDistance += p.getDistance(s);
				if (totDistance > maxDistance){
					maxDistance = totDistance;
					farthest = p;
				}
			}
			seeds.add(farthest);
			f.remove(farthest);
		}
		//write to disk
		try{
			fs  = FileSystem.get(c);
			BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(op,true)));
			int i =0;
			for (FVector s : seeds){
				String[] w = s.toString().split(",");
				StringBuilder out= new StringBuilder().append(i).append(":");
				for (int j = 1; j<w.length;++j){
					if (w[j].contains(newLine))
						w[j] =w[j].replace(newLine, "");
					if (type.equals("IMDB"))
						out.append(w[j].replace(".0", "")).append(",1;");
					else if (type.equals("Netflix"))
						out.append(j-1).append(",").append(w[j]).append(";");
					else return -1;
				}
				out.append("\n");
				br.write(out.toString());
				++i;
			}
			br.close();
		}
		catch (IOException e){
			System.err.println("KSeeds : unable to write seeds");
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	/**
	 * Generate K seeds by taking randomly K vectors in the input set
	 * @param args The args input table containing the path to the train set
	 * @param K The number of seeds to be generated
	 * @param type "IMDB" or "Netflix" so that the function handle genericity
	 * @return 0 if success, -1 otherwise
	 */
	public static int runRandom(String[] args, final int K, String type){
		Path ip;
		Path op;
		if(type.equals("IMDB")){
			ip=new Path(args[0]+"/features");
			op=new Path(args[2]+"/clusterIMDB0/part-0");
		}
		else if (type.equals("Netflix")){
			ip=new Path(args[2]+"/V0");
			op=new Path(args[2]+"/clusterNetflix0/part-0");
		}
		else return -1;
		
		Configuration c= new Configuration();
		FileSystem fs;
		//READ F
		ArrayList<String> f = new ArrayList<String>();
		FileStatus[] status;
		try {
			fs  = FileSystem.get(c);
			status = fs.listStatus(ip);
			for (int i=0;i<status.length;i++)
			{
				String fileName[] = status[i].getPath().toString().split("/");
				if (fileName[fileName.length-1].contains("features")||
						fileName[fileName.length-1].contains("part")) {
					BufferedReader br=new BufferedReader(
							new InputStreamReader(fs.open(status[i].getPath())));
					String line=br.readLine();
					//for each line
					while (line != null)
					{
						if(line.split(",").length>1)
							f.add(line);
						line = br.readLine();
					}
					br.close();
				}
			}
		} catch (IOException e) {
			System.err.println("KSeeds : unable to fetch features");
			e.printStackTrace();
			return -1;
		}  

		//WRITE RANDOM SEEDS
		try{
			fs  = FileSystem.get(c);
			BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(op,true)));
			Random r = new Random();

			String line;
			ArrayList<Integer> hashes = new ArrayList<Integer>(K);
			for(int i=0;i<K;i++){
				//avoid repetitions
				line=f.get(r.nextInt(f.size()));
				while (hashes.contains(line.hashCode())){
					line=f.get(r.nextInt(f.size()));
				}
				hashes.add(line.hashCode());
				//write to disk
				String[] w = line.split(",");
				StringBuilder out= new StringBuilder().append(i).append(":");
				for (int j = 1; j<w.length;++j){
					if (w[j].contains(newLine))
						w[j] =w[j].replace(newLine, "");
					if (type.equals("IMDB"))
						out.append(w[j]).append(",1;");
					else if (type.equals("Netflix"))
						out.append(j-1).append(",").append(w[j]).append(";");
					else return -1;
				}
				out.append("\n");
				br.write(out.toString());
			}
			br.close();
		}
		catch (IOException e){
			System.err.println("KSeeds : unable to write seeds");
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
}
