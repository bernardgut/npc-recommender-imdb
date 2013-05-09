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

public class KSeeds {
	static String newLine = System.getProperty("line.separator"); 

	public static int run(String[] args, final int K){
		Configuration c= new Configuration();
		Path ip = new Path(args[0]+"/features");
		Path op = new Path(args[2]+"/clusterIMDB0/part-0");
		FileSystem fs;
		//READ F
		ArrayList<String> f = new ArrayList<String>();
		FileStatus[] status;
		try {
			fs  = FileSystem.get(c);
			status = fs.listStatus(ip);
			//For each correponding file
			for (int i=0;i<status.length;i++)
			{
				String fileName[] = status[i].getPath().toString().split("/");
				if (fileName[fileName.length-1].contains("features")) {
					BufferedReader br=new BufferedReader(
							new InputStreamReader(fs.open(status[i].getPath())));
					String line=br.readLine();
					//for each line
					while (line != null)
					{
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
				//ensure non repeatability
				line=f.get(r.nextInt(f.size()))+"\n";
				hashes.add(line.hashCode());
				while (hashes.contains(line)){
					line=f.get(r.nextInt(f.size()))+"\n";
					hashes.add(line.hashCode());
				}
				//write to disk
				String[] w = line.split(",");
				String out=i+":";
				for (int j = 1; j<w.length;++j){
					if (w[j].contains(newLine))
						w[j] =w[j].replace(newLine, "");
					if(!w[j].equals(""))
						out+=w[j]+",1;";
				}
				out+="\n";
				System.out.println("writing :"+out);
				br.write(out);
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
