package ordo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.ObjectInputStream;

import java.net.InetAddress;

import java.rmi.Naming;

import java.util.ArrayList;

import formats.Format;
import formats.Format.Type;
import hdfs.*;
import formats.KVFormat;
import formats.LineFormat;
import formats.*;
import map.MapReduce;
import config.Project;

public class JobLauncher implements JobInterfaceX {

	Type inputformat;
	String inputfname;
	Type outputformat;
	String outputfname;
	int nbReduces;
	int nbMaps;
	SortComparator sortComparator;

	public JobLauncher() {
	}

	public JobLauncher(Type format, String name, int nb_maps) {
		this.inputformat = format;
		this.outputformat = format;
		this.inputfname = name;
		this.outputfname = name + "out";
		this.nbMaps = nb_maps;
		this.nbReduces = 1;
	}

	@Override
	public void setInputFormat(Type ft) {
		this.inputformat = ft;

	}

	@Override
	public void setInputFname(String fname) {
		this.inputfname = fname;

	}

	// On prend ici des valeurs indicatifs qui seront modifiÃ©es par la suite
	static int nbFrags = 10;
	static int nbWorkers = 5;

	public void startJob(MapReduce mr) {
		// Initialisation des fichiers entree et sortie des workers
		ArrayList<Format> reader, writer;

		// initialisation du callback
		CallBackImpl cb = null;
		try {
			// On instancie le callback avec nbFrags qui sera determinÃ© grace aux methodes
			// definies par le groupe ayant pris le lot A
			cb = new CallBackImpl(nbFrags);

		} catch (Exception e) {
			e.printStackTrace();
		}

		// Initialisation des workers
		// Tout d'abord on instancie un tableau de "Worker" constituÃ© de nbWorkers
		// "Worker"
		Worker[] Workers = new WorkerImpl[nbWorkers];
		for (int i = 0; i < nbWorkers; i++) {
			String url;
			try {
				url = "//" + InetAddress.getLocalHost().getHostName() + ":" + Project.ports[i] + "/Worker"
						+ Project.ids[i];
				// On fait appel Ã la RMI registry pour trouver nos workers
				Workers[i] = (Worker) Naming.lookup(url);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		// Affectation des taches aux workers
		// L'affectation se fait par le runmap dÃ©finit ci-dessous dans la boucle for
		// cependant on l'a mis en commentaire parce qu'on a pas encore les methodes de
		// l'autre groupe
		// qui prend le lot A pour pouvoir extraire reader et writer de chaque fragment

		// Connect to NameNode
		String ip_namenode = "localhost";
		NameNodeInterface namenode;
		try {
			namenode = (NameNodeInterface) Naming.lookup("//" + ip_namenode + ":2021/NameNode");

			// Check if file doesn't exist.
			if (!namenode.CheckFile(inputfname)) {
				HdfsClient.HdfsWrite(inputformat, inputfname, 1);
			}
		



		// Recuprer les serveurs hdfs où sont stockés les fragments de fichiers.
		INode fichier_inode = namenode.getNode(inputfname);

		nbFrags = fichier_inode.getNumberOfChunks();

		reader = new ArrayList<Format>();
		writer = new ArrayList<Format>();

		// for (int j=0; j < nbFrags; j++) {
		int j = 0;
		for (java.util.Map.Entry<Integer, HDFSServer> s : fichier_inode.getChunksBlocsID().entrySet()) {

			Format readerTEMP, writerTEMP;
			// Le nom du fragment qu'on doit récupérer du serveur HDFS correspondant.
			String readerName = inputfname + s.getKey();

			String WriterName = inputfname + "out" + s.getKey();

			// Format KV
			if (inputformat == Format.Type.KV) {
				// un fichier où on stocke le fragment du fichier donné en param.
				readerTEMP = new KVFormat(readerName);
				writerTEMP = new KVFormat(WriterName);
			}
			// Format LINE
			else {
				readerTEMP = new LineFormat(readerName);
				writerTEMP = new LineFormat(WriterName);
			}

			// Connecter au serveur correspondant
			Request r = new Request(s.getValue(), readerName, 4040, Request.typeCMD.READ, fichier_inode.getType());
			r.run();
			ObjectInputStream part_received_stream = r.getReadingStream();
			KV read = (KV) part_received_stream.readObject();
			while (read != null) {
				readerTEMP.write(read);
				read = (KV) part_received_stream.readObject();
			}

			reader.add(readerTEMP);
			writer.add(writerTEMP);

			try {
				Workers[j % nbWorkers].runMap(mr, reader.get(j), writer.get(j), cb);
				// On bloque la semaphore et quand toutes les MAPs terminent leurs executions
				// ils appellent le callback
				// qui fait un release dans CallBackImpl et qui le debloque et Ã§a sera comme
				// Ã§a qu'on signal la fin de l'execution
				// des maps

				cb.getVerrou().acquire();
			} catch (Exception e) {
				e.printStackTrace();
			}
			// Incr
			j++;

		} 

		// Lancement du Reduce qui copie dans son FS local le fichier
		// résultat des Map (composé de fragments), grâce à HdfsClient qui contacte les
		// HdfsServer

		// Initialisation du fichier d'entree du reduce
		// Format InReduce = new KVFormat("Path + Nom fichier à l'entree TODO........ ")
		// ;
		File out = new File(outputfname);
		BufferedWriter bw = new BufferedWriter(new FileWriter(out));
		for (int i = 0; i < writer.size(); i++) {
			File outtemp = new File (outputfname+"out"+i);
			BufferedReader br = new BufferedReader(new FileReader(outtemp));
			String ligne = br.readLine();
			while (ligne != null) {
				bw.write(ligne);
				ligne = br.readLine();
			}
			bw.flush();
			br.close();
		}
		bw.close();
		Format InReduce;
		// Format KV
		if (outputformat == Format.Type.KV) {
			InReduce = new KVFormat(outputfname);
		}
		// Format LINE
		else {
			InReduce = new LineFormat(outputfname);
		}
		//Format InReduce = new KVFormat(outputfname)

		//Initialisation du fichier de sortie du reduce
		Format OutReduce = new KVFormat(outputfname+"-res") ;
		mr.reduce(InReduce, OutReduce);
			
		} catch (Exception e1) {
		}


		}
		
		


	@Override
	public void setNumberOfReduces(int tasks) {
		this.nbReduces = tasks;
		
	}

	@Override
	public void setNumberOfMaps(int tasks) {
		this.nbMaps = tasks;
		
	}

	@Override
	public void setOutputFormat(Type ft) {
		this.outputformat = ft;
		
	}

	@Override
	public void setOutputFname(String fname) {
		this.outputfname = fname;
		
	}

	@Override
	public void setSortComparator(SortComparator sc) {
		this.sortComparator = sc;
		
	}

	@Override
	public int getNumberOfReduces() {
		return this.nbReduces;
	}

	@Override
	public int getNumberOfMaps() {
		return this.nbMaps;
	}

	@Override
	public Type getInputFormat() {
		return this.inputformat;
	}

	@Override
	public Type getOutputFormat() {
		return this.outputformat;
	}

	@Override
	public String getInputFname() {
		return this.inputfname;
	}

	@Override
	public String getOutputFname() {
		return this.outputfname;
	}

	@Override
	public SortComparator getSortComparator() {
		return this.sortComparator;
	}
	


}