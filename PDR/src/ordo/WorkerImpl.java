package ordo;

import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import formats.Format;
import map.Mapper;

public class WorkerImpl extends UnicastRemoteObject implements Worker {
	
	// id du Worker
	private int id;
	// Le constructeur
	protected WorkerImpl(int id) throws RemoteException {
		this.id = id;
	}

	// La methode qui va run le map
	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		// creer un thread secondaire qui execute le map
		mapRunner t = new mapRunner(m, reader, writer, cb);
		// on va faire t.start qui fera appel à la methode run de mapRunner en dessous 
		t.start();	
	}

	public static void main(String args[]) {
		if (args.length < 2) {
			System.out.println("java WorkerImpl <id> <port>");
		} else {
			try {
				// On récupere l'id du Worker
				int id = Integer.parseInt(args[0]);
				// On récupere le port
				int port = Integer.parseInt(args[1]);
				// On crée une registre pour le port ci-dessus
				Registry r = LocateRegistry.createRegistry(port);
				// On determine l'url correspondant au worker determiné avec son id
				String url = "//" + InetAddress.getLocalHost().getHostName() + ":" + port + "/Worker" + id;
				// On instancie le Worker
				Worker serverWorker = new WorkerImpl(id);
				// On met cette méthode à disposition via son url grace à rebind
				Naming.rebind(url, serverWorker);
				System.out.println("Worker numero " + id + " à " + url);
			} catch (Exception e) {
				System.out.println("Worker error");
				e.printStackTrace();
			}
		}

	}

	
	// Recuperer l'id d'un Worker
	public int getid() {
		return id;
	}

	// Modifier l'id d'un worker
	public void setid(int id) {
		this.id = id;
	}


}

// Une classe qui s'occupera d'executer les Worker
class mapRunner extends Thread  {
	Mapper m;
	Format reader;
	Format writer;
	CallBack cb;

	// Le constructeur
	public mapRunner(Mapper m, Format reader, Format writer, CallBack cb) {
		this.m = m;
		this.reader = reader;
		this.writer = writer;
		this.cb = cb;
	}

	// La procedure run qui permettera de faire un start pour commencer l'execution
	@Override
	public void run() {
		try {
			// On ouvre le fichier reader 
			reader.open(Format.OpenMode.R);
			// On ouvre le fichier writer
			writer.open(Format.OpenMode.W);
			// On commence le map
			m.map(reader, writer);
			// Quand l'execution est terminée on appelle un CallBack
			cb.mapFinished();
			// On ferme le fichier reader
			reader.close();
			// On ferme le fichier writer
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}