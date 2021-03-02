package hdfs;

import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class HDFSServer implements Runnable, Serializable {

	
	// Attributs
	private static final long serialVersionUID = 1L;
	private String Name;
	private Integer Port = 4040;

	public String getName() {
		return Name;
	}
	public HDFSServer(){
		InetAddress ip;
		try {
			ip = InetAddress.getLocalHost();
			Name = ip.getHostAddress();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void run() {
		System.out.println("Server has started ...");
		System.out.println("Connecting to NameNode ...");
		// Connect to the NameNode.
		try {
			Socket s = new Socket("localhost", 2020);
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			oos.writeObject("CONNECT");
			oos.writeObject(this);
			oos.close();
			s.close();
			System.out.println("Connection Succeeded");
		} catch (Exception e) {
			System.out.println("Connection failed");
			e.printStackTrace();
		}

		// Accept Requests and launch them in HandleDemand.
		ServerSocket ss;
		Socket s;
		try {
			System.out.println("Listening for new Demands ...");
			ss = new ServerSocket(Port);
			while (true){
				s = ss.accept();
				HandleDemand demand = new HandleDemand(s);
				demand.run();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		




	}

}
