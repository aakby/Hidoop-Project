package hdfs;

import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;

public class NameNodeRequest extends Thread {

    // Attributs
    private Socket s;
    private ArrayList<HDFSServer> HDFSServers;

    public NameNodeRequest(Socket vs, ArrayList<HDFSServer> vHDFSServers) {
        s = vs;
        HDFSServers = vHDFSServers;
    }

    public void run() {
        try {
            ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
            String cmd = (String) ois.readObject();
            switch (cmd) {
                case "CONNECT":
                    HDFSServer server = (HDFSServer) ois.readObject();
                    HDFSServers.add(server);
                    System.out.println("Server "+server.getName()+" has connected successfully to NameNode");
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }



    }
    
}
