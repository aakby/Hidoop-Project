package hdfs;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class NameNode  extends UnicastRemoteObject implements Runnable, NameNodeInterface{

    
    // Attributs
    private static final long serialVersionUID = 1L;
    private ArrayList<HDFSServer> HDFSServers;
    private ArrayList<INode> INodeList;
    private static Integer Port = 2020;
    private static String Name = "localhost";

    // Constructor
    public NameNode() throws java.rmi.RemoteException {
        HDFSServers = new ArrayList<HDFSServer>();
        INodeList = new ArrayList<INode>();
    }

    // Check if a file exists.
    public Boolean CheckFile(String name) throws java.rmi.RemoteException {
        for (int i = 0; i < INodeList.size(); i++) {
            if (INodeList.get(i).getFileName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    // Add Node to the INodeList.
    public void addNode(INode node) throws java.rmi.RemoteException {
        INodeList.add(node);
    }

    // Get Node from INodeList.
    // returns null if it doesn't exist.
    public INode getNode(String name) throws java.rmi.RemoteException {
        for (int i = 0; i < INodeList.size(); i++) {
            if (INodeList.get(i).getFileName().equals(name)) {
                return INodeList.get(i);
            }
        }
        return null;
    }

    // Add Server to HDFSServers.
    public void addServer(HDFSServer server) throws java.rmi.RemoteException {
        HDFSServers.add(server);
    }

    // Get HDFS Servers.
    public ArrayList<HDFSServer> getHDFSServers() throws java.rmi.RemoteException {
        return HDFSServers;
    }

    // Delete Node from INodeList (Already exists).
    public void deleteNode(INode node) throws java.rmi.RemoteException {
        int index = -1;
        for (int i = 0; i < INodeList.size(); i++) {
            if (node.getFileName().equals(INodeList.get(i).getFileName())){
                index = i;
                break;
            }
        }
        INodeList.remove(index);
    }

    // Run Thread.
    public void run() {
        Socket s;
        ServerSocket ss;
        try {
            System.out.println("Setup NameNode ...");
            ss = new ServerSocket(Port);
            System.out.println("Listening for new Requests ... ");
            while (true) {
                // Accept Connections
                s = ss.accept();
                // Launch a new thread where to execute the command.
                NameNodeRequest nnr = new NameNodeRequest(s, HDFSServers);
                nnr.run();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getName() throws java.rmi.RemoteException {
        return Name;
    }

    public Integer getPort() throws java.rmi.RemoteException {
        return Port;
    }
}
