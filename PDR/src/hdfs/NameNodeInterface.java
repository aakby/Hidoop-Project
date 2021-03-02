package hdfs;
import java.util.ArrayList;

public interface NameNodeInterface extends java.rmi.Remote {
    public Boolean CheckFile(String name) throws java.rmi.RemoteException;

    public void addNode(INode node) throws java.rmi.RemoteException;

    public INode getNode(String name) throws java.rmi.RemoteException;

    public void addServer(HDFSServer server) throws java.rmi.RemoteException;

    public ArrayList<HDFSServer> getHDFSServers() throws java.rmi.RemoteException;

    public void deleteNode(INode node) throws java.rmi.RemoteException;

    public String getName() throws java.rmi.RemoteException;

    public Integer getPort() throws java.rmi.RemoteException;
}
