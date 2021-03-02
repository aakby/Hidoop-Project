package hdfs;

import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
public class NameNodeStart {

    public static void main(String[] args) {
        try {
            // Create and run the NameNode.
            NameNode namenode = new NameNode();
            Registry registry = LocateRegistry.createRegistry(2021);
            Naming.rebind("//localhost:2021/NameNode",namenode);
            namenode.run();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
