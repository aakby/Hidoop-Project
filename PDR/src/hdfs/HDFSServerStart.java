package hdfs;

public class HDFSServerStart {
    // Attributs
    static HDFSServer server;

    public static void main(String[] args){
        // Create and run the server.
        server = new HDFSServer();
        server.run();
    }
}