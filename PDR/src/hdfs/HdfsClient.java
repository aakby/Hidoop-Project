/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import formats.Format.OpenMode;
public class HdfsClient {

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file (Name of file)> <file (Name of file where to save)>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file (Name of file)>");
        System.out.println("Usage: java HdfsClient delete <file (Name of file)>");
    }

    public static void HdfsDelete(String hdfsFname) {
        try {
            // Connect to NameNode.
            NameNodeInterface namenode = (NameNodeInterface) Naming.lookup("//localhost:2021/NameNode");

            // Check if file exists.
            if (!namenode.CheckFile(hdfsFname)){
                System.out.println("Le fichier "+hdfsFname+" n'existe pas !");
                System.exit(1);
            }
            // Get INode of our file.
            INode file_inode = namenode.getNode(hdfsFname);

            // Get Servers where the parts of our file are saved.
            HashMap<Integer,HDFSServer> file_servers = file_inode.getChunksBlocsID();

            // Launch requests to delete the parts of file from servers.
            for (Entry<Integer, HDFSServer> s : file_servers.entrySet()){
                String name = hdfsFname + s.getKey();
                Request r = new Request(s.getValue(),name,4040,Request.typeCMD.DELETE,file_inode.getType());
                r.run();
            }

            // Delete the file from INode.
            namenode.deleteNode(file_inode);
            
            System.out.println("Le fichier "+hdfsFname+" a été supprimé.");
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor) {
        try {
            // Connect to NameNode.
            NameNodeInterface namenode = (NameNodeInterface) Naming.lookup("//localhost:2021/NameNode");

            // Check if file doesn't exist.
            if (namenode.CheckFile(localFSSourceFname)){
                System.out.println("Le fichier "+localFSSourceFname+" existe déja !");
                System.exit(1);
            }

            // Get HDFSServers.
            ArrayList<HDFSServer> servers = namenode.getHDFSServers();


            // Create the INode of our file.
            File file = new File(localFSSourceFname);
            Long file_size = file.length();
            Integer  chunks_number = servers.size();
            HashMap<Integer,HDFSServer> chunks_server = new HashMap<Integer,HDFSServer>();

            INode file_inode = new INode(localFSSourceFname,file_size,chunks_number,chunks_server,fmt);

            // Divide the file locally  and add Chunk to ChunkList.
            Long chunk_size = file_size/chunks_number;
            int chunk_number_counter = 0;
            FileReader fr = new FileReader(localFSSourceFname);
            BufferedReader br = new BufferedReader(fr);
            FileWriter fw = new FileWriter(localFSSourceFname+chunk_number_counter,true);
            BufferedWriter bw = new BufferedWriter(fw);
            String read = br.readLine();
            int file_size_counter = 0;
            file_inode.addChunk(chunk_number_counter,servers.get(chunk_number_counter));
            while (read != null){
                if (file_size_counter + read.getBytes().length > chunk_size){
                    bw.close();
                    fw.close();
                    chunk_number_counter++;
                    file_inode.addChunk(chunk_number_counter,servers.get(chunk_number_counter));
                    fw = new FileWriter(localFSSourceFname+chunk_number_counter,true);
                    bw = new BufferedWriter(fw);
                    bw.write(read);
                    bw.newLine();
                    file_size_counter = read.getBytes().length;

                } else {
                    bw.write(read);
                    bw.newLine();
                    file_size_counter += read.getBytes().length;
                }
                read = br.readLine();
            }
            bw.close();
            br.close();

            // Send requests to write the parts into HDFSServers.
            for (Entry<Integer, HDFSServer> s : file_inode.getChunksBlocsID().entrySet()){
                
                String name = localFSSourceFname + s.getKey();
                System.out.println("Envoi au serveur "+s.getValue().getName()+" pour l'ecriture du fichier "+name);
                Request r = new Request(s.getValue(),name,4040,Request.typeCMD.WRITE,fmt);
                r.run();
            }

            // Add INode to INode List.
            namenode.addNode(file_inode);

            // Delete parts of file created locally.
            for (int i = 0; i <= chunk_number_counter; i++ ){
                File file_to_delete = new File(localFSSourceFname+i);
                file_to_delete.delete();
            }
            System.out.println("Le fichier "+localFSSourceFname+" a été ajouté.");
        } catch (Exception e){
            e.printStackTrace();
        }




      }

    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
        try {
            // Connect to NameNode.
            NameNodeInterface namenode = (NameNodeInterface) Naming.lookup("//localhost:2021/NameNode");

            // Check if file exists
            if (!namenode.CheckFile(hdfsFname)){
                System.out.println("Le fichier "+hdfsFname+" n'existe pas !");
                System.exit(1);
            }

            // Get the INode of our file.
            INode file_inode = namenode.getNode(hdfsFname);

            // Create file where to save our file.
            Format file = null;
            switch (file_inode.getType().toString()){
                case "KV" : 
                    file = new KVFormat(localFSDestFname);
                    break;
                case "LINE":
                    file = new LineFormat(localFSDestFname);
                    break;
                default :
                    System.out.println("Format Inconnue !");
                    System.exit(1);
            }
            file.open(OpenMode.W);

            // Send Requests to get File parts and write them in our file.
            KV read;
            for (Entry<Integer,HDFSServer> s : file_inode.getChunksBlocsID().entrySet()){
                String name = hdfsFname + s.getKey();
                Request r = new Request(s.getValue(),name,4040,Request.typeCMD.READ,file_inode.getType());
                r.run();
                ObjectInputStream part_received_stream = r.getReadingStream();
                read = (KV) part_received_stream.readObject();
                while (read != null){
                    file.write(read);
                    read = (KV) part_received_stream.readObject();
                }
            }
            file.close();
            



     } catch (Exception e){
         e.printStackTrace();
     }
    }
	
    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read": HdfsRead(args[1],args[2]); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
