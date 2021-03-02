package hdfs;

import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import formats.Format.OpenMode;

public class HandleDemand {

    // Attributs.
    private Socket s;

    public HandleDemand(Socket vs) {
        s = vs;
    }

    public void run() {
        try {
            // Read the command.
            ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
            String CMD = (String) ois.readObject();
            String FileName;
            switch(CMD) {
                case "DELETE":
                    // Read FileName and delete file from server (The part of file saved in the server).
                    FileName = (String) ois.readObject();
                    System.out.println("Demande de suppression du fichier : "+FileName);
                    File file = new File(FileName);
                    file.delete();
                    System.out.println(FileName+" a été supprimé.");
                    break;
                case "READ":
                    // Read FileName and Type of file.
                    FileName = (String) ois.readObject();
                    System.out.println("Demande de lecture du fichier : "+FileName);
                    String TypeFile = (String) ois.readObject();

                    Format file_read = null;
                    // Choose the right Format to open the file to read.
                    switch (TypeFile){
                        case "KV":
                            file_read = new KVFormat(FileName);
                            break;
                        case "LINE":
                            file_read = new LineFormat(FileName);
                            break;
                        default:
                            System.out.println("Format inconnue !");
                            System.exit(1);     
                    };
                    file_read.open(OpenMode.R);

                    // Send the file.
                    ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
                    KV file_parts;
                    file_parts = file_read.read();
                    while (file_parts != null){
                        oos.writeObject(file_parts);
                        oos.reset();
                        file_parts = file_read.read();
                    }
                    oos.writeObject(file_parts);
                    file_read.close();
                    System.out.println(FileName+" a été envoyé au client.");
                    break;
                case "WRITE":
                    // Read FileName and Type of file.
                    FileName = (String) ois.readObject();
                    System.out.println("Demande d'écriture du fichier : "+FileName);
                    String TypeFileW = (String) ois.readObject();
                    Format file_write = null;

                    // Choose the right format to save the file.
                    switch (TypeFileW){
                        case "KV":
                            file_write = new KVFormat(FileName);
                            break;
                        case "LINE":
                            file_write = new LineFormat(FileName);
                            break;
                        default:
                            System.out.println("Format inconnue !");
                            System.exit(1);     
                    };
                    file_write.open(OpenMode.W);

                    // Collect the file.
                    KV file_parts_write = (KV) ois.readObject();
                    while (file_parts_write != null){
                        file_write.write(file_parts_write);
                        file_parts_write = (KV) ois.readObject();
                    }
                    file_write.close();
                    System.out.println(FileName+" a été enregistré");

                    break;
                default :
                    System.out.println("Opération Inconnue !");
                    System.exit(1);  
            }
            ois.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

}
