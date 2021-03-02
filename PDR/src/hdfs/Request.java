package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import formats.Format.OpenMode;

public class Request extends Thread {
	// Types of commands.
	public static enum typeCMD {
		DELETE, WRITE, READ
	};
	// Attributs.
	private Socket s;
	private String FileName;
	private ObjectInputStream ois;
	private typeCMD CMD;
	private Format.Type TypeFile;

	
	// Constructor of request (Delete,Read and Write)
	public Request(HDFSServer server, String name, int port, typeCMD type,Format.Type fmt)
			throws UnknownHostException, IOException {
		s = new Socket(server.getName(),port);
		FileName = name;
		CMD = type;
		TypeFile = fmt;
	}

	// Run Thread.
	public void run(){
		ObjectOutputStream oos;
		switch (CMD) {
			case DELETE:
				try {
					// Connecting and sending necessary Info.
					oos = new ObjectOutputStream(s.getOutputStream());
					oos.writeObject("DELETE");
					oos.writeObject(FileName);
					s.close();
					oos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				break;
			case WRITE:
				try {
					// Connecting and sending necessary Info.
					oos = new ObjectOutputStream(s.getOutputStream());
					oos.writeObject("WRITE");
					oos.writeObject(FileName);
					oos.writeObject(TypeFile.toString());
					
					Format file;
					
					switch(TypeFile.toString()){
						case "KV":
							file = new KVFormat(FileName);
							break;
						case "LINE":
							file = new LineFormat(FileName);
							break;
						default:
							file = null;
							System.out.println("Format Inconnue !ll");
							System.exit(1);
					}
					// Sending File to write in the server.
					KV file_parts;
					file.open(Format.OpenMode.R);
					file_parts = file.read();
					while (file_parts != null){
						oos.writeObject(file_parts);
						oos.reset();
						file_parts = file.read();
					}
					oos.writeObject(file_parts);
					file.close();
					oos.close();
					s.close();

				} catch (IOException e){
					e.printStackTrace();
				}
				
				break;
			case READ:
				try {
					// Connecting and sending necessary Info.
					oos = new ObjectOutputStream(s.getOutputStream());
					oos.writeObject("READ");
					oos.writeObject(FileName);
					oos.writeObject(TypeFile.toString());

					// Save ObjectInputStream to get File parts from our HdfsClient.
					ois = new ObjectInputStream(s.getInputStream());
				} catch (IOException e){
					e.printStackTrace();
				}
				break;
			default:
				break;
		}
	}



	public ObjectInputStream getReadingStream() {
		return ois;
	}

}
