package ordo;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.Semaphore;

public interface CallBack extends Remote {
	
	// declarer qu'un worker a terminer son traitement map
	public void mapFinished() throws RemoteException;
	public Semaphore getVerrou() throws RemoteException;

	
}
