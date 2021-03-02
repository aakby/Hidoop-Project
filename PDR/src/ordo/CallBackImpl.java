package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {
	// On initialise une semaphore pour le callback 
	private Semaphore s = new Semaphore(0);
	// le nombre de fragments 
	int nbFrag;
	// Un compteur qui s'incrementera à chaque fois qu'un fragment est terminé
	int compteur = 0;

	public CallBackImpl(int nbFrag) throws RemoteException {
		this.s = s;
		this.nbFrag = nbFrag;
	}
	
	public Semaphore getVerrou() {
		
		return this.s;
		
	}

	@Override
	public void mapFinished() throws RemoteException {
		compteur++;
		// Si tous les fragments se terminent on fait un release pour debloquer le semaphore
		// qui a ete bloqué dans le joblauncher
		if (compteur == nbFrag) {
			s.release();
		}
	}

}
