package cs425.mp4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Server to collect output of Storm topology and write to a file
 */
public class DashBoard {
	final static int PORT=8888;
	@SuppressWarnings("resource")
	public static void main(String [] args) {
		try{
			ServerSocket welcomeSocket=new ServerSocket(PORT);
			while(true){
				Socket connectionSocket = welcomeSocket.accept();
				BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
				String msg=inFromClient.readLine();
				System.out.println("["+System.currentTimeMillis()+"],"+msg);
				connectionSocket.close();
			}
		}catch(IOException e){
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
