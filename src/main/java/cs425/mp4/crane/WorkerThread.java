package cs425.mp4.crane;

import cs425.mp4.crane.Topology.Bolt;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

/**
 * Created by parijatmazumdar on 03/12/15.
 */
public class WorkerThread extends Thread {
    private final Bolt bolt;
    private final Forwarder fd;
    private final HashMap<String,TaskAddress> taskAddress;
    private final ServerSocket socket;
    public WorkerThread(Bolt b, Forwarder fd, int port, HashMap<String,TaskAddress> task2Address) throws IOException {
        bolt=b;
        this.fd=fd;
        taskAddress=task2Address;
        socket=new ServerSocket(port);
        bolt.open();
    }

    @Override
    public void run() {
        try {
            Socket sock=socket.accept();
            ObjectInputStream is=new ObjectInputStream(sock.getInputStream());
            CraneData in=(CraneData) is.readObject();
            sock.close();

            in.val=bolt.execute(in.val);
            for (String taskID : fd.getForwardTaskIDs(in.val)) {
                forwardResult(in,taskAddress.get(taskID).hostname,taskAddress.get(taskID).port);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void forwardResult(CraneData in, String hostname, int port) {
        try {
            Socket sock=new Socket(hostname,port);
            ObjectOutputStream os=new ObjectOutputStream(sock.getOutputStream());
            os.writeObject(in);
            os.flush();
            sock.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
