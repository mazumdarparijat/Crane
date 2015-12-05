package cs425.mp4.crane;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

import cs425.mp4.crane.Exceptions.UnhandledCaseException;
import cs425.mp4.crane.Messages.AcceptTaskMessage;
import cs425.mp4.crane.Messages.Message;
import cs425.mp4.crane.Messages.UpdateTaskList;
import cs425.mp4.crane.Topology.Bolt;

/**
 * Created by parijatmazumdar on 01/12/15.
 */
public class Worker {
    private static final int CRANE_PORT=9500;
    private final ServerSocket socket;
    HashMap<String,TaskAddress> task2Address;
    private int freePort;
    public Worker() throws IOException {
        socket=new ServerSocket(CRANE_PORT);
        freePort=9600;
        task2Address=new HashMap<String, TaskAddress>();
    }
    public void run() {
        while (true) {
            try {
                Socket incom=socket.accept();
                ObjectInputStream is=new ObjectInputStream(incom.getInputStream());
                ObjectOutputStream os=new ObjectOutputStream(incom.getOutputStream());
                handleMessage(is,os);
                incom.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleMessage(ObjectInputStream is, ObjectOutputStream os) {
        try {
            Message m= (Message) is.readObject();
            if (m.getMessageType().equals(AcceptTaskMessage.messageType)) {
                Bolt recdBolt=(Bolt) is.readObject();
                Forwarder fd=(Forwarder) is.readObject();
                os.writeObject(freePort);
                os.flush();
                WorkerThread wt=new WorkerThread(recdBolt,fd,freePort++,task2Address);
                wt.setDaemon(true);
                wt.start();
            } else if (m.getMessageType().equals(UpdateTaskList.messageType)) {
                task2Address=(HashMap<String,TaskAddress>) is.readObject();
            } else {
                throw new UnhandledCaseException("Message not recognised");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (UnhandledCaseException e) {
            e.printStackTrace();
        }
    }
}
