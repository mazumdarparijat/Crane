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
import cs425.mp4.crane.Messages.UpdateTaskListMessage;
import cs425.mp4.crane.Topology.Bolt;

/**
 * Created by parijatmazumdar on 01/12/15.
 */
public class Worker extends Thread {
    private final ServerSocket socket;
    HashMap<String,TaskAddress> task2Address;
    private int freePort;

    public Worker() throws IOException {
        socket=new ServerSocket(Constants.WORKER_PORT);
        freePort=Constants.FREE_PORT_INIT;
        task2Address=new HashMap<String, TaskAddress>();
    }

    @Override
    public void interrupt() {

    }

    @Override
    public void run() {
        System.out.println("[Worker] Worker Started");
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
                String taskID=(String) is.readObject();
                Bolt recdBolt=(Bolt) is.readObject();
                Forwarder fd=(Forwarder) is.readObject();
                os.writeObject(freePort);
                os.flush();
                WorkerThread wt=new WorkerThread(taskID,recdBolt,fd,freePort++,this);
                wt.setDaemon(true);
                wt.start();
            } else if (m.getMessageType().equals(UpdateTaskListMessage.messageType)) {
                task2Address=(HashMap<String,TaskAddress>) is.readObject();
                System.out.println("[WORKER] task 2 address "+task2Address);
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

    public HashMap<String, TaskAddress> getTask2AddressMap() {
        return task2Address;
    }
}
