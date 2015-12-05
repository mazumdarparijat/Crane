package cs425.mp4.crane;

import cs425.mp4.crane.Topology.Spout;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by parijatmazumdar on 03/12/15.
 */
public class SpoutTask extends Thread {
    private final Spout sp;
    private final Forwarder fd;
    private final HashMap<String,TaskAddress> taskAddress;
    private final AtomicBoolean emitNext;
    private final int ackPort;
    private long tupleID;
    public SpoutTask(Spout sp, Forwarder fd, HashMap<String, TaskAddress> taskAddress,
                     AtomicBoolean emitNext, int port) {
        this.sp = sp;
        this.fd = fd;
        this.taskAddress = taskAddress;
        this.emitNext = emitNext;
        ackPort=port;
    }

    @Override
    public void run() {
        sp.open();
        tupleID=0;
        while(true) {
            if (!emitNext.get()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                HashMap<String,String> out=sp.nextTuple();
                for (String taskID : fd.getForwardTaskIDs(out)) {
                    forwardResult(new CraneData(tupleID++,out),taskAddress.get(taskID).hostname,
                            taskAddress.get(taskID).port);
                }
            }
        }
    }

    private void forwardResult(CraneData data, String hostname, int port) {
        try {
            Socket sock=new Socket(hostname,port);
            ObjectOutputStream os=new ObjectOutputStream(sock.getOutputStream());
            os.writeObject(data);
            os.flush();
            sock.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
