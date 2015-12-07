package cs425.mp4.crane;

import cs425.mp4.crane.Topology.Spout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by parijatmazumdar on 03/12/15.
 */
public class SpoutTask extends Thread {
    private class QueueData {
        public final CraneData data;
        public final long expiryTime;
        public QueueData(CraneData cd, long expiryTime) {
            this.data=cd;
            this.expiryTime=expiryTime;
        }
    }
    private final int BYTE_LEN=10000;
    private final long WAIT_TIME=2000;
    private final int QCAPACITY=1000;
    private final Spout sp;
    private final Forwarder fd;
    private final HashMap<String,TaskAddress> taskAddress;
    private final AtomicBoolean emitNext;
    private final int ackPort;
    private long tupleID;
    private Queue<QueueData> unAcked;
    private Map<Long,Integer> acks;
    public SpoutTask(Spout sp, Forwarder fd, HashMap<String, TaskAddress> taskAddress,
                     AtomicBoolean emitNext, int port) {
        this.sp = sp;
        this.fd = fd;
        this.taskAddress = taskAddress;
        this.emitNext = emitNext;
        ackPort=port;


        unAcked=new LinkedList<QueueData>();
        acks=new ConcurrentHashMap<Long,Integer>(10,(float) 0.75,2);
        try {
            new AckerThread(ackPort,acks).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        sp.open();
        tupleID=1;
        while(true) {
//            System.err.println("[SPOUT_TASK] Queue size "+unAcked.size());
//            manageUnacked();
            if (!emitNext.get()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                if (unAcked.size()==QCAPACITY) {
                    manageUnacked();
                }

                HashMap<String,String> outVal;
                long id;
                if (unAcked.peek()==null || unAcked.peek().expiryTime>System.currentTimeMillis()) {
                    if (unAcked.size()<QCAPACITY) {
                        outVal = sp.nextTuple();
                        if (outVal == null) {
                            System.err.println(unAcked.size());
                            continue;
                        }

                        id = tupleID++;
//                        System.err.println("[SPOUT_TASK] " + System.currentTimeMillis() + " Emit : " + id);
                    } else {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        continue;
                    }
                }
                else {
                    QueueData top=unAcked.poll();
                    outVal=top.data.val;
                    id=top.data.tupleID;
                    if (acks.get(id)>=fd.numAcks) {
                        acks.remove(id);
                        continue;
                    }
//                    System.err.println("[SPOUT_TASK] Re-Emit : " + id);
                }

//                System.err.println("[SPOUT_TASK] UNACKED SIZE : " + unAcked.size());
                CraneData out=new CraneData(id,outVal);
                unAcked.add(new QueueData(out, System.currentTimeMillis() + WAIT_TIME));
                acks.put(out.tupleID,0);

                for (String taskID : fd.getForwardTaskIDs(outVal)) {
                    forwardResult(out,taskAddress.get(taskID).hostname,
                            taskAddress.get(taskID).port);
                }
            }
        }
    }

    private void manageUnacked() {
        Iterator<QueueData> it=unAcked.iterator();
        while (it.hasNext()) {
            QueueData d=it.next();
            if (acks.get(d.data.tupleID)>=fd.numAcks)
                it.remove();
        }
    }

    private void forwardResult(CraneData data, String hostname, int port) {
        try {
            ByteArrayOutputStream bo=new ByteArrayOutputStream(BYTE_LEN);
            ObjectOutputStream os=new ObjectOutputStream(bo);
            os.writeObject(data);
            os.flush();
            byte [] sendBytes=bo.toByteArray();
            DatagramPacket dp=new DatagramPacket(sendBytes,sendBytes.length, InetAddress.getByName(hostname),port);
            DatagramSocket dSock=new DatagramSocket();
            dSock.send(dp);
            os.close();
            dSock.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
