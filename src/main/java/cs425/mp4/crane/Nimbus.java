package cs425.mp4.crane;

import cs425.mp3.FailureDetector.FailureDetector;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import cs425.mp3.Pid;
import cs425.mp4.crane.Exceptions.UnhandledCaseException;
import cs425.mp4.crane.Messages.AcceptTaskMessage;
import cs425.mp4.crane.Messages.AcceptTopologyMessage;
import cs425.mp4.crane.Messages.Message;
import cs425.mp4.crane.Messages.UpdateTaskList;
import cs425.mp4.crane.Topology.Bolt;
import cs425.mp4.crane.Topology.Spout;
import cs425.mp4.crane.Topology.Topology;
import cs425.mp4.crane.Topology.TopologyRecord;

/**
 * Created by parijatmazumdar on 01/12/15.
 */
public class Nimbus extends Thread {
    private static final int NIMBUS_TIMEOUT=100;
    private static final int CRANE_PORT=9500;
    private static final int SPOUT_PORT=9501;
    private final FailureDetector failureDetector;
    private final ServerSocket sock;
    private HashMap<String,ArrayList<String>> workerID2Tasks;
    private HashMap<String,TaskAddress> task2Address;
    private HashMap<String,Task> id2Task;
    private AtomicBoolean spoutEmitTuples;
    public Nimbus(FailureDetector failureDetector) throws IOException {
        this.failureDetector = failureDetector;
        sock=new ServerSocket(CRANE_PORT);
        sock.setSoTimeout(NIMBUS_TIMEOUT);
        workerID2Tasks=new HashMap<String, ArrayList<String>>();
        task2Address=new HashMap<String, TaskAddress>();
        id2Task=new HashMap<String, Task>();
        spoutEmitTuples =new AtomicBoolean(false);
    }

    public void run() {
        while(true) {
            taskRedistribution();
            try {
                Socket reqSock=sock.accept();
                ObjectInputStream is=new ObjectInputStream(reqSock.getInputStream());
                ObjectOutputStream os=new ObjectOutputStream(reqSock.getOutputStream());
                handleMessage(is,os);
                reqSock.close();
            } catch (SocketTimeoutException e) {
                System.out.println("[NIMBUS] Socket timeout");
            } catch (IOException e) {
                System.out.println("[NIMBUS] Unknown IOException");
                e.printStackTrace();
            }
        }
    }

    private void taskRedistribution() {
        boolean redistributionDone=false;
        for (String workerID : workerID2Tasks.keySet()) {
            if (!failureDetector.isAlive(workerID)) {
                redistributionDone=true;
                Random rn=new Random();
                List<String> availableWorkers=failureDetector.getMemlistSkipIntroducer();
                for (String taskID : workerID2Tasks.get(workerID)) {
                    String newWorker=availableWorkers.get(rn.nextInt(availableWorkers.size()));
                    int newPort=communicateLaunch(newWorker,id2Task.get(taskID).b,id2Task.get(taskID).fd);
                    task2Address.put(taskID,new TaskAddress(Pid.getPid(newWorker).hostname,newPort));
                    if (!workerID2Tasks.containsKey(newWorker))
                        workerID2Tasks.put(newWorker,new ArrayList<String>());

                    workerID2Tasks.get(newWorker).add(taskID);
                }

                workerID2Tasks.remove(workerID);
            }
        }

        if (redistributionDone)
            updateTaskLists();
    }

    private void updateTaskLists() {
        for (String workerid : workerID2Tasks.keySet()) {
            try {
                Socket sock=new Socket(Pid.getPid(workerid).hostname,CRANE_PORT);
                ObjectOutputStream os=new ObjectOutputStream(sock.getOutputStream());
                os.writeObject(new UpdateTaskList());
                os.writeObject(task2Address);
                os.flush();
                sock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleMessage(ObjectInputStream is, ObjectOutputStream os) {
        try {
            Message recd=(Message) is.readObject();
            if (recd.getMessageType().equals(AcceptTopologyMessage.messageType)) {
                Topology recdTopology=(Topology) is.readObject();
                workerID2Tasks=new HashMap<String, ArrayList<String>>();
                task2Address=new HashMap<String, TaskAddress>();
                spoutEmitTuples.set(false);
                distributeTasks(recdTopology);
                updateTaskLists();
                spoutEmitTuples.set(true);
            } else {
                throw new UnhandledCaseException("Message unknown in Nimbus");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (UnhandledCaseException e) {
            e.printStackTrace();
        }
    }

    private void distributeTasks(Topology recdTopology) throws UnhandledCaseException {
        HashMap<String,ArrayList<String>> children=new HashMap<String, ArrayList<String>>();
        HashMap<String,TopologyRecord> idToRecord=new HashMap<String, TopologyRecord>();
        for (TopologyRecord tr : recdTopology.getRecords()) {
            idToRecord.put(tr.id,tr);
            if (children.containsKey(tr.dependency))
                children.get(tr.dependency).add(tr.id);
            else {
                children.put(tr.dependency,new ArrayList<String>());
                children.get(tr.dependency).add(tr.id);
            }
        }

        for (TopologyRecord tr : recdTopology.getRecords()) {
            Forwarder fd=new Forwarder();
            for (String child : children.get(tr.id)) {
                fd.addChild(child,idToRecord.get(child).numTasks,idToRecord.get(child).groupingField);
            }

            if (tr.type==TopologyRecord.spoutType) {
                SpoutTask st=new SpoutTask((Spout) tr.operationUnit,fd,task2Address,spoutEmitTuples,SPOUT_PORT);
                st.setDaemon(true);
                st.start();
            } else if (tr.type==TopologyRecord.boltType) {
                launchTasks(tr,fd);
            } else {
                throw new UnhandledCaseException("type not recognized");
            }
        }
    }

    private void launchTasks(TopologyRecord tr, Forwarder fd) {
        List<String> workers=failureDetector.getMemlistSkipIntroducer();
        Random rn=new Random();
        for (int i=0;i<tr.numTasks;i++) {
            String workerID=workers.get(rn.nextInt(workers.size()));
            String taskID=tr.id+String.valueOf(i);
            int launchedPort=communicateLaunch(workerID,(Bolt) tr.operationUnit,fd);
            task2Address.put(taskID,new TaskAddress(Pid.getPid(workerID).hostname,launchedPort));
            id2Task.put(taskID,new Task((Bolt) tr.operationUnit,fd));
            if (!workerID2Tasks.containsKey(workerID))
                workerID2Tasks.put(workerID,new ArrayList<String>());

            workerID2Tasks.get(workerID).add(taskID);
        }
    }

    private int communicateLaunch(String workerID, Bolt operationUnit, Forwarder fd) {
        try {
            Socket sock=new Socket(Pid.getPid(workerID).hostname,CRANE_PORT);
            ObjectOutputStream os=new ObjectOutputStream(sock.getOutputStream());
            os.writeObject(new AcceptTaskMessage());
            os.writeObject((Bolt) operationUnit);
            os.writeObject(fd);
            os.flush();

            ObjectInputStream is=new ObjectInputStream(sock.getInputStream());
            Integer portReplied=(Integer) is.readObject();
            sock.close();
            return portReplied;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return -1;
        }
    }
}
