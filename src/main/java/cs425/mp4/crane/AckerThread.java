package cs425.mp4.crane;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Map;

/**
 * Created by parijatmazumdar on 04/12/15.
 */
public class AckerThread extends Thread {
    private static final int BYTE_LEN=10000;
    private final DatagramSocket sock;
    Map<Long,Integer> ackRecords;
    private PrintWriter pr;
    public AckerThread(int ackPort,Map<Long,Integer> ackRecords) throws IOException {
        sock=new DatagramSocket(ackPort);
        this.ackRecords=ackRecords;
        pr=new PrintWriter(System.getProperty("user.home")+"/results.txt","utf-8");
    }

    @Override
    public void run() {
        byte [] receiveData=new byte[BYTE_LEN];
        while(true) {
            try {
                DatagramPacket pack=new DatagramPacket(receiveData,receiveData.length);
                sock.receive(pack);
                ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(receiveData));
                CraneData data = (CraneData) is.readObject();
                Integer numAcks= (Integer) is.readObject();
                is.close();
                if (data.val!=null) {
                    pr.println(System.currentTimeMillis() + "," + data.tupleID + "," + data.val);
                    pr.flush();
                }
                System.err.println("[ACKER] "+System.currentTimeMillis()+" "+"tuple_id : " + data.tupleID);
                ackRecords.put(data.tupleID, ackRecords.get(data.tupleID) + numAcks);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
