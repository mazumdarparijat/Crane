package cs425.mp4.crane;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Map;

/**
 * Created by parijatmazumdar on 04/12/15.
 */
public class AckerThread extends Thread {
    private static final int BYTE_LEN=1024;
    private final DatagramSocket sock;
    Map<Long,Integer> ackRecords;
    public AckerThread(int ackPort,Map<Long,Integer> ackRecords) throws IOException {
        sock=new DatagramSocket(ackPort);
        this.ackRecords=ackRecords;
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
//                System.out.println("[ACKER] tuple_id : " + data.tupleID + ", tuple : " + data.val + ", numAcks : " + numAcks);
                ackRecords.put(data.tupleID,ackRecords.get(data.tupleID)+numAcks);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
