package cs425.mp4.crane;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Map;

/**
 * Thread class for receiving acks for completely processed tuples.
 * It Uses a UDP receiver socket to list to incoming acks and
 * does bookkeeping of these acks.
 */
public class AckerThread extends Thread {
    private static final int BYTE_LEN=10000;
    private final DatagramSocket sock;
    Map<Long,Integer> ackRecords;
    private PrintWriter pr;

    /**
     *
     * @param ackPort port in which to listen to
     * @param ackRecords bookkeeping data structure
     * @throws IOException
     */
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

                ackRecords.put(data.tupleID, ackRecords.get(data.tupleID) + numAcks);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
