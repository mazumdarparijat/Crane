package cs425;

import cs425.mp3.sdfsproxyMain;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Thread Class for processing user input in Nimbus
 *
 */
public class InputProcessorIntroThread extends Thread{
	/**Process user input
	 * @param line
	 */
	private void processUserCommand(String line) {
        if (line.equals("m")) {
        	System.err.println("MEMBERSHIP LIST");
        	List<String> memlist = NimbusMain.FD.getMemlistSkipIntroducer();
        	for(String member: memlist){
        		System.err.println(member);
        	}
        } else if (line.equals("i")) {
        	System.err.println("SELF ID");
        	System.err.println(NimbusMain.FD.getSelfID().toString());
        } else if (line.equals("l")) {
        	System.err.println("Leave Initiated");
        	NimbusMain.FD.leaveInitiate();
        } else if (line.equals("d")) {
			System.err.println("Task Distribution");
			NimbusMain.nb.printTaskDistribution();
		} else {
            System.err.println("argument not recognised. Press m to get membership list, i to get id, l to leave,d" +
					" to get task distribution");
        }
    }
	@Override
	public void run(){
		System.err.println("Ready to take arguments. Press m to get membership list, i to get id, l to leave and "
            		+ "d to get task distribution");
		BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
		while(true){
	        try {
				String line=br.readLine();
				processUserCommand(line);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
	}
}
