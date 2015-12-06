package cs425;

import cs425.mp3.FailureDetector.FDIntroducer;
import cs425.mp3.FailureDetector.FailureDetector;
import cs425.mp3.FailureDetectorThread;
import cs425.mp4.crane.Nimbus;
import org.apache.commons.cli.*;

import java.io.IOException;

/**
 * Created by parijatmazumdar on 04/12/15.
 */
public class NimbusMain {
    private static int FDport;
    public static FailureDetector FD;
    public static Nimbus nb;
    /**
     * Formats commandLine inputs and flags
     */
    private static void FormatCommandLineInputs(String [] args) {
        Options op=createOptions();
        CommandLineParser parser=new DefaultParser();
        CommandLine line=null;
        try {
            line=parser.parse(op,args);
        } catch (ParseException e) {
            printHelp(op);
            e.printStackTrace();
        }
        FDport = Integer.parseInt(line.getOptionValue("port"));
    }
    /** Creates the required options to look for in command line arguments
     * @return Options object
     */
    private static Options createOptions() {
        Option port = Option.builder("port").argName("serverPort").hasArg().desc("Port to run faliure detector server")
                .required().build();
        Options op=new Options();
        op.addOption(port);
        return op;
    }

    /** print helper for usage
     * @param op options
     */
    private static void printHelp(Options op) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("failureDetector", op);
    }

    /**Setup Failure Detector and Master Tracker
     * @return
     * @throws IOException
     */
    public static void setupServices() throws IOException {
        FD=new FDIntroducer(FDport);
    }

    /**Main function for launching SDFSProxy
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String [] args) throws IOException, InterruptedException {
        FormatCommandLineInputs(args);
        setupServices();
        //Start Failure Detector
        FailureDetectorThread FDThread = new FailureDetectorThread(FD);
        FDThread.setDaemon(true);
        FDThread.start();

        Thread.sleep(1000);

        nb=new Nimbus(FD);
        nb.setDaemon(true);
        nb.start();

        //Start User input Processor
        InputProcessorIntroThread InputThread = new InputProcessorIntroThread();
        InputThread.setDaemon(true);
        InputThread.start();

        FDThread.join();
        nb.join();
    }
}
