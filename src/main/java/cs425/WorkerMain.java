package cs425;

import cs425.mp3.ElectionService.ElectionService;
import cs425.mp3.ElectionServiceThread;
import cs425.mp3.FailureDetector.FailureDetector;
import cs425.mp3.FailureDetectorThread;
import cs425.mp3.FileServer.FileServer;
import cs425.mp3.InputProcessorThread;
import cs425.mp3.MasterService.MasterService;
import cs425.mp3.MasterService.MasterServiceThread;
import cs425.mp4.crane.Worker;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.util.List;

/**
 * Created by parijatmazumdar on 04/12/15.
 */
public class WorkerMain {
    private static int FDport=0;
    public static int intro_port=0;
    public static String intro_address="";
    public static FailureDetector FD;
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
        intro_address=line.getOptionValues("i")[0];
        intro_port=Integer.parseInt(line.getOptionValues("i")[1]);
    }
    /** Creates the required options to look for in command line arguments
     * @return Options object
     */
    private static Options createOptions() {
        Option port = Option.builder("port").argName("serverPort").hasArg().desc("Port to run faliure detector server")
                .required().build();
        Option i = Option.builder("i").desc("Describes the address and port of introducer").numberOfArgs(2).required().build();
        Options op=new Options();
        op.addOption(port);
        op.addOption(i);
        return op;
    }

    /** print helper for usage
     * @param op options
     */
    private static void printHelp(Options op) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("failureDetector", op);
    }

    /**Setup Failure Detector
     * @return
     * @throws IOException
     */
    public static void setupServices() throws IOException {
        FD=new FailureDetector(FDport,intro_address,intro_port);
    }

    /**Main function for launching Worker
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

        // Start worker thread
        Thread.sleep(1000);
        Worker wk=new Worker();
        wk.setDaemon(true);
        wk.start();

        //Start User input Processor
        InputProcessorThread InputThread = new InputProcessorThread();
        InputThread.setDaemon(true);
        InputThread.start();

        //Wait for Failure Detector and worker
        FDThread.join();
        wk.join();
    }
}
