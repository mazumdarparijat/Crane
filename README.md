# Stream Processing System (Crane)
Crane is a stream processing system that is faster than Apache Storm.
Crane is similar to Apache Storm in that it uses tuples, spouts, bolts, sinks, and
topologies. The major difference between Crance and Storm is that Crane topologies are trees. Crane bolts
support the main types of functionalities supported by Storm bolts,
namely: filter, transforms, join of a stream with a static database (database could
just be a file). Further, Crane is fault-tolerant: with up to two simultaneous failures of
machines, i.e. no tuples are lost.

Crane provides a very generic framework to write and launch topologies similar to Storm. 
We provide storm topologies and crane topologies for three real case applications in the repository and can be
found at ```mp4-crane/src/main/java/cs425/mp4/storm/Apps``` and ```mp4-crane/src/main/java/cs425/mp4/crane/Apps``` respectively.

Note: We assume that Nimbus used in Crane doesnot fail

## Design

## Package Dependencies
- Java 7
- Maven

## Instructions
### Step 1 - Set up code in VM's
The following are the steps to get code and compile in application in a VM:
1. ssh into the vm machine : Eg - ```ssh <NETID>@fa15-cs425-gNN-XX.cs.illinois.edu```
2. Type ```git clone https://gitlab-beta.engr.illinois.edu/cs425-agupta80-pmazmdr2/mp4-crane.git```
3. cd into the project root directory
4. run ```mvn package```. You should see build success.

### Step 2 - Run Nimbus
Default scripts assume Nimbus to run at VM-1
1. ssh into the vm machine 1 : ```ssh <NETID>@fa15-cs425-gNN-01.cs.illinois.edu```
2. cd into the project root directory
3. run ```scripts/crane/run_nimbus.sh > ~/log.txt``` 

### Step 3 - Run Worker
You can run any number of Workers you want in the group
1. ssh into the vm machine: ```ssh <NETID>@fa15-cs425-gNN-XX.cs.illinois.edu```
2. cd into the project root directory
3. run ```scripts/crane/run_worker.sh > ~/log.txt```

### Step 4 - Run Topology
You can write your own Topology based on Crane's framework.
We provide 3 crane Topologies in the repository and a script to launch FlagPostRedditTopology for reference.
To launch FlagPostRedditTopology follow the steps below after doing Step1-3
1. ssh into any vm machine: ```ssh <NETID>@fa15-cs425-gNN-XX.cs.illinois.edu```
2. cd into the project root directory
3. run ```scripts/crane/run_FlagPostRedditTopology.sh > ~/log.txt```