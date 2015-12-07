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