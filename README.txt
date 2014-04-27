Replicated Key-Value Storage

Introduction

At this point, most of you are probably ready to understand and implement a Dynamo-style key-value storage; this assignment is about implementing a simplified version of Dynamo. (And you might argue that it’s not Dynamo any more ;-) There are three main pieces you need to implement: 1) Partitioning, 2) Replication, and 3) Failure handling.
The main goal is to provide both availability and linearizability at the same time. In other words, your implementation should always perform read and write operations successfully even under failures. At the same time, a read operation should always return the most recent value. To accomplish this goal, this document gives you a guideline of the implementation. However, you have freedom to come up with your own design as long as you provide availability and linearizability at the same time (that is, to the extent that the tester can test). The exception is partitioning and replication, which should be done exactly the way Dynamo does.
This document assumes that you are already familiar with Dynamo. If you are not, that is your first step. There are many similarities between this assignment and the previous assignment for the most basic functionalities, and you are free to reuse your code from the previous assignment.
References

Before we discuss the requirements of this assignment, here are two references for the Dynamo design:
Lecture slides on Dynamo: http://www.cse.buffalo.edu/~stevko/courses/cse486/spring13/lectures/29-dynamo.pptx
Dynamo paper: http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf
The lecture slides give an overview, but do not discuss Dynamo in detail, so it should be a good reference to get an overall idea. The paper presents the detail, so it should be a good reference for actual implementation.
Step 0: Importing the project template

Just like the previous assignment, we have a project template you can import to Eclipse.
Download the project template zip file to a directory.
Import it to your Eclipse workspace.
Open Eclipse.
Go to “File” -> “Import”
Select “General/Existing Projects into Workspace” (Caution: this is not “Android/Existing Android Code into Workspace”).
In the next screen (which should be “Import Projects”), do the following:
Choose “Select archive file:” and select the project template zip file that you downloaded.
Click “Finish.”
At this point, the project template should have been imported to your workspace.
You might get an error saying “Android requires compiler compliance level...” If so, right click on “SimpleDynamo” from the Package Explorer, choose “Android Tools” -> “Fix Project Properties” which will fix the error.
You might also get an error about android-support-v4.jar. If so, right click on “SimpleDynamo” from the Package Explorer, choose “Properties” -> “Java Build Path” -> “Libraries” and either fix the android-support-v4.jar’s path or replace it with your SDK’s correct android-support-v4.jar.
Try running it on an AVD and verify that it’s working.
Use the project template for implementing all the components for this assignment.
The template has the package name of “edu.buffalo.cse.cse486586.simpledynamo“. Please do not change this.
The template also defines a content provider authority and class. Please use it to implement your Dynamo functionalities.
We will use SHA-1 as our hash function to generate keys just as last time.
The template is very minimal for this assignment. However, you can reuse any code from your previous submissions.
You can add more to the main Activity in order to test your code. However, this is entirely optional and there is no grading component for your Activity.
Step 1: Writing the Content Provider

Just like the previous assignment, the content provider should implement all storage functionalities. For example, it should create server and client threads (if this is what you decide to implement), open sockets, and respond to incoming requests. When writing your system, you can make the following assumptions:
Just like the previous assignment, you need to support insert/query/delete operations. Also, you need to support @ and * queries.
There are always 5 nodes in the system. There is no need to implement adding/removing nodes from the system.
However, there can be at most 1 node failure at any given time. We will emulate a failure only by force closing an app instance. We will not emulate a failure by killing an entire emulator instance.
All failures are temporary; you can assume that a failed node will recover soon, i.e., it will not be permanently unavailable during a run.
When a node recovers, it should copy all the object writes it missed during the failure. This can be done by asking the right nodes and copy from them.
Please focus on correctness rather than performance. Once you handle failures correctly, if you still have time, you can improve your performance.
Your content provider should support concurrent read/write operations.
Your content provider should handle a failure happening at the same time with read/write operations.
Replication should be done exactly the same way as Dynamo does. In other words, a (key, value) pair should be replicated over three consecutive partitions, starting from the partition that the key belongs to.
Unlike Dynamo, there are two things you do not need to implement.
Virtual nodes: Your implementation should use physical nodes rather than virtual nodes, i.e., all partitions are static and fixed.
Hinted handoff: Your implementation do not need to implement hinted handoff. This means that when there is a failure, it is OK to replicate on only two nodes.
All replicas should store the same value for each key. This is “per-key” consistency. There is no consistency guarantee you need to provide across keys. More formally, you need to implement per-key linearizability.
Each content provider instance should have a node id derived from its emulator port. This node id should be obtained by applying the above hash function (i.e., genHash()) to the emulator port. For example, the node id of the content provider instance running on emulator-5554 should be, node_id = genHash(“5554”). This is necessary to find the correct position of each node in the Dynamo ring.
Your content provider’s URI should be “content://edu.buffalo.cse.cse486586.simpledynamo.provider”, which means that any app should be able to access your content provider using that URI. This is already defined in the template, so please don’t change this. Your content provider does not need to match/support any other URI pattern.
We have fixed the ports & sockets.
Your app should open one server socket that listens on 10000.
You need to use run_avd.py and set_redir.py to set up the testing environment.
The grading will use 5 AVDs. The redirection ports are 11108, 11112, 11116, 11120, and 11124.
You should just hard-code the above 5 ports and use them to set up connections.
Please use the code snippet provided in PA1 on how to determine your local AVD.
emulator-5554: “5554”
emulator-5556: “5556”
emulator-5558: “5558”
emulator-5560: “5560”
emulator-5562: “5562”
Any app (not just your app) should be able to access (read and write) your content provider. As with the previous assignment, please do not include any permission to access your content provider.
The following is a guideline for your content provider based on the design of Amazon Dynamo:
Membership
Just as the original Dynamo, every node can know every other node. This means that each node knows all other nodes in the system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node without using a ring-based routing.
Request routing
Unlike Chord, each Dynamo node knows all other nodes in the system and also knows exactly which partition belongs to which node.
Under no failures, all requests are directly forwarded to the coordinator, and the coordinator should be in charge of serving read/write operations.
Quorum replication
For linearizability, you can implement a quorum-based replication used by Dynamo.
Note that the original design does not provide linearizability. You need to adapt the design.
The replication degree N should be 3. This means that given a key, the key’s coordinator as well as the 2 successor nodes in the Dynamo ring should store the key.
Both the reader quorum size R and the writer quorum size W should be 2.
The coordinator for a get/put request should always contact other two nodes and get the votes.
For write operations, all objects can be versioned in order to distinguish stale copies from the most recent copy.
For read operations, if the readers in the reader quorum have different versions of the same object, the coordinator should pick the most recent version and return it.
Chain replication
Another replication strategy you can implement is chain replication, which provides linearizability.
If you are interested in more details, please take a look at the following paper: http://www.cs.cornell.edu/home/rvr/papers/osdi04.pdf
In chain replication, a write operation always comes to the first partition; then it propagates to the next two partitions in sequence. The last partition returns the result of the write.
A read operation always comes to the last partition and reads the value from the last partition.
Failure handling
Handling failures should be done very carefully because there can be many corner cases to consider and cover.
Just as the original Dynamo, each request can be used to detect a node failure.
For this purpose, you can use a timeout for a socket read; you can pick a reasonable timeout value, e.g., 100 ms, and if a node does not respond within the timeout, you can consider it a failure.
Do not rely on socket creation or connect status to determine if a node has failed. Due to the Android emulator networking setup, it is not safe to rely on socket creation or connect status to judge node failures. Please use an explicit method to test whether an app instance is running or not, e.g., using a socket read timeout as described above.
When a coordinator for a request fails and it does not respond to the request, its successor can be contacted next for the request.
Testing

We have testing programs to help you see how your code does with our grading criteria. There are 6 phases in testing.
Testing basic ops
This phase will test basic operations, i.e., insert, query, delete, @, and *. This will test if everything is correctly replicated. There is no concurrency in operations and there is no failure either.
Testing concurrent ops with different keys
This phase will test if your implementation can handle concurrent operations under no failure.
The tester will use independent (key, value) pairs inserted/queried concurrently on all the nodes.
Testing concurrent ops with same keys
This phase will test if your implementation can handle concurrent operations with same keys under no failure.
The tester will use the same set of (key, value) pairs inserted/queried concurrently on all the nodes.
Testing one failure
This phase will test one failure with every operation.
One node will crash before operations start. After all the operations are done, the node will recover.
This will be repeated for each and every operation.
Testing concurrent operations with one failure
This phase will execute operations concurrently and crash one node in the middle of the execution. After some time, the failed node will also recover in the middle of the execution.
Testing concurrent operations with one consistent failure
This phase will crash one node at a time consistently, i.e., one node will crash then recover, and another node will crash and recover, etc.
There will be a brief period of time in between the crash-recover sequence.
Each testing phase is quite intensive (i.e., it will take some time for each phase to finish), so the tester allows you to specify which testing phase you want to test. You won’t have to wait until everything is finished every time. However, you still need to make sure that you run the tester in its entirety before you submit. We will not test individual testing phases separately in our grading.
You can specify which testing phase you want to test by providing ‘-p’ or ‘--phase’ argument to the tester.
‘-h’ argument will show you what options are available.
Download a testing program for your platform. If your platform does not run it, please report it on Piazza.
Windows: We’ve tested it on 32- and 64-bit Windows 8.
Linux: We’ve tested it on 32- and 64-bit Ubuntu 12.04.
OS X: We’ve tested it on 32- and 64-bit OS X 10.9 Mavericks.
Before you run the program, please make sure that you are running five AVDs. python run_avd.py 5 will do it.
Run the testing program from the command line.
On your terminal, it will give you your partial and final score, and in some cases, problems that the testing program finds.
Submission

We use the CSE submit script. You need to use either “submit_cse486” or “submit_cse586”, depending on your registration status. If you haven’t used it, the instructions on how to use it is here: https://wiki.cse.buffalo.edu/services/content/submit-script
You need to submit one file described below. Once again, you must follow everything below exactly. Otherwise, you will get no point on this assignment.
Your entire Eclipse project source code tree zipped up in .zip: The name should be SimpleDynamo.zip. Please do not change the name. To do this, please do the following
Open Eclipse.
Go to “File” -> “Export”.
Select “General -> Archive File”.
Select your project. Make sure that you include all the files and check “Save in zip format”.
Please do not use any other compression tool other than zip, i.e., no 7-Zip, no RAR, etc.
Deadline: 5/9/14 (Friday) 1:59pm

The deadline is firm; if your timestamp is 2pm, it is a late submission.
Grading

This assignment is 15% of your final grade. Also there is extra credit if you pass all 6 phases.
Phase 1: 2%
Phase 2: 3%
Phase 3: 2%
Phase 4: 4%
Phase 5: 4%
Phase 6: 3%
