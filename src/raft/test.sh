#!bin/bash
python dtest.py \
TestInitialElection2A \
TestReElection2A \
TestManyElections2A \
TestBasicAgree2B \
TestRPCBytes2B \
TestFailAgree2B \
TestFailNoAgree2B \
TestConcurrentStarts2B \
TestRejoin2B \
TestBackup2B \
TestCount2B \
-n 10000 \
-p 2 \
-r 
