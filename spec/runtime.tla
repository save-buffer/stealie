---------------------------- MODULE runtime ----------------------------
EXTENDS Sequences,
        Naturals

CONSTANTS   Proc,
            ConcurrentIOs

VARIABLES   task_pool,
            buffer_pool,
            io_request_pool,
            io_sqes,
            io_cqes,
            is_executing,
            buffers_in_use,
            is_checking_task_pool

Vars == <<task_pool, buffer_pool, io_request_pool, io_sqes, io_cqes, is_executing, buffers_in_use, is_checking_task_pool>>

Min(A, B) == IF A < B THEN A ELSE B

Init == /\ task_pool = [p \in Proc |-> 1]
        /\ buffer_pool = [p \in Proc |-> 0]
        /\ io_request_pool = [p \in Proc |-> 0]
        /\ io_sqes = [p \in Proc |-> 0]
        /\ io_cqes = [p \in Proc |-> 0]
        /\ is_executing = [p \in Proc |-> FALSE]
        /\ buffers_in_use = [p \in Proc |-> 0]
        /\ is_checking_task_pool = [p \in Proc |-> TRUE]

BeginTask(p) == /\ task_pool' = [task_pool EXCEPT ![p] = @ - 1]
                /\ is_executing' = [is_executing EXCEPT ![p] = TRUE]

StealTask(p) == /\ \E victim \in Proc : /\ task_pool[victim] /= 0
                                        /\ task_pool' = [task_pool EXCEPT ![victim] = @ - 1]
                /\ is_executing' = [is_executing EXCEPT ![p] = TRUE]

PollTaskQueue(p) == /\ IF task_pool[p] /= 0 
                       THEN BeginTask(p)
                       ELSE IF \E victim \in Proc : task_pool[victim] /= 0
                       THEN StealTask(p)
                       ELSE UNCHANGED <<task_pool, is_executing>>
                    /\ is_checking_task_pool' = [is_checking_task_pool EXCEPT ![p] = FALSE]
                    /\ UNCHANGED <<buffer_pool, io_request_pool, io_sqes, io_cqes, buffers_in_use>>

SubmitSQE(p) == LET num_available_global == 2 * ConcurrentIOs - (io_cqes[p] + io_sqes[p])
                    num_available_sqe == ConcurrentIOs - io_sqes[p]
                    num_available == Min(num_available_global, num_available_sqe)
                    to_insert == Min(io_request_pool[p], num_available)
                IN  /\ io_sqes' = [io_sqes EXCEPT ![p] = @ + to_insert]
                    /\ io_request_pool' = [io_request_pool EXCEPT ![p] = @ - to_insert]

StealSQE(p) == \E victim \in Proc : /\ io_request_pool[victim] /= 0
                                    /\ io_request_pool' = [io_request_pool EXCEPT ![victim] = @ - 1]
                                    /\ io_sqes' = [io_sqes EXCEPT ![p] = @ + 1]

CompleteCQEs(p) == /\ is_executing' = [is_executing EXCEPT ![p] = TRUE]
                   /\ io_cqes' = [io_cqes EXCEPT ![p] = 0]
                   /\ task_pool' = [io_sqes EXCEPT ![p] = @ + io_cqes[p] - 1]
                  
StealCQE(p) == /\ \E victim \in Proc : /\ io_cqes[victim] /= 0
                                       /\ io_cqes' = [io_cqes EXCEPT ![victim] = @ - 1]
               /\ is_executing' = [is_executing EXCEPT ![p] = TRUE]
               /\ UNCHANGED <<task_pool>>

PollIoQueue(p) == /\ IF io_sqes[p] + io_cqes[p] = 2 * ConcurrentIOs
                     THEN UNCHANGED <<io_sqes, io_request_pool>>
                     ELSE IF io_request_pool[p] > 0
                     THEN SubmitSQE(p)
                     ELSE IF \E victim \in Proc : /\ io_sqes[victim] /= 0
                     THEN StealSQE(p)
                     ELSE UNCHANGED <<io_sqes, io_request_pool>>
                  /\ IF io_cqes[p] > 0
                     THEN CompleteCQEs(p)
                     ELSE IF \E victim \in Proc : io_cqes[victim] /= 0
                     THEN StealCQE(p)
                     ELSE UNCHANGED <<task_pool, io_cqes, is_executing>>
                  /\ is_checking_task_pool' = [is_checking_task_pool EXCEPT ![p] = TRUE]
                  /\ UNCHANGED <<buffer_pool, buffers_in_use>>

ExecuteThreadPool(p) == /\ is_executing[p] = FALSE
                        /\ IF is_checking_task_pool[p]
                           THEN PollTaskQueue(p)
                           ELSE PollIoQueue(p)

ScheduleTask(p) == /\ task_pool' = [task_pool EXCEPT ![p] = @ + 1]
                   /\ UNCHANGED <<buffer_pool, io_request_pool, io_sqes, io_cqes, is_executing, buffers_in_use, is_checking_task_pool>>

StealBuffer(p) == /\ buffer_pool[p] = 0
                  /\ \E victim \in Proc : /\ buffer_pool[victim] /= 0
                                          /\ buffer_pool' = [buffer_pool EXCEPT ![victim] = @ - 1]                    
MapBuffer(p) == /\ buffer_pool[p] = 0
                /\ \A victim \in Proc : buffer_pool[victim] = 0
                /\ UNCHANGED <<buffer_pool>>
GetBufferFromPool(p) == /\ buffer_pool[p] /= 0
                        /\ buffer_pool' = [buffer_pool EXCEPT ![p] = @ + 1]
GetBuffer(p) == /\ \/ StealBuffer(p)
                   \/ MapBuffer(p)
                   \/ GetBufferFromPool(p)
                /\ buffers_in_use' = [buffers_in_use EXCEPT ![p] = @ + 1]
                /\ UNCHANGED <<task_pool, io_request_pool, io_sqes, io_cqes, is_executing, is_checking_task_pool>>
                
ReturnBuffer(p) == buffers_in_use[p] /= 0
                   /\ buffer_pool' = [buffer_pool EXCEPT ![p] = @ + 1]
                   /\ buffers_in_use' = [buffers_in_use EXCEPT ![p] = @ - 1]
                   /\ UNCHANGED <<task_pool, io_request_pool, io_sqes, io_cqes, is_executing, is_checking_task_pool>>

AddToSQE(p) == /\ io_sqes' = [io_sqes EXCEPT ![p] = @ + 1] 
               /\ UNCHANGED <<io_request_pool>>
AddToRequestPool(p) == /\ io_request_pool' = [io_request_pool EXCEPT ![p] = @ + 1]    
                       /\ UNCHANGED <<io_sqes>>  
ScheduleIO(p) == /\ IF io_sqes[p] < ConcurrentIOs /\ io_sqes[p] + io_cqes[p] < 2 * ConcurrentIOs
                    THEN AddToSQE(p) 
                    ELSE AddToRequestPool(p)
                 /\ UNCHANGED <<task_pool, buffer_pool, io_cqes, is_executing, buffers_in_use, is_checking_task_pool>>

CompleteTask(p) == /\ is_executing' = [is_executing EXCEPT ![p] = FALSE]
                   /\ UNCHANGED <<task_pool, buffer_pool, io_request_pool, io_sqes, io_cqes, buffers_in_use, is_checking_task_pool>>

ExecuteTask(p) == /\ is_executing[p] = TRUE
                  /\ \/ ScheduleTask(p)
                     \/ GetBuffer(p)
                     \/ ReturnBuffer(p)
                     \/ ScheduleIO(p)
                     \/ CompleteTask(p)

CompleteSQE(p) == /\ io_sqes[p] /= 0
                  /\ io_sqes' = [io_sqes EXCEPT ![p] = @ - 1]
                  /\ io_cqes' = [io_cqes EXCEPT ![p] = @ + 1]
                  /\ UNCHANGED <<task_pool, buffer_pool, io_request_pool, is_executing, buffers_in_use, is_checking_task_pool>>

Terminate == /\ \A p \in Proc : /\ task_pool[p] = 0
                                /\ io_request_pool[p] = 0
                                /\ io_sqes[p] = 0
                                /\ io_cqes[p] = 0
                                /\ is_executing[p] = FALSE
             /\ UNCHANGED Vars

Next == \/ Terminate
        \/ \E p \in Proc : \/ ExecuteThreadPool(p)
                           \/ ExecuteTask(p)
                           \/ CompleteSQE(p)
            
Spec == /\ Init 
        /\ [][Next]_Vars 
        /\ \A p \in Proc : WF_Vars(CompleteTask(p)) /\ WF_Vars(CompleteSQE(p))

TypeOK == /\ task_pool \in [Proc -> Nat]
          /\ buffer_pool \in [Proc -> Nat]
          /\ io_request_pool \in [Proc -> Nat]
          /\ io_sqes \in [Proc -> Nat]
          /\ io_cqes \in [Proc -> Nat]
          /\ is_executing \in [Proc -> {TRUE, FALSE}]
          /\ buffers_in_use \in [Proc -> Nat]
          /\ is_checking_task_pool \in [Proc -> {TRUE, FALSE}]

IOsInRange == \A p \in Proc : /\ io_sqes[p] <= ConcurrentIOs
                              /\ io_cqes[p] <= 2 * ConcurrentIOs
                              /\ io_sqes[p] + io_cqes[p] <= 2 * ConcurrentIOs
=============================================================================
\* Modification History
\* Last modified Sun Dec 03 16:35:19 PST 2023 by sasha
\* Created Thu Dec 08 10:57:24 PST 2022 by sasha
