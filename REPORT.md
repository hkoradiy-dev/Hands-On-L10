# Hands-on L10: Report

**Name:Hardi Koradiya**

**Student ID:801484363**

**Email:hkoradiy@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`: 801484363

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.

```bash
python3 datagen.py YOUR_STUDENT_ID

docker compose up -d

# Task 1
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task1 /opt/spark/work-dir/shared

# Task 2: append mode
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task2 /opt/spark/work-dir/shared

# Task 2: update mode
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task2 /opt/spark/work-dir/shared update


The output directories and checkpoints were reset before the initial runs, as required for a fresh execution.
```

---

## Results

### Task 1: ingest and parse

Paste the micro-batch summary printed at the end of the run, and the number of rows in
`shared-folder/output/task1` (`cat shared-folder/output/task1/*.csv | grep -vc trip_id`).

```
Task 1 processed 279 rows across 12 micro-batches. The input row counts for the batches were 21, 28, 22, 21, 23, 25, 20, 28, 26, 23, 22, and 20.

The output CSV contains 279 data rows. The query uses append mode, a CSV file sink, a checkpoint, and the availableNow trigger.

Task 1 successfully processed 12 input micro-batches. The number of input rows in each batch was:
=== task1: micro-batch summary ===
batch input rows            watermark state rows
    0         21                    -          -
    1         28                    -          -
    2         22                    -          -
    3         21                    -          -
    4         23                    -          -
    5         25                    -          -
    6         20                    -          -
    7         28                    -          -
    8         26                    -          -
    9         23                    -          -
   10         22                    -          -
   11         20                    -          -

The micro-batch summary showed that all 12 batches ran without a reported error. No watermark or state rows were reported for Task 1 because it does not use a stateful window aggregation.

```
How many micro-batches ran, and why that number?
- Twelve micro-batches ran because the generator created 12 input files and the streaming source was configured to process at most one new file per micro-batch. Each batch processed one file.The Task 1 output contains the ride details and the calculated fare_per_km column.

### Task 2: trips and fares per window

Paste the windows written by the stream (`cat shared-folder/output/task2/*.csv | grep -v
window_start | sort`), the summary, and the batch table that `main.py` prints at the end.

```
Task 2 used two-minute event-time windows and a two-minute watermark. The micro-batch summary showed 12 batches containing input rows, followed by batch 12 with zero input rows. The final empty batch allowed Spark to advance its watermark and finalize eligible windows.

The batch calculation over all input files produced these results:

=== task2: micro-batch summary ===
batch input rows            watermark state rows
    0         21                    -          2
    1         28  2026-10-27 07:58:57          2
    2         22  2026-10-27 07:59:49          3
    3         21  2026-10-27 08:00:37          2
    4         23  2026-10-27 08:01:52          3
    5         25  2026-10-27 08:02:52          2
    6         20  2026-10-27 08:03:50          3
    7         28  2026-10-27 08:04:46          2
    8         26  2026-10-27 08:05:44          3
    9         23  2026-10-27 08:06:46          2
   10         22  2026-10-27 08:07:36          3
   11         20  2026-10-27 08:08:55          2
   12          0  2026-10-27 08:09:57          2

=== task2: the same windows computed in batch over all files ===
+-------------------+-------------------+-----+----------+
|window_start       |window_end         |trips|total_fare|
+-------------------+-------------------+-----+----------+
|2026-10-27 07:58:00|2026-10-27 08:00:00|1    |59.3      |
|2026-10-27 08:00:00|2026-10-27 08:02:00|59   |2598.78   |
|2026-10-27 08:02:00|2026-10-27 08:04:00|39   |1876.36   |
|2026-10-27 08:04:00|2026-10-27 08:06:00|52   |2392.47   |
|2026-10-27 08:06:00|2026-10-27 08:08:00|48   |2112.31   |
|2026-10-27 08:08:00|2026-10-27 08:10:00|44   |2037.2    |
|2026-10-27 08:10:00|2026-10-27 08:12:00|36   |1585.55   |
+-------------------+-------------------+-----+----------+


The append-mode CSV output contained five windows:
ndow_start | sort
2026-10-27 07:58:00,2026-10-27 08:00:00,1,59.3
2026-10-27 08:00:00,2026-10-27 08:02:00,57,2506.13
2026-10-27 08:02:00,2026-10-27 08:04:00,39,1876.36
2026-10-27 08:04:00,2026-10-27 08:06:00,52,2392.47
2026-10-27 08:06:00,2026-10-27 08:08:00,48,2112.31

```

---

## Questions

**1. Append versus update.** Run task 2 a second time with `update` (it prints to the
console). In which batch does each window first appear in update mode, and in which batch
is it written in append mode? Explain the difference with the watermark values from the
summary.
- In update mode, a window appears when Spark has received data for that window. Its values can change in later batches as more rides arrive. From the terminal output, the first appearance of each window was:
        Window	      First appeared in update batch
        07:58–08:00	        0
        08:00–08:02	        0
        08:02–08:04     	2
        08:04–08:06	        4
        08:06–08:08	        6
        08:08–08:10	        8
        08:10–08:12	        10

Append mode works differently. It writes a window only after the watermark indicates that the window is complete. This prevents the output from being repeatedly updated.

The watermark advanced from approximately 07:58:57 in batch 1 to 08:09:57 in batch 12. Because the watermark had not passed 08:10 by the end of the run, the windows ending at 08:10 and 08:12 were not both guaranteed to be written. In particular, the 08:08–08:10 window was not yet complete according to the final watermark.

Therefore, update mode is useful for seeing changing results, while append mode is useful for writing completed windows.


**2. Where did the rows go?** Compare the stream's windows with the batch table. Which
windows have fewer trips in the stream, by how many, and why? Which windows are missing
from the stream's output altogether, and what would make Spark write them? (Look at
`datagen.py` for how the late events were made.)

The batch calculation includes every input record, while a watermarked streaming aggregation can drop events that arrive too late.

The batch table showed 59 trips in the 08:00–08:02 window. In update mode, the final displayed count for that window was 57 trips, with a total fare of 2,506.13. This is two fewer trips than the batch calculation.

The other windows reached the same final trip counts as the batch results in the console output:

08:02–08:04: 39 trips

08:04–08:06: 52 trips

08:06–08:08: 48 trips

08:08–08:10: 44 trips

08:10–08:12: 36 trips

The main difference is that the batch calculation includes late events that the streaming query may discard after the watermark has advanced.

The 08:08–08:10 and 08:10–08:12 windows were not yet complete at the end of the append-mode run, based on the final watermark of approximately 08:09:57. Spark would need to process more data with sufficiently advanced event times, or otherwise advance the watermark beyond each window's end, to finalize and write those windows.



**3. Checkpoint.** Run task 1 again without deleting `shared-folder/checkpoint/task1`. What
happens, and why? List the subfolders of the checkpoint and say in one line what each one
holds.

A checkpoint stores information about a streaming query's progress. It allows Spark to remember which input data has already been processed.

When Task 1 is run again without deleting its checkpoint, Spark should recognize the previously processed input files. If no new files are available, the query should not process the same input records as new data again.

The checkpoint folder should be inspected after the rerun to confirm the actual subfolders. Typical Spark checkpoint contents include:

offsets: records the input offsets or progress for each batch.

commits: records completed batches.

sources: stores source-related progress information.

metadata: contains query metadata.

The exact folders depend on the query and Spark version. The actual checkpoint directory contents should be listed before final submission.

- I reran Task 1 without deleting its checkpoint. The query completed with batch 12, which had zero input rows. This shows that Spark remembered the files already processed and did not process them as new input again.


---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.

Both Task 1 and Task 2 completed and printed their micro-batch summaries and batch comparison table. The results showed that Task 2's streaming aggregation may differ from the batch calculation because of late events and watermark handling.

- The Task 1 rerun processed zero new rows because the existing checkpoint recorded the earlier progress. This was expected behavior, not an error. To process the same files from the beginning, the Task 1 checkpoint must be deleted before rerunning the query.


No specific error message was included in the supplied terminal output. Therefore, I have not listed an unverified error or claimed that a particular error was fixed.
