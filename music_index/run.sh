
HADOOP_CMD="/usr/local/src/hadoop-1.2.1/bin/hadoop"
STREAM_JAR_PATH="/usr/local/src/hadoop-1.2.1/contrib/streaming/hadoop-streaming-1.2.1.jar"

INPUT_FILE_PATH_1="/music_meta.txt.small"
OUTPUT_Z_PATH="/output_z_fenci"
OUTPUT_D_PATH="/output_d_fenci"

#$HADOOP_CMD fs -rmr $OUTPUT_Z_PATH

# Step 1.
$HADOOP_CMD jar $STREAM_JAR_PATH \
    -input $INPUT_FILE_PATH_1 \
    -output $OUTPUT_Z_PATH \
    -mapper "python map.py mapper_func" \
    -jobconf "mapred.reduce.tasks=0" \
    -jobconf  "mapred.job.name=jieba_fenci_demo" \
    -file "./jieba.tar.gz" \
    -file "./map.py"

# Step 2.
$HADOOP_CMD jar $STREAM_JAR_PATH \
    -input $OUTPUT_Z_PATH \
    -output $OUTPUT_D_PATH \
    -mapper "python map_inverted.py mapper_func" \
    -reducer "python red_inverted.py reducer_func" \
    -jobconf "mapred.reduce.tasks=2" \
    -jobconf  "mapred.job.name=jieba_fenci" \
    -file "./map_inverted.py" \
    -file "./red_inverted.py"


