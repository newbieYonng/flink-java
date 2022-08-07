package state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;

public class MyAtLeastOnceFileSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {

    private boolean flag = true;
    private long offset = 0;
    private String path;
    private transient ListState<Long> listState;

    public MyAtLeastOnceFileSource(String path) {
        this.path = path;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>("offset-state", Long.class);
        listState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            for (Long offset : listState.get()) {
                this.offset = offset;
            }
        }
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

        RandomAccessFile raf = new RandomAccessFile(path + "\\" + indexOfThisSubtask + ".txt", "r");
        raf.seek(offset);

        while (flag) {
            String line = raf.readLine();
            if (line != null) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(indexOfThisSubtask + ":" + line);
                    offset = raf.getFilePointer();
                }
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listState.clear();
        listState.add(offset);
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
