package com.example.dtb.batch.partitioner;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

/**
 * CSV 파일의 라인 범위를 gridSize개의 파티션으로 분할한다.
 *
 * 각 파티션의 ExecutionContext:
 *   linesToSkip — FlatFileItemReader가 건너뛸 라인 수 (헤더 + 이전 파티션 데이터)
 *   lineCount   — 이 파티션이 읽을 데이터 라인 수
 */
public class TransactionLineRangePartitioner implements Partitioner {

    private final int totalDataLines;
    private final int headerLines;

    public TransactionLineRangePartitioner(int totalDataLines, int headerLines) {
        this.totalDataLines = totalDataLines;
        this.headerLines = headerLines;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        int partitionSize = (int) Math.ceil((double) totalDataLines / gridSize);
        Map<String, ExecutionContext> partitions = new HashMap<>();

        for (int i = 0; i < gridSize; i++) {
            int dataOffset = i * partitionSize;
            if (dataOffset >= totalDataLines) break;

            int lineCount = Math.min(partitionSize, totalDataLines - dataOffset);
            int linesToSkip = headerLines + dataOffset;

            ExecutionContext context = new ExecutionContext();
            context.putInt("linesToSkip", linesToSkip);
            context.putInt("lineCount", lineCount);
            partitions.put("partition" + i, context);
        }

        return partitions;
    }
}
