package com.antoniocali.sources.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class BigQuerySplitEnumeratorStateSerializer implements SimpleVersionedSerializer<BigQuerySplitEnumeratorState> {
    private final int VERSION = 0;
    public static BigQuerySplitEnumeratorStateSerializer INSTANCE = new BigQuerySplitEnumeratorStateSerializer();

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(BigQuerySplitEnumeratorState bigQuerySplitEnumeratorState) throws IOException {

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(
                baos)) {
            out.writeBoolean(bigQuerySplitEnumeratorState.isAssigned());
            out.writeLong(bigQuerySplitEnumeratorState.getCurrentMaxTimestamp());
            out.writeUTF(bigQuerySplitEnumeratorState.getCurrentSplit().getSplitName());
            out.writeLong(bigQuerySplitEnumeratorState.getCurrentSplit().getMinTimestamp());
            out.writeLong(bigQuerySplitEnumeratorState.getCurrentSplit().getMaxTimestamp());
            out.flush();
            return baos.toByteArray();
        }

    }

    @Override
    public BigQuerySplitEnumeratorState deserialize(int version, byte[] serialized) throws IOException {

        if (getVersion() != version) {
            throw new IllegalArgumentException(
                    String.format("The provided serializer version (%d) is not expected (expected : %s).", version,
                            VERSION));
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized); DataInputStream in = new DataInputStream(
                bais)) {
            switch (version) {
                case VERSION:
                    boolean isAssigned = in.readBoolean();
                    long currentMaxTimestamp = in.readLong();
                    String splitName = in.readUTF();
                    long splitMinTimestamp = in.readLong();
                    long splitMaxTimestamp = in.readLong();
                    BigQuerySourceSplit currentSplit = new BigQuerySourceSplit(splitName, splitMinTimestamp,
                            splitMaxTimestamp);
                    return new BigQuerySplitEnumeratorState(currentSplit, isAssigned, currentMaxTimestamp);
                default:
                    throw new IOException("Unknown version: " + version);
            }
        }
    }
}