package com.antoniocali.sources.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class BigQuerySourceSplitSerializer implements SimpleVersionedSerializer<BigQuerySourceSplit> {
    public static final BigQuerySourceSplitSerializer INSTANCE = new BigQuerySourceSplitSerializer();
    final int VERSION = 0;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(BigQuerySourceSplit bigQuerySourceSplit) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(
                baos)) {
            out.writeUTF(bigQuerySourceSplit.getSplitName());
            out.writeLong(bigQuerySourceSplit.getMinTimestamp());
            out.writeLong(bigQuerySourceSplit.getMaxTimestamp());
            out.flush();
            return baos.toByteArray();
        }

    }

    @Override
    public BigQuerySourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (getVersion() != version) {
            throw new IllegalArgumentException(
                    String.format("The provided serializer version (%d) is not expected (expected : %s).", version,
                            VERSION));
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized); DataInputStream in = new DataInputStream(
                bais)) {
            switch (version) {
                case VERSION:
                    String splitName = in.readUTF();
                    long minTimestamp = in.readLong();
                    long maxTimestamp = in.readLong();
                    return new BigQuerySourceSplit(splitName, minTimestamp, maxTimestamp);
                default:
                    throw new IOException("Unknown version: " + version);
            }
        }
    }
}
