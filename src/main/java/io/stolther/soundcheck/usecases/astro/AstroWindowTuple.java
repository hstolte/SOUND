package io.stolther.soundcheck.usecases.astro;

import java.io.Serializable;
import java.util.Objects;

import io.stolther.soundcheck.core.DataSeries;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class AstroWindowTuple implements Serializable {

    public final long timestamp;
    public final float maxDiff;
    public final DataSeries dataSeries;
    public final long maxStimulus;

    public AstroWindowTuple(long timestamp, float maxDiff, DataSeries dataSeries, long maxStimulus) {
        this.timestamp = timestamp;
        this.maxDiff = maxDiff;
        this.dataSeries = dataSeries;
        this.maxStimulus = maxStimulus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AstroWindowTuple that = (AstroWindowTuple) o;
        return timestamp == that.timestamp &&
                Float.compare(that.maxDiff, maxDiff) == 0 &&
                maxStimulus == that.maxStimulus &&
                Objects.equals(dataSeries, that.dataSeries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, maxDiff, dataSeries, maxStimulus);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("timestamp", timestamp)
                .append("maxDiff", maxDiff)
                .append("dataSeries", dataSeries)
                .append("maxStimulus", maxStimulus)
                .toString();
    }

    public static class KryoSerializer extends com.esotericsoftware.kryo.Serializer<AstroWindowTuple> implements Serializable {

        @Override
        public void write(Kryo kryo, Output output, AstroWindowTuple object) {
            output.writeLong(object.timestamp);
            output.writeFloat(object.maxDiff);
            kryo.writeObject(output, object.dataSeries);
            output.writeLong(object.maxStimulus);
        }

        @Override
        public AstroWindowTuple read(Kryo kryo, Input input, Class<AstroWindowTuple> type) {
            long timestamp = input.readLong();
            float maxDiff = input.readFloat();
            DataSeries dataSeries = kryo.readObject(input, DataSeries.class);
            long maxStimulus = input.readLong();
            return new AstroWindowTuple(timestamp, maxDiff, dataSeries, maxStimulus);
        }
    }
}
