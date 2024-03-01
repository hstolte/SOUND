package io.stolther.soundcheck.usecases.astro;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;
import java.util.regex.Pattern;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class AstroDataPoint implements Serializable {


    public final long time;
    public final double flux;
    public final double sigma;
    public final double relative_error;
    public final int is_upper_lim;
    public final long stimulus;
    public final String sourceName;

    private static final Pattern DELIMITER_PATTERN = Pattern.compile(",");

    public static AstroDataPoint fromReading(String reading) {
        try {
            String[] tokens = DELIMITER_PATTERN.split(reading.trim());
            final long time = Long.valueOf(tokens[0]);
            final double flux = Double.valueOf(tokens[1]);
            final double sigma = Double.valueOf(tokens[2]);
            final double relative_error = Double.valueOf(tokens[3]);
            final int is_upper_lim = Integer.valueOf(tokens[4]);
            final String sourceName = tokens[5];
            final long stimulus = System.currentTimeMillis();
            return new AstroDataPoint(time, flux, sigma, relative_error, is_upper_lim, stimulus, sourceName);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Failed to parse reading: %s", reading), e);
        }
    }

    public AstroDataPoint(long time, double flux, double sigma, double relative_error, int is_upper_lim, long stimulus, String sourceName) {
        this.time = time;
        this.flux = flux;
        this.sigma = sigma;
        this.relative_error = relative_error;
        this.is_upper_lim = is_upper_lim;
        this.stimulus = stimulus;
        this.sourceName = sourceName;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("time", time)
                .append("flux", flux)
                .append("sigma", sigma)
                .append("relative_error", relative_error)
                .append("is_upper_lim", is_upper_lim)
                .append("sourceName", sourceName)
                .toString();
    }

    public static class KryoSerializer extends com.esotericsoftware.kryo.Serializer<AstroDataPoint>
            implements Serializable {

        @Override
        public void write(Kryo kryo, Output output, AstroDataPoint object) {
            output.writeLong(object.time);
            output.writeDouble(object.flux);
            output.writeDouble(object.sigma);
            output.writeDouble(object.relative_error);
            output.writeInt(object.is_upper_lim);
            output.writeLong(object.stimulus);
            output.writeString(object.sourceName);
        }

        @Override
        public AstroDataPoint read(Kryo kryo, Input input, Class<AstroDataPoint> type) {
            return new AstroDataPoint(input.readLong(), input.readDouble(), input.readDouble(),
                    input.readDouble(), input.readInt(), input.readLong(), input.readString());
        }
    }


}
