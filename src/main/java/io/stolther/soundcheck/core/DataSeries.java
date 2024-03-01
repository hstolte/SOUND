package io.stolther.soundcheck.core;

import java.io.Serializable;
import java.util.Objects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class DataSeries implements Serializable {

    public final long t;
    public final double[] v;
    public final double[] sigma;
    public final String key;

    public DataSeries(long t, double[] v, double[] sigma, String key) {
        if (sigma != null && v.length != sigma.length) {
            throw new IllegalArgumentException("Arrays have different lengths");
        }
        this.t = t;
        this.v = v;
        this.sigma = sigma;
        this.key = key;
    }

    public DataSeries(long t, double[] v, double[] sigma) {
        this(t, v, sigma, null);
    }

    public DataSeries(long t, double[] v) {
        this(t, v, null);
    }

    public DataSeries shorten(int n_points) {
        if (n_points == 0) {
            return this;
        }
        return new DataSeries(t, getFirstNElements(n_points, v), getFirstNElements(n_points, sigma), key);
    }

    public static double[] getFirstNElements(int n, double[] originalArray) {
        if (originalArray == null) {
            return originalArray;
        }
        if (originalArray.length < n) {
            return originalArray.clone();
        } else {
            double[] newArray = new double[n];
            for (int i = 0; i < n; i++) {
                newArray[i] = originalArray[i];
            }
            return newArray;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSeries that = (DataSeries) o;
        return t == that.t &&
                Objects.deepEquals(v, that.v) &&
                Objects.deepEquals(sigma, that.sigma) &&
                Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(t, v, sigma, key);
    }

    public static class KryoSerializer extends com.esotericsoftware.kryo.Serializer<DataSeries> implements Serializable {

        @Override
        public void write(Kryo kryo, Output output, DataSeries object) {
            output.writeLong(object.t);
            output.writeInt(object.v.length);
            for (double value : object.v) {
                output.writeDouble(value);
            }
            if (object.sigma != null) {
                output.writeBoolean(true);
                for (double s : object.sigma) {
                    output.writeDouble(s);
                }
            } else {
                output.writeBoolean(false);
            }
            output.writeString(object.key);
        }

        @Override
        public DataSeries read(Kryo kryo, Input input, Class<DataSeries> type) {
            long t = input.readLong();
            int length = input.readInt();
            double[] v = new double[length];
            for (int i = 0; i < length; i++) {
                v[i] = input.readDouble();
            }
            double[] sigma = null;
            if (input.readBoolean()) {
                sigma = new double[length];
                for (int i = 0; i < length; i++) {
                    sigma[i] = input.readDouble();
                }
            }
            String key = input.readString();
            return new DataSeries(t, v, sigma, key);
        }
    }

}

