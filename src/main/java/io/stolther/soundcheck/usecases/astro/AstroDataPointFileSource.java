package io.stolther.soundcheck.usecases.astro;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.SimpleTextSource;

import java.io.File;

public class AstroDataPointFileSource extends SimpleTextSource<AstroDataPoint> {

    public AstroDataPointFileSource(ExperimentSettings settings) {
        super(settings, true);
    }

    public AstroDataPointFileSource(String name, ExperimentSettings settings) {
        super(name, settings, true);
    }

    @Override
    protected String inputFile() {
        return settings.inputFolder() + File.separator + settings.inputFile("csv");
    }

    @Override
    protected AstroDataPoint getTuple(String line) {
        return AstroDataPoint.fromReading(line);
    }

    @Override
    public long getTimestamp(AstroDataPoint tuple) {
        return tuple.time;
    }
}

