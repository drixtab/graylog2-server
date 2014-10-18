/**
 * Copyright 2014 Lennart Koopmann <lennart@torch.sh>
 *
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package org.graylog2.integrations.librato;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class GaugeMetric implements LibratoMetric {

    private final String name;
    private final Number value;
    private final int measureTime;
    private final String source;

    public GaugeMetric(String name, Number value, int measureTime, String source) {
        this.name = name;
        this.value = value;
        this.measureTime = measureTime;
        this.source = source;
    }

    public String getName() {
        return name;
    }

    public Number getValue() {
        return value;
    }

    public int getMeasureTime() {
        return measureTime;
    }

    public String getSource() {
        return source;
    }

    @Override
    public String getToplevelName() {
        return "gauges";
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> value = Maps.newHashMap();

        value.put("name", getName());
        value.put("value", getValue());
        value.put("source", getSource());
        value.put("measure_time", getMeasureTime());

        return value;
    }

}
