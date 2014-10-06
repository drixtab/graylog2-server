package org.graylog2.rest.resources.tools.responses;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect
public class SubstringTesterResponse {
    public boolean successful;
    public String cut;
    @JsonProperty("begin_index")
    public int beginIndex;
    @JsonProperty("end_index")
    public int endIndex;
}
