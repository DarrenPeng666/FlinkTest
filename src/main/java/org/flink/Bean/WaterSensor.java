package org.flink.Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor implements Serializable {
    public String id;
    public Long ts;
    public Integer vc;

}

