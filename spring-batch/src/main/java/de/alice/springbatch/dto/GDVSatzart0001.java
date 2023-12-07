package de.alice.springbatch.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
public class GDVSatzart0001 extends GDV {

    private String absender;
    private String adressat;
}
