package com.pge.krakencis.models.profilereads;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProfileReadPayload {
    private String externalId;
    private String sourceHes;
    private String profileName;
    private List<String> registers;
    private List<String> readings;
}
