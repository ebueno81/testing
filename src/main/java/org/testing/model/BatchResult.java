package org.testing.model;

import lombok.Data;

@Data
public class BatchResult {
    private int created;
    private int updated;
    private int total;
}