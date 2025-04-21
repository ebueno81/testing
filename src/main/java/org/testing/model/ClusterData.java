package org.testing.model;

import lombok.Data;

@Data
public class ClusterData {

    private String email;
    private String clusterCampana;
    private String usuarioCreacion;

    public ClusterData() {
    }

    public ClusterData(String email, String clusterCampana, String usuarioCreacion) {
        this.email = email;
        this.clusterCampana = clusterCampana;
        this.usuarioCreacion=usuarioCreacion;
    }
}