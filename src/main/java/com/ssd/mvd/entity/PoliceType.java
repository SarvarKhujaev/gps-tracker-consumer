package com.ssd.mvd.entity;

import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.interfaces.EntityToCassandraConverter;

import java.util.UUID;

public final class PoliceType
        implements EntityToCassandraConverter {
    private UUID uuid;
    private String icon;
    private String icon2;
    private String policeType;

    public PoliceType() {}

    @Override
    public CassandraTables getEntityTableName () {
        return CassandraTables.POLICE_TYPE;
    }

    @Override
    public CassandraTables getEntityKeyspaceName () {
        return CassandraTables.TABLETS;
    }
}
