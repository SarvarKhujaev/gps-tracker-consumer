package com.ssd.mvd.entity;

import com.ssd.mvd.interfaces.EntityToCassandraConverter;
import com.ssd.mvd.constants.CassandraTables;
import java.util.UUID;

public final class PoliceType implements EntityToCassandraConverter {
    private UUID uuid;
    private String icon;
    private String icon2;
    private String policeType;

    public PoliceType() {}

    @Override
    @lombok.NonNull
    public CassandraTables getEntityTableName () {
        return CassandraTables.POLICE_TYPE;
    }
}
