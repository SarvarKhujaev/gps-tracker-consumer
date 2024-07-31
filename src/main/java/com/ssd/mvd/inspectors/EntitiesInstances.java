package com.ssd.mvd.inspectors;

import com.ssd.mvd.entity.*;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.entity.patrulDataSet.PatrulFuelStatistics;

/*
хранит instance на все объекты
*/
public final class EntitiesInstances {
    public static final Icons ICONS = new Icons();
    public static final Patrul PATRUL = new Patrul();
    public static final ReqCar REQ_CAR = new ReqCar();
    public static final PoliceType POLICE_TYPE = new PoliceType();
    public static final TupleOfCar TUPLE_OF_CAR = new TupleOfCar();
    public static final TrackerInfo TRACKER_INFO = new TrackerInfo();
    public static final PositionInfo POSITION_INFO = new PositionInfo();
    public static final PatrulFuelStatistics PATRUL_FUEL_STATISTICS = new PatrulFuelStatistics();
}
