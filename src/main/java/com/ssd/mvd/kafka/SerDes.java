package com.ssd.mvd.kafka;

import lombok.Data;
import com.google.gson.Gson;

import com.ssd.mvd.entity.ReqCar;
import com.ssd.mvd.entity.Position;
import com.ssd.mvd.entity.TupleOfCar;

@Data
public class SerDes {
    private final Gson gson = new Gson();
    private static SerDes serDes = new SerDes();

    public static SerDes getSerDes () { return serDes != null ? serDes : ( serDes = new SerDes() ); }

    public String serialize ( ReqCar object ) { return this.getGson().toJson( object ); }

    public String serialize ( Position object ) { return this.getGson().toJson( object ); }

    public String serialize ( TupleOfCar object ) { return this.getGson().toJson( object ); }

    public Position deserialize ( String object ) { return this.getGson().fromJson( object, Position.class ); }
}
