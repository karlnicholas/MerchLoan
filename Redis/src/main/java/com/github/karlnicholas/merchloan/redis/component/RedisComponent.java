package com.github.karlnicholas.merchloan.redis.component;

import jakarta.enterprise.context.ApplicationScoped;
import redis.clients.jedis.JedisPooled;

import java.nio.ByteBuffer;
import java.time.LocalDate;

@ApplicationScoped
public class RedisComponent {
    private final JedisPooled jedis;

    public RedisComponent() {
        jedis = new JedisPooled("localhost", 6379);
    }


    public void updateBusinessDate(LocalDate businessDate) {
        jedis.set(encodeKey(1L).array(), encodeValue(businessDate).array());
    }

    public LocalDate getBusinessDate() {
        return decodeValue(ByteBuffer.wrap(jedis.get(encodeKey(1L).array())));
    }

    private LocalDate decodeValue(ByteBuffer byteBuffer) {
        byteBuffer.rewind();
        return LocalDate.of(byteBuffer.getInt(), byteBuffer.getInt(), byteBuffer.getInt());
    }

    private ByteBuffer encodeKey(Long aLong) {
        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
        return bb.putLong(aLong);
    }

    private ByteBuffer encodeValue(LocalDate localDate) {
        ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES*3);
        bb.putInt(localDate.getYear());
        bb.putInt(localDate.getMonthValue());
        return bb.putInt(localDate.getDayOfMonth());
    }
}
