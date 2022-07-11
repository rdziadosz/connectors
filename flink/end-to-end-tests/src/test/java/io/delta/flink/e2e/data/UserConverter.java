package io.delta.flink.e2e.data;

import org.apache.flink.types.Row;

public class UserConverter {

    public static Row toRow(User user) {
        return Row.of(user.getName(), user.getSurname(), user.getCountry(), user.getBirthYear());
    }

    public static User fromRow(Row row) {
        return UserBuilder.anUser()
            .name((String) row.getField(0))
            .surname((String) row.getField(1))
            .country((String) row.getField(2))
            .birthYear((int) row.getField(3))
            .build();
    }

}
