package io.delta.flink.e2e.data;

public class User {

    private final String name;
    private final String surname;
    private final String country;
    private final int birthYear;

    public User(String name, String surname, String country, int birthYear) {
        this.name = name;
        this.surname = surname;
        this.country = country;
        this.birthYear = birthYear;
    }

    public String getName() {
        return name;
    }

    public String getSurname() {
        return surname;
    }

    public String getCountry() {
        return country;
    }

    public int getBirthYear() {
        return birthYear;
    }
}
