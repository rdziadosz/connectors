package io.delta.flink.e2e.data;

public final class UserBuilder {
    private String name;
    private String surname;
    private String country;
    private int birthYear;

    private UserBuilder() {
    }

    public static UserBuilder anUser() {
        return new UserBuilder();
    }

    public UserBuilder name(String name) {
        this.name = name;
        return this;
    }

    public UserBuilder surname(String surname) {
        this.surname = surname;
        return this;
    }

    public UserBuilder country(String country) {
        this.country = country;
        return this;
    }

    public UserBuilder birthYear(int birthYear) {
        this.birthYear = birthYear;
        return this;
    }

    public User build() {
        return new User(name, surname, country, birthYear);
    }
}
