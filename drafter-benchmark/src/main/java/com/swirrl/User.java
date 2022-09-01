package com.swirrl;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

/**
 * Wrapper around a drafter user representation
 */
public class User {
    private final Object user;

    private User(Object user) {
        this.user = user;
    }

    /**
     * @return The internal drafter representation of this user
     */
    public Object obj() { return this.user; }

    /**
     * Creates a new user with the given email and role
     * @param email The user email
     * @param roleName Representation of the user role e.g. "editor" or "publisher"
     * @return The representation of the created user
     */
    public static User create(String email, String roleName) {
        Util.require("drafter.user");
        IFn createFn = Clojure.var("drafter.user", "create-authenticated-user");
        return new User(createFn.invoke(email, Util.keyword(roleName)));
    }

    /**
     * Creates a test user in the publisher role
     * @return The test publisher user
     */
    public static User publisher() {
        return create("publisher@swirrl.com", "publisher");
    }
}
