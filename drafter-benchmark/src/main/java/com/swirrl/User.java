package com.swirrl;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class User {
    private final Object user;

    private User(Object user) {
        this.user = user;
    }

    public Object obj() { return this.user; }

    public static User create(String email, String roleName) {
        Util.require("drafter.user");
        IFn createFn = Clojure.var("drafter.user", "create-authenticated-user");
        return new User(createFn.invoke(email, Util.keyword(roleName)));
    }

    public static User publisher() {
        return create("publisher@swirrl.com", "publisher");
    }
}
