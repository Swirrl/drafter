(ns drafter.user.repository)

(defprotocol UserRepository
  (find-user-by-email-address [this email]
    "Attempts to find a user with the given email address (user name)
    in the underlying store. Returns nil if no such user was found."))
