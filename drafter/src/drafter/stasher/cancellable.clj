(ns drafter.stasher.cancellable)

(defprotocol Cancellable
  (cancel [this]))
