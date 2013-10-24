(ns clojure.core.async.net-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!! >!!]]
            [clojure.core.async.net :refer :all])
  (:import [java.nio ByteBuffer]))

(deftest bind-connect
  (let [a (atom 0)]
    (bind "localhost" 8080 (fn [x] (swap! a inc)))
    (connect "localhost" 8080 (fn [x] (swap! a inc)))
    (Thread/sleep 100)
    (is (= @a 2))))

(defn string->byte-buffer ^ByteBuffer [^String s]
  (let [bb (-> s
               (.getBytes "UTF-8")
               (ByteBuffer/wrap))]
    #_(.flip bb)
    bb))

(defn byte-buffer->string [^ByteBuffer b]
  (-> b
      .array
      (String. "UTF-8")))

(deftest send-recv
  (let [a (atom 0)
        client-p (promise)
        connect-p (promise)]
    (bind "localhost" 8081 client-p)
    (connect "localhost" 8081 connect-p)
    (let [put-chan (chan 100)
          take-chan (chan 100)]
      (chan->socket put-chan @client-p)
      (socket->chan @connect-p take-chan 1024)
      (dotimes [x 3000]
        (let [data (vec (range x))]
          (println "putting " x)
          (>!! put-chan data)
          (println x)
          (is (= data
                 (<!! take-chan))))))))
