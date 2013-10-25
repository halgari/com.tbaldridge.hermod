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
  (restart-selector!)
  (let [a (atom 0)
        client-p (promise)
        connect-p (promise)]
    (bind "localhost" 8081 client-p)
    (connect "localhost" 8081 connect-p)
    (let [put-chan (chan 100)
          take-chan (chan 100)]
      (chan->socket put-chan @client-p)
      (socket->chan @connect-p take-chan)
      (dotimes [x 300]
        (let [data (vec (range x))]
          (>!! put-chan {:data data})
          (is (= {:data data}
                 (<!! take-chan))))))))


(deftest local-mailbox
  (testing "send recv directly"
    (with-open [mb (mailbox)]
      (>!! mb 42)
      (is (= (<!! mb) 42))))

  (testing "registration"
    (is (= nil (get *mailboxes* :foo)))
    (with-open [mb (mailbox :foo)]
      (is (not= nil (get *mailboxes* :foo))))
    (is (= nil (get *mailboxes* :foo)))))

#_(deftest remote-mailbox-tests
  (testing "send recv"
    (restart-selector!)
    (listen 8080)
    (with-open [mb (mailbox :foo)]
      (>!! (remote-mailbox "localhost" 8080 :foo) 42)
      (is (= (<!! mb) 42)))))
