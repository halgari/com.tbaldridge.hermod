(ns clojure.core.async.net-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!! >!! go]]
            [clojure.core.async.net.impl.internals :refer :all]
            [clojure.core.async.net :as net])
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
                 (<!! take-chan)))))))
  (restart-selector!))


(deftest send-recv-loop-test
  (restart-selector!)
  (let [a (atom 0)
        client-p (promise)
        connect-p (promise)]
    (bind "localhost" 8082 client-p)
    (connect "localhost" 8082 connect-p)
    (let [put-local-chan (chan 100)
          take-local-chan (chan 100)
          put-remote-chan (chan 100)
          take-remote-chan (chan 100)]
      (chan->socket put-local-chan @client-p)
      (socket->chan @client-p take-local-chan)

      (socket->chan @connect-p take-remote-chan)
      (chan->socket put-remote-chan @connect-p)

      (go (while true
            (>! put-remote-chan (<! take-remote-chan))))

      (dotimes [x 300]
        (let [data (vec (range x))]
          (>!! put-local-chan {:data data})
          (is (= {:data data}
                 (<!! take-local-chan))))))))


(deftest local-mailbox
  (testing "send recv directly"
    (with-open [mb (net/mailbox)]
      (>!! mb 42)
      (is (= (<!! mb) 42))))

  (testing "registration"
    (is (= nil (get *mailboxes* :foo)))
    (with-open [mb (net/mailbox :foo)]
      (is (not= nil (get *mailboxes* :foo))))
    (is (= nil (get *mailboxes* :foo)))))

(deftest remote-mailbox-tests
  (testing "send recv"
    (restart-selector!)
    (listen 8083)
    (with-open [mb (net/mailbox :foo)]
      (>!! (remote-mailbox "localhost" 8083 :foo) 42)
      (is (= (<!! mb) 42))))

  (testing "can send mailboxes to mailboxes"
    (restart-selector!)
    (listen 8084)

    (with-open [mb (net/mailbox :foo)]
      (go (let [{:keys [respond-to message]} (<! mb)]
            (>! respond-to message)))

      (let [remote-box (remote-mailbox "localhost" 8084 :foo)]
        (with-open [respond-to (net/mailbox)]
          (>!! remote-box
               {:respond-to respond-to
                :message "echo this"})
          (is (= (<!! respond-to) "echo this")))))))
