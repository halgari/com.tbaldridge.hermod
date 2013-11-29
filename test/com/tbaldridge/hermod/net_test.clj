(ns com.tbaldridge.hermod.net-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!! >!! go]]
            [com.tbaldridge.hermod.impl.internals :refer :all]
            [com.tbaldridge.hermod :as net])
  (:import [java.nio ByteBuffer]))

(deftest bind-connect
  (restart-selector!)
  (let [a (atom 0)]
    (bind "localhost" 8080 (fn [x] (swap! a inc)))
    (connect "localhost" 8080 (fn [x] (swap! a inc)))
    (Thread/sleep 100)
    (is (= @a 2)))
  (restart-selector!))

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
    (bind "localhost" 8080 client-p)
    (connect "localhost" 8080 connect-p)
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
    (bind "localhost" 8080 client-p)
    (connect "localhost" 8080 connect-p)
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


(deftest local-mailbox-tests
  (restart-selector!)
  (testing "send recv directly"
    (with-open [mb (net/mailbox)]
      (>!! mb 42)
      (is (= (<!! mb) 42))))

  (testing "registration"
    (is (= nil (get *mailboxes* :foo)))
    (with-open [mb (net/mailbox :foo)]
      (is (not= nil (get *mailboxes* :foo))))
    (is (= nil (get *mailboxes* :foo))))

    (restart-selector!))

(deftest remote-mailbox-tests
  (restart-selector!)

  (testing "send recv"
    (restart-selector!)
    (listen 8080)
    (with-open [mb (net/mailbox :foo)]
      (>!! (remote-mailbox "localhost" 8080 :foo) 42)
      (is (= (<!! mb) 42))))


  (testing "can send mailboxes to mailboxes"
    (restart-selector!)
    (listen 8080)

    (with-open [mb (net/mailbox :foo)]
      (go (let [{:keys [respond-to message]} (<! mb)]
            (>! respond-to message)))

      (let [remote-box (remote-mailbox "localhost" 8080 :foo)]
        (with-open [respond-to (net/mailbox)]
          (>!! remote-box
               {:respond-to respond-to
                :message "echo this"})
          (is (= (<!! respond-to) "echo this"))))))
  (restart-selector!))
