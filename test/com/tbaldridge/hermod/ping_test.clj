(ns com.tbaldridge.hermod
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!! >!! go alt!! timeout]]
            [com.tbaldridge.hermod :refer :all]))

(defn ping-mailbox [name]
  (go
   (with-open [m (mailbox name)]
     (loop []
       (when-let [{:keys [return-to msg]} (<! m)]
         (>! return-to msg)
         (recur))))))
kjghl
(deftest ping-test
  ;; Only used during testing to make sure we have a clean state
  (restart-selector!)

  ;; Start listening on 4242
  (listen 4242)

  ;; Create a local box with a known name, and wire up a echo service
  (ping-mailbox :ping-box)


  ;; Create a pointer to the :ping-box
  (let [rbx (remote-mailbox "localhost" 4242 :ping-box)]

    ;; Create a response box
    (with-open [lbx (mailbox)]
      (dotimes [x 100]
        ;; Send to the remote box and wait for the reply. In a request/response
        ;; situation, like this. We will alt on a timeout and throw an exception
        ;; if the message is dropped. We could also resend and wait again.
        (>!! rbx {:return-to lbx
                  :msg x})
        (alt!! [lbx] ([v] (is (= x v)))
               [(timeout 1000)] (assert false "Timeout after 1000 ms"))))))
