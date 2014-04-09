(ns com.tbaldridge.hermod.ping-server
  (:require [clojure.core.async :refer [chan <!! >!! go alt!! timeout]]
            [com.tbaldridge.hermod :refer :all]))

(defn ping-mailbox [name]
  (go
   (with-open [m (mailbox name)]
     (loop []
       (when-let [{:keys [return-to msg]} (<! m)]
         (>! return-to msg)
         (recur))))))

(defn start-server []
  (println "starting server")
  ;; Start listening on 4242
  (listen 4242)

  ;; Create a local box with a known name, and wire up a echo service
  (ping-mailbox :ping-box)
  (Thread/sleep 100000))

(defn start-client []
  (println "starting client")
  ;; Create a pointer to the :ping-box
  (let [rbx (remote-mailbox "localhost" 4242 :ping-box)]

    ;; Create a response box
    (with-open [lbx (mailbox)]
      (dotimes [x 10000]
        (println ".")
        (dotimes [x 100]
          ;; Send to the remote box and wait for the reply. In a request/response
          ;; situation, like this. We will alt on a timeout and throw an exception
          ;; if the message is dropped. We could also resend and wait again.
          (>!! rbx {:return-to lbx
                    :msg x})
          (alt!! [lbx] ([v] (println v))
                 [(timeout 1000)] (println "Timeout after 1000 ms")))))))

(defn -main [client-or-server]
  (if (= client-or-server "server")
    (start-server)
    (start-client)))
