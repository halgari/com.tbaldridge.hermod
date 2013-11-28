(ns com.tbaldridge.hermod
  (:require [com.tbaldridge.hermod.impl.internals :as internals]
            [clojure.core.async :refer [dropping-buffer]]))


(defn mailbox
  "Returns a mailbox after it has been registered with the mailbox registry.
   Puts to mailboxes are non-blocking and may drop messages if network buffers
   are full. Takes from mailboxes may block.

   Mailboxes implement java.io.Closeable and so may be used with with-open blocks.

   Mailboxes implmeent clojure.core.async.impl.protocols/Channel and so may be used
   as an argument to close!

  [name buffer]

  name - optional, name for the mailbox (should be unique to this JVM instance)
  buffer - optional, a buffer to use for delivered messages, must be non-blocking"
  ([]
     (mailbox (symbol (str (java.util.UUID/randomUUID)))))
  ([name]
     (mailbox name (dropping-buffer 1024)))
  ([name buffer]
     (internals/mailbox name buffer)))

(defn remote-mailbox
  "Creates a pointer to a remote mailbox.
   [host port name]

  host - the hostname (or ip) of the remote mailbox
  port - the remote ip port of the mailbox
  name - the name of the remote mailbox

  Remote mailboxes may be sent messages via core.async put methods"
  [host port name]
  (internals/remote-mailbox host port name))

(defn listen
  "Opens a local TCP port and listens to incoming message deliveries."
  [port]
  (internals/listen port))


(defn restart-selector!
  "Restarts the selector thread and closes all connections. This should only need to be done during testing. "
  []
  (internals/restart-selector!))
