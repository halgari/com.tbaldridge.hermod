(ns com.tbaldridge.hermod.impl.internals
  (:import [java.net InetSocketAddress]
           [java.util.concurrent Executors]
           [java.io Closeable]
           [java.nio ByteBuffer]
           [java.nio.channels Selector SelectionKey SelectableChannel ServerSocketChannel SocketChannel]
           [org.fressian.handlers WriteHandler ReadHandler]
           [org.fressian Writer Reader])
  (:require [clojure.core.async :refer [put! take! chan thread dropping-buffer go close! <!! >!!]]
            [clojure.core.async.impl.dispatch :as dispatch]
            [clojure.data.fressian :as fressian]
            [clojure.stacktrace :as st]
            [clojure.core.async.impl.protocols :as async-impl]))

(set! *warn-on-reflection* true)

(declare net-write-handlers)
(declare net-read-handlers)

(def ^:dynamic *active-addr*)
(def ^:dynamic *remote-connections* (atom {}))



(defn reset-old!
  "Like reset! but returns the value of the atom before the reset happens"
  [a new-val]
  (loop [o @a]
    (if (compare-and-set! a o new-val)
      o
      (recur @a))))


(defn dispatch-ops
  "Dispatches the correct function in the .attachment based on the SelectionKey state"
  [^SelectionKey k selector]
  (when (.isReadable k)
    ((get (.attachment k) SelectionKey/OP_READ) k selector))
  (when (.isWritable k)
    ((get (.attachment k) SelectionKey/OP_WRITE) k selector))
  (when (.isConnectable k)
    ((get (.attachment k) SelectionKey/OP_CONNECT) k selector)
    (.cancel k))
  (when (.isAcceptable k)
    ((get (.attachment k) SelectionKey/OP_ACCEPT) k selector)))

(defprotocol ISelector
  (start-control-thread [this])
  (enqueue-op [this channel flags f])
  (shutdown [this c]))

(deftype SelectorControl [^Selector selector
                          ops
                          kill-chan]
  ISelector
  (start-control-thread [this]
    (thread
     (try
       (loop []
         ;; Handle new ops
         (doseq [[^SelectableChannel c ^int flag ^SelectionKey f] (reset-old! ops [])]
           (.configureBlocking c false)
           (if (.isRegistered c)
             (let [^SelectionKey key (.keyFor c selector)]
               (.interestOps key (bit-or (.interestOps key) flag))
               (.attach key (assoc (.attachment key) flag f)))
             (.register c selector flag {flag f})))
         ;; handle selector state changes
         (when-not (= 0 (.select selector))
           (let [iterator (.iterator (.selectedKeys selector))]
             (while (.hasNext iterator)
               (let [key (.next iterator)]
                 (dispatch-ops key selector)
                 (.remove iterator)))))

         ;; If we're dead, shutdown
         (if @kill-chan
           (do
             (doseq [k (-> selector
                           .keys
                           .iterator
                           iterator-seq)]
               (.close (.channel ^SocketChannel k)))
             (.close selector)
             (close! @kill-chan))
           (recur)))
       (catch Throwable ex
         (println ex)
         (st/print-stack-trace ex))))
    this)

  (enqueue-op [this channel flags f]
    (swap! ops conj [channel flags f])
    (.wakeup selector))
  (shutdown [this c]
    (if @kill-chan
      (assert false "already shutting down")
      (do (reset! kill-chan c)

          (.wakeup selector)))
    c))

(def ^:dynamic *selector* (start-control-thread (SelectorControl. (Selector/open)
                                                                  (atom [])
                                                                  (atom nil))))

(defn restart-selector! []
  (reset! *remote-connections* {})
  (alter-var-root #'*selector*
                  (fn [x]
                    (<!! (shutdown x (chan 1)))
                    (start-control-thread
                          (SelectorControl. (Selector/open)
                                            (atom [])
                                            (atom nil))))))

(defn bind [^String addr port handler-fn]
  (let [server (ServerSocketChannel/open)]
    (.configureBlocking server false)
    (-> server .socket (.bind (InetSocketAddress. addr (long port))))
    (enqueue-op *selector* server SelectionKey/OP_ACCEPT
                (fn [_ _]
                  (let [client (.accept server)]
                    (handler-fn client))))))


(defn connect [^String addr port handler-fn]
  (let [channel (SocketChannel/open (InetSocketAddress. addr (long port)))]
    (.configureBlocking channel false)
    (if (.isConnected channel)
      (handler-fn channel)
      (enqueue-op *selector* channel SelectionKey/OP_CONNECT
                  (fn [_ _]
                    (handler-fn channel))))))

(defn defressian [^SocketChannel channel data]
  (binding [*active-addr* (-> channel
                              .socket
                              .getRemoteSocketAddress)]
    (fressian/read data :handlers net-read-handlers)))

(defn socket->chan
  "Enables machinery that reads from a socket, deserializes the data,
   and writes it to a core.async channel."
  [^SocketChannel channel c]
  (let [flag-chan (chan (dropping-buffer 1))
        read-fn (fn read-fn [^ByteBuffer out-buffer out-chan]
                  (.read channel out-buffer)
                  (if (.hasRemaining out-buffer)
                    (take! flag-chan (fn [_]
                                       (read-fn out-buffer out-chan)))
                    (do
                      (.flip out-buffer)
                      (put! out-chan out-buffer)))
                  out-chan)]
    (go
     (try
       (let [tmp-c (chan 1)]
         (loop []
           (let [s (<! (read-fn (ByteBuffer/allocate 4) tmp-c))
                 data-size (.getInt ^ByteBuffer s)
                 data (<! (read-fn (ByteBuffer/allocate data-size) tmp-c))
                 obj (defressian channel data)]
             (>! c obj))
           (recur)))
       (catch Throwable ex
         (close! c)
         (println ex)
         (clojure.stacktrace/print-stack-trace ex)
         (println "----"))))
    (enqueue-op *selector* channel SelectionKey/OP_READ
                (fn [_ _]
                  (put! flag-chan :ping)))))


(defn chan->socket
  "Writes objects received from c to the given socket."
  [c ^SocketChannel channel]
  (let [flag-chan (chan (dropping-buffer 1))
        enable-chan (chan)]
    (enqueue-op *selector* channel SelectionKey/OP_WRITE
                (fn [^SelectionKey k ^Selector s]
                  (.interestOps k (-> SelectionKey/OP_WRITE
                                      bit-not
                                      (bit-and (.interestOps k))))
                  (put! flag-chan :ping)
                  (take! enable-chan (fn [_]
                                       (.interestOps k (bit-or SelectionKey/OP_WRITE
                                                               (.interestOps k)))))))
    (go (try (loop []
               (when-let [v (<! c)]
                 (let [binary (fressian/write v :handlers net-write-handlers)
                       out-buff (ByteBuffer/allocate (+ (.limit binary) 4))]
                   (.putInt out-buff (.limit binary))
                   (.put out-buff binary)
                   (.flip out-buff)
                   (while (.hasRemaining ^ByteBuffer out-buff)
                     (<! flag-chan)
                     (.write ^SocketChannel channel ^ByteBuffer out-buff)
                     (>! enable-chan :ping)))
                 (recur)))
             (catch Throwable ex
               (println ex)
               (clojure.stacktrace/print-stack-trace ex)
               (println "----"))))))


(defprotocol IMailboxRegistry
  (register! [this mailbox])
  (un-register! [this mailbox-name]))


(defprotocol INamed
  (-name [this]))

(deftype MailboxRegistry [mailboxes]
  clojure.lang.ILookup
  (valAt [this key]
    (.valAt this key nil))
  (valAt [this key notFound]
    (get @mailboxes key notFound))
  IMailboxRegistry
  (register! [this mailbox]
    (swap! mailboxes assoc (-name mailbox) mailbox))
  (un-register! [this mailbox-name]
    (swap! mailboxes dissoc mailbox-name)))

(def ^:dynamic *mailboxes* (MailboxRegistry. (atom {})))


(deftype LocalMailbox [chan name-sym]
  INamed
  (-name [this]
    name-sym)
  async-impl/WritePort
  (put! [port val fn0-handler]
    (async-impl/put! chan val fn0-handler))
  async-impl/ReadPort
  (take! [port fn1-handler]
    (async-impl/take! chan fn1-handler))
  async-impl/Channel
  (close! [this]
    (un-register! *mailboxes* (-name this))
    (async-impl/close! chan))
  java.io.Closeable
  (close [this]
    (close! this)))


(defn mailbox
  ([name buffer]
     (let [mb (LocalMailbox. (chan buffer) name)]
       (register! *mailboxes* mb)
       mb)))

(defn dispatcher [c]
  (go
   (try
     (loop []
       (when-let [{:keys [mailbox message] :as orig} (<! c)]
         (let [mb (if (extends? async-impl/WritePort (class mailbox))
                    mailbox
                    (get *mailboxes* mailbox))]
           (put! mb message))
         (recur)))
     (catch Throwable ex
       (println ex)))))

(defn connect-and-wire [^InetSocketAddress addr c]
  (connect (.getHostString addr)
           (.getPort addr)
           (fn [socket]
             (chan->socket c socket)
             (let [c (chan 1024)]
               (socket->chan socket c)
               (dispatcher c))))
  c)

(defn get-or-connect [^InetSocketAddress addr]
  (loop [rc @*remote-connections*]
    (if-let [c (get rc addr)]
      c
      (let [c (chan 1)
            new-state (assoc rc addr c)]
        (if (compare-and-set! *remote-connections* rc new-state)
          (connect-and-wire addr c)
          (recur @*remote-connections*))))))

(defprotocol IHost
  (host [this]))

(deftype RemoteMailbox [addr name-sym]
  INamed
  (-name [this]
    name-sym)
  IHost
  (host [this]
    addr)
  async-impl/WritePort
  (put! [this val fn0-handler]
    (let [msg {:mailbox (-name this)
               :message val}
          c (get-or-connect addr)]
      (async-impl/put! c msg fn0-handler))))

(deftype ForwadingMailbox [addr remote-box]
  async-impl/WritePort
  (put! [this val fn0-handler]
    (let [msg {:mailbox remote-box
               :message val}
          c (get-or-connect addr)]
      (assert c)
      (async-impl/put! c msg fn0-handler))))

(defn forwarding-mailbox [^String host ^Long port remote-box]
  (ForwadingMailbox. (InetSocketAddress. host port) remote-box))


(defn listen
  "Opens a local TCP port and listens to incoming message deliveries."
  ([port]
     (bind "localhost" port
           (fn [^SocketChannel socket]
             (let [in-c (chan)
                   out-c (chan)]
               (chan->socket out-c socket)
               (socket->chan socket in-c)
               (dispatcher in-c)
               (swap! *remote-connections* assoc (-> socket
                                                     .socket
                                                     .getRemoteSocketAddress)
                      out-c))))))



(defn remote-mailbox
  "Creates a pointer to a remote mailbox.
   [host port name]

  host - the hostname (or ip) of the remote mailbox
  port - the remote ip port of the mailbox
  name - the name of the remote mailbox

  Remote mailboxes may be sent messages via core.async put methods"
  [^String host ^Long port name]
  (RemoteMailbox. (InetSocketAddress. host port) name))


(def net-read-handlers
  (-> (merge fressian/clojure-read-handlers
             {"lmb"
              (reify ReadHandler (read [_ rdr tag _]
                                   (RemoteMailbox. *active-addr* (.readObject rdr))))}
             {"rmb"
              (reify ReadHandler (read [_ rdr tag _]
                                   (RemoteMailbox. (.readObject rdr) (.readObject rdr))))}
             {"inetaddr"
              (reify ReadHandler (read [_ rdr tag _]
                                   (InetSocketAddress. ^String (.readObject rdr) (int (.readObject rdr)))))})
      fressian/associative-lookup))

(def net-write-handlers
  (-> (merge fressian/clojure-write-handlers
             {InetSocketAddress
              {"inetaddr"
               (reify WriteHandler (write [_  w addr]
                                     (.writeTag w "inetaddr" 2)
                                     (.writeObject w (.getHostString ^InetSocketAddress addr))
                                     (.writeObject w (.getPort ^InetSocketAddress addr))))}}
             {LocalMailbox
              {"lmb"
               (reify WriteHandler (write [_  w mb]
                                     (.writeTag w "lmb" 1)
                                     (.writeObject w (-name mb))))}}
             {RemoteMailbox
              {"rmb"
               (reify WriteHandler (write [_ w rmb]
                                     (.writeTag w "rmb" 2)
                                     (.writeObject w (host rmb))
                                     (.writeObject w (-name rmb))))}})
      fressian/associative-lookup))
