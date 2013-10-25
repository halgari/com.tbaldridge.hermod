(ns clojure.core.async.net
  (:import [java.net InetSocketAddress]
           [java.util.concurrent Executors]
           [java.io Closeable]
           [java.nio ByteBuffer]
           [java.nio.channels Selector SelectionKey SelectableChannel ServerSocketChannel SocketChannel])
  (:require [clojure.core.async :refer [put! take! chan thread dropping-buffer go close! <!! >!!]]
            [clojure.core.async.impl.dispatch :as dispatch]
            [clojure.core.async.net.impl.fressian :as fressian]
            [clojure.stacktrace :as st]
            [clojure.core.async.impl.protocols :as async-impl]))


#_(defn put!-or-die [c val]
    (alt!! [[c val]] ([] :ok)
           :default ([] (throw (AssertionError. "Put to blocked channel")))))

(def control-chan (chan 1))

(defn reset-old! [a new-val]
  (loop [o @a]
    (if (compare-and-set! a o new-val)
      o
      (recur @a))))

(defn key-type [^SelectionKey k]
  (cond
   (.isConnectable k) :connect
   (.isAcceptable k) :accept
   (.isReadable k) :read
   (.isWritable k) :write))

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
         (doseq [[^SelectableChannel c ^int flags ^SelectionKey f] (reset-old! ops [])]
           (println "add op" flags c)
           (.configureBlocking c false)
           (.register c selector flags f))
         (when-not (= 0 (.select selector))
           (let [iterator (.iterator (.selectedKeys selector))]
             #_(println (.hasNext iterator))
             (while (.hasNext iterator)
               #_(println "-----")
               (let [key (.next iterator)]
                 #_(println (.isConnectable key) (.isAcceptable key))
                 ((.attachment key) key selector)
                 (when (.isConnectable key)
                   (.cancel key))
                 (.remove iterator)))))
         (if @kill-chan
           (do
             (println "kill")
             (doseq [k (-> selector
                             .selectedKeys
                             .iterator
                             iterator-seq)]
                 (-> (.channel k)
                     .close))
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
    (println "lil" kill-chan c)
    (if @kill-chan
      (close! @kill-chan)
      (do (reset! kill-chan c)
          (println kill-chan)
          (.wakeup selector)))
    c))

(def ^:dynamic *selector* (start-control-thread (SelectorControl. (Selector/open) (atom []) (atom nil))))

(defn restart-selector! []
  (<!! (shutdown *selector* (chan 1)))
  (alter-var-root #'*selector* (fn [x] (start-control-thread (SelectorControl. (Selector/open) (atom []) (atom nil))))))

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


(defn socket->chan [^SocketChannel channel c]
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
     (let [tmp-c (chan 1)]
       (loop []
         (let [s (<! (read-fn (ByteBuffer/allocate 4) tmp-c))
               data-size (.getInt ^ByteBuffer s)
               data (<! (read-fn (ByteBuffer/allocate data-size) tmp-c))
               obj (fressian/defressian data :handlers fressian/clojure-read-handlers)]
           (>! c obj))
         (recur))))
    (enqueue-op *selector* channel SelectionKey/OP_READ
                (fn [_ _]
                  (put! flag-chan :ping)))))

(defn chan->socket [c ^SocketChannel channel]
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
                 (let [binary (fressian/byte-buf v :handlers fressian/clojure-write-handlers)
                       out-buff (ByteBuffer/allocate (+ (.limit binary) 4))]
                   (.putInt out-buff (.limit binary))
                   (.put out-buff binary)
                   (.flip out-buff)
                   (while (.hasRemaining ^ByteBuffer out-buff)
                     (<! flag-chan)
                     (.write ^SocketChannel channel out-buff)
                     (>! enable-chan :ping)))
                 (recur)))
             (catch Throwable ex
               (println ex))))))


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
  ([]
     (mailbox (symbol (str (java.util.UUID/randomUUID)))))
  ([name]
     (mailbox name (dropping-buffer 1024)))
  ([name buffer]
     (let [mb (LocalMailbox. (chan buffer) name)]
       (register! *mailboxes* mb)
       mb)))


(def ^:dynamic *remote-connections* (atom {}))

(defn dispatcher [c]
  (go
   (loop []
     (println "dispatch loop")
     (when-let [{:keys [mailbox message] :as orig} (<! c)]
       (println "dispatch got" orig)
       (let [mb (get *mailboxes* mailbox)]
         (put! mb message))
       (recur)))))

(defn connect-and-wire [^InetSocketAddress addr c]
  (println "connecting to " addr)
  (connect (.getHostString addr)
           (.getPort addr)
           (fn [socket]
             (println "connected to" addr)
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

(deftype RemoteMailbox [addr name-sym]
  INamed
  (-name [this]
    name-sym)
  async-impl/WritePort
  (put! [this val fn0-handler]
    (println "remote put")
    (let [msg {:mailbox (-name this)
               :message val}
          c (get-or-connect addr)]
      (async-impl/put! c msg fn0-handler))))


(defn listen [port]
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
                   out-c)))))

(defn remote-mailbox [host port name]
  (RemoteMailbox. (InetSocketAddress. host port) name))
