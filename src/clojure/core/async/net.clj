(ns clojure.core.async.net
  (:import [java.net InetSocketAddress]
           [java.util.concurrent Executors]
           [io.netty.bootstrap Bootstrap ServerBootstrap]
           [io.netty.buffer ByteBuf]
           [io.netty.channel ChannelInboundHandlerAdapter ChannelPipeline ChannelOption ChannelInitializer ChannelHandler]
           [io.netty.channel.nio NioEventLoopGroup]
           [io.netty.channel.socket.nio NioServerSocketChannel NioSocketChannel]
           [java.nio ByteBuffer]
           [java.nio.channels Selector SelectionKey SelectableChannel ServerSocketChannel SocketChannel])
  (:require [clojure.core.async :refer [put! take! chan thread dropping-buffer go]]
            [clojure.core.async.impl.dispatch :as dispatch]
            [clojure.core.async.net.impl.fressian :as fressian]
            [clojure.stacktrace :as st]))
(comment

  (defprotocol IStartStop
    (start [this])
    (stop [this]))

  (defprotocol IServer
    (-bind [this ip port]))

  (defn new-handler []
    (proxy [ChannelInboundHandlerAdapter]
        []
      (channelRead [ctx ^ByteBuf msg]
        (println "Connect" msg)
        (.release msg))
      (exceptionCaught [context ^Throwable cause]
        (.printStackTrace cause)
        (.close context))))

  (defn add-remote-endpoint [rep ^InetSocketAddress addr ^SocketChannel chan]
    )


  (defrecord UnifiedBootstrap [^Bootstrap client-bootstrap
                               ^ServerBootstrap server-bootstrap
                               ^NioEventLoopGroup boss-group
                               ^NioEventLoopGroup worker-group
                               remote-endpoints]
    (start [this]
      (-> server-bootstrap
          (.group boss-group worker-group)
          (.channel NioServerSocketChannel)
          (.childHandler (proxy [ChannelInitializer]
                             []
                           (initChannel [channel]
                             (-> channel
                                 .pipeline
                                 (.addLast (into-array ChannelHandler [(new-handler)]))))))
          (.childOption ChannelOption/TCP_NODELAY true)
          (.childOption ChannelOption/SO_KEEPALIVE false))
      (-> client-bootstrap
          (.group worker-group)
          (.channel NioSocketChannel)
          (.option ChannelOption true)
          (.handler (proxy [ChannelInitializer]
                        []
                      (init-proxy [ch]
                        (-> ch
                            .pipeline
                            (.addLast (into-array ChannelHandler [()]))))))))
    (bind [this ip port]
      (-> server-bootstrap
          (.bind (InetSocketAddress. ^String ip ^int port))
          .sync))
    (connect [this ip port]
      (-> bootstrap
          )))


  (defn -main [& args]
    (let [handler (new-handler)
          boss-group (NioEventLoopGroup.)
          worker-group (NioEventLoopGroup.)
          bootstrap (Bootstrap.)]
      (try (-> bootstrap
               (.group boss-group #_worker-group)
               (.channel NioServerSocketChannel)
               (.childHandler (proxy [ChannelInitializer]
                                  []
                                (initChannel [channel]
                                  (-> channel
                                      .pipeline
                                      (.addLast (into-array ChannelHandler [handler]))))))
               (.childOption ChannelOption/TCP_NODELAY true)
               (.childOption ChannelOption/SO_KEEPALIVE false))
           (let [future (-> bootstrap
                            (.bind (InetSocketAddress. 8080))
                            (.sync))
                 future2 (-> bootstrap
                             (.bind (InetSocketAddress. 8081))
                             (.sync))]
             (-> future
                 .channel
                 .closeFuture
                 .sync))
           #_(finally
               (.shutdown bootstrap))))))















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
  (enqueue-op [this channel flags f]))

(defrecord SelectorControl [^Selector selector ops]
  ISelector
  (start-control-thread [this]
    (thread
     (try
       (loop []
         (doseq [[^SelectableChannel c ^int flags ^SelectionKey f] (reset-old! ops [])]
           (println "add op" flags c)
           (.configureBlocking c false)
           (.register c selector flags f))
         (println "loop")
         (when-not (= 0 (.select selector))
           (let [iterator (.iterator (.selectedKeys selector))]
             (println (.hasNext iterator))
             (while (.hasNext iterator)
               #_(println "-----")
               (let [key (.next iterator)]
                 #_(println (.isConnectable key) (.isAcceptable key))
                 ((.attachment key) key selector)
                 (when (.isConnectable key)
                   (.cancel key))
                 (.remove iterator)))))
         (recur))
       (catch Throwable ex
         (println ex)
         (st/print-stack-trace ex))))
    this)
  (enqueue-op [this channel flags f]
    (swap! ops conj [channel flags f])
    (.wakeup selector)))

(def ^:dynamic *selector* (start-control-thread (->SelectorControl (Selector/open) (atom []))))


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


(defn socket->chan [^SocketChannel channel c buffer-size]
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
               obj (fressian/defressian data)]
           (>! c obj))
         (recur))))
    (enqueue-op *selector* channel SelectionKey/OP_READ
                (fn [_ _]
                  (put! flag-chan :ping)
                  #_(let [buffer (ByteBuffer/allocate buffer-size)]
                      (println "read")
                      (.read channel buffer)
                      (put! c buffer))))))

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
                 (let [binary (fressian/byte-buf v)
                       out-buff (ByteBuffer/allocate (+ (.limit binary) 4))]
                   (.putInt out-buff (.limit binary))
                   (.put out-buff binary)
                   (.flip out-buff)
                   (while (.hasRemaining ^ByteBuffer out-buff)
                     (println "writing")
                     (<! flag-chan)
                     (.write ^SocketChannel channel out-buff)
                     (>! enable-chan :ping)))
                 (recur)))
             (catch Throwable ex
               (println ex))))))
