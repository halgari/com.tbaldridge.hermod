# Hermod

This is a library designed to answer the question "how do I easily send data from a go on one JVM to a go on another"

The analogy embraced in this library is the concept of mailboxes. In the physical world, mailboxes are places where messages
can be sent, but only members of a single household (a JVM) may take messages from the mailbox. In addition, if someone wants
to express to their friend where they can be reached, they send them an address were responses can be delivered to. A person
doesn't send their mailbox, they send a pointer to their mailbox.

In addition, sending a letter does not guarantee the message will arrive, the only way to be sure a message arrived and has been
read by the addressee is to receive a response from the reader.

This library's semantics can thus be defined quite simply:

1. Mailboxes can be created
2. Mailboxes that exist on the local VM can be written to and read from
3. Mailbox pointers (aka, remote mailboxes) can only be sent to
4. Sending messages to mailboxes is unreliable. Messages will be delivered complete, without data corruption,
   only best-effort semantics are guaranteed
5. Sending a message to a mailbox is always non-blocking. If network buffers are too full, the message may be dropped.

## Current Version

    [com.tbaldridge.hermod "0.1.3"]

## Technology

This library is built on top of Fressian and Java NIO.

A single thread uses a NIO Selector to watch all the TCP connections managed by the system. As sockets become
available for writing/reading worker threads are spawned and the processing is handled by core.async gos.

## Usage

This library consists of a few primitives that can be combined to create more complex systems:

(mailbox name buffer) - Creates a local mailbox and registers it with the mailbox registry. Both name and buffer
are optional. Name defaults to a auto generated UUID, and buffer defaults to (dropping-buffer 1024).

(remote-mailbox host port name) - Creates a pointer to a remote mailbox. If a mailbox is included in a message,
the serialization functions will automatically convert it to a remote-mailbox. An example of this can be found in
ping-test.clj in the tests folder.

(listen port) - opens a local port that other machines may send messages to. Since TCP connections are bi-directional,
this function need not be called on all servers. In a client/server situation only the server needs to listen, the
sockets used by clients to contact the server will then be used for responses to those clients.

## Example

    (ns clojure.core.async.ping-test
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

    (deftest ping-test
      ;; Only used during testing to make sure we have a clean state, you shouldn't
      ;; need to do this in normal use cases.
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
            ;; situation, like this, we will alt on a timeout and throw an exception
            ;; if the message is dropped. We could also resend and wait again.
            (>!! rbx {:return-to lbx
                      :msg x})
            (alt!! [lbx] ([v] (is (= x v)))
                   [(timeout 1000)] (assert false "Timeout after 1000 ms"))))))

## Contributing

If you have improvements to the library feel would be beneficial to the community at large, please submit them via a pull
request. One of the project maintainers will take a look at the patch and comment/merge as soon as possible.


## Meaning of "Hermod"

In Norse mythology Hermod is the son of Odin and Frigg and the messenger of the gods

## License

Copyright © 2013 Timothy Baldridge

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
