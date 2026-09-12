import os
import socket
import struct
import threading
import time

print('SERVER STARTED')
HOST = '0.0.0.0'  # listen on all interfaces inside the container
PORT = int(os.environ.get('SOCKET_PORT', 1310))  # internal port your app listens on
RETRY_INTERVAL = float(os.environ.get('MESSAGE_RETRY_INTERVAL', 1.0))
MESSAGE_SEPARATOR = '<!-WEBCHAT-!>'
CLIENT_ID_PREFIX = '<!-WEBCHATID-!>'
HEARTBEAT_PAYLOAD = '<!-WEBCHAT-PING-!>'

# How long an undelivered message is allowed to sit in the retry queue
# before being given up on. Without this, a message addressed to a
# recipient who never comes back online would retry forever and the
# queue would grow without bound for the life of the process.
MAX_PENDING_AGE = float(os.environ.get('MESSAGE_MAX_PENDING_AGE', 600))

# ---------------- FRAMING ---------------- #
# A single recv()/send() call is NOT guaranteed to line up with exactly
# one application-level message on a TCP stream - the OS is free to
# split one message across multiple recv()s, or merge several sends into
# one recv(). The previous version read a fixed-size chunk and assumed
# it was exactly one message, which could silently truncate, merge, or
# corrupt chat text. Every message (including the keep-alive ping) is
# now sent as a 4-byte big-endian length prefix followed by that many
# UTF-8 bytes. The client uses the exact same scheme - both sides must
# agree on this.
_FRAME_HEADER_SIZE = 4
_MAX_FRAME_SIZE = 1_000_000  # sanity cap, far above any real chat message


def _recv_exact(sock, num_bytes):
    """Read exactly num_bytes from sock, looping over partial reads.
    Returns None if the connection closes before that many bytes arrive."""
    chunks = []
    remaining = num_bytes
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def send_frame(sock, text):
    """Send one length-prefixed UTF-8 text frame."""
    payload = text.encode('utf-8')
    sock.sendall(struct.pack('!I', len(payload)) + payload)


def recv_frame(sock):
    """Read exactly one length-prefixed UTF-8 text frame. Returns the
    decoded text, or None if the connection closed cleanly. Raises
    ValueError/UnicodeDecodeError if the stream is corrupt/desynced -
    that's unrecoverable and the caller should close the connection."""
    header = _recv_exact(sock, _FRAME_HEADER_SIZE)
    if header is None:
        return None
    (length,) = struct.unpack('!I', header)
    if length > _MAX_FRAME_SIZE:
        raise ValueError(f'Frame too large ({length} bytes)')
    body = _recv_exact(sock, length)
    if body is None:
        return None
    return body.decode('utf-8')


print(f'listening on {HOST}:{PORT}')
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((HOST, PORT))
server.listen(10)  # Number of connections accepted at a time

# id -> {'socket': socket, 'lock': threading.Lock()}. The per-client lock
# guarantees that two threads (this client's own handler thread, plus the
# background retry worker, both of which can send to the same recipient)
# never interleave partial writes on the same socket - that would corrupt
# both messages even though each is individually framed correctly.
clients = {}
clients_lock = threading.Lock()

# Each item is [sent_flag, message_details, queued_at_timestamp].
Unsend_Message = []
unsent_message_lock = threading.Lock()


def _register_client(client_id, sock):
    with clients_lock:
        clients[client_id] = {'socket': sock, 'lock': threading.Lock()}


def _unregister_client(client_id, sock):
    with clients_lock:
        entry = clients.get(client_id)
        if entry is not None and entry['socket'] is sock:
            del clients[client_id]


def _remove_pending_message(pending_message):
    """Remove a queued message by object identity, not by value."""
    with unsent_message_lock:
        for index, item in enumerate(Unsend_Message):
            if item is pending_message:
                del Unsend_Message[index]
                return


def _try_send_pending_message(pending_message):
    """Try to deliver one queued message and remove it after success."""
    # Reserve the item so the retry thread and the message-receiving thread
    # cannot send the same message concurrently.
    with unsent_message_lock:
        if not any(item is pending_message for item in Unsend_Message):
            return False
        if pending_message[0]:
            return False
        pending_message[0] = True

    message_details = pending_message[1]
    send_id = message_details['send_id']

    with clients_lock:
        entry = clients.get(send_id)

    if entry is None:
        # Keep the item in Unsend_Message and make it eligible for retry.
        with unsent_message_lock:
            pending_message[0] = False
        return False

    target = entry['socket']
    payload = (
        message_details['sender_id']
        + MESSAGE_SEPARATOR
        + message_details['message']
    )

    try:
        # The lock ensures this send can't interleave on the wire with
        # another thread sending to the same recipient at the same time.
        with entry['lock']:
            send_frame(target, payload)
    except (ConnectionError, OSError) as error:
        print(f'CLIENT DISCONNECTED while sending to {send_id}: {error}')
        _unregister_client(send_id, target)
        with unsent_message_lock:
            pending_message[0] = False
        return False

    # True indicates that delivery succeeded. Remove the item immediately
    # afterward, as requested, so the list contains only undelivered messages.
    pending_message[0] = True
    _remove_pending_message(pending_message)
    print('msg sent to', send_id)
    return True


def retry_unsent_messages():
    """Continuously retry queued messages without blocking client handlers.
    Also drops messages that have been sitting undelivered for too long,
    so a recipient who never reconnects doesn't grow this list forever."""
    while True:
        now = time.monotonic()
        with unsent_message_lock:
            pending_messages = list(Unsend_Message)

        for pending_message in pending_messages:
            queued_at = pending_message[2]
            if now - queued_at > MAX_PENDING_AGE:
                send_id = pending_message[1]['send_id']
                print(f'Giving up on message to {send_id} after {MAX_PENDING_AGE:.0f}s undelivered')
                _remove_pending_message(pending_message)
                continue
            _try_send_pending_message(pending_message)

        time.sleep(RETRY_INTERVAL)


def sevto_msg(sender_id, msg, send_id):
    """Queue an outbound message and attempt immediate delivery."""
    pending_message = [
        False,
        {
            'sender_id': sender_id,
            'message': msg,
            'send_id': send_id,
        },
        time.monotonic(),
    ]

    with unsent_message_lock:
        Unsend_Message.append(pending_message)

    # Try immediately when possible. If the recipient is offline or the send
    # fails, the background worker will keep retrying this same list item.
    _try_send_pending_message(pending_message)


def handle_client(client, addr):
    # Give each connection its own recv loop so a slow/idle client
    # never blocks the server from accepting new connections.
    my_id = None
    try:
        while True:
            try:
                data = recv_frame(client)
            except (ValueError, UnicodeDecodeError) as frame_error:
                # The stream is desynced (a corrupt/garbage length
                # prefix) - there is no way to recover a byte position
                # in the stream from here, so this connection has to end.
                print(f'Frame error from {addr}, closing connection: {frame_error}')
                break

            if data is None:
                break  # client closed the connection

            if data == HEARTBEAT_PAYLOAD:
                # Keep-alive ping - nothing to process, but receiving it
                # (and this loop iterating) is what keeps the connection
                # from looking idle to any proxy/timeout in the path.
                continue

            print(data)

            if data.startswith(CLIENT_ID_PREFIX):
                my_id = data[len(CLIENT_ID_PREFIX):]
                _register_client(my_id, client)
                print(f'registered id: {my_id}')
            else:
                data_list = data.split(MESSAGE_SEPARATOR)
                if len(data_list) < 3:
                    # Framing was fine - we know exactly where this
                    # message ends - so this is just an application-level
                    # malformed message. Skip it, keep the connection.
                    print('malformed message, ignoring:', data_list)
                    continue

                my_id = data_list[0]
                _register_client(my_id, client)
                print(data_list)
                sevto_msg(data_list[0], data_list[1], data_list[2])
    except (ConnectionError, OSError) as error:
        print('connection error:', error)
    finally:
        if my_id is not None:
            _unregister_client(my_id, client)
        client.close()
        print(f'connection closed: {addr}')


retry_thread = threading.Thread(
    target=retry_unsent_messages,
    name='unsent-message-retry-worker',
    daemon=True,
)
retry_thread.start()

while True:
    try:
        client, addr = server.accept()
        print('new connection from', addr)
        thread = threading.Thread(target=handle_client, args=(client, addr), daemon=True)
        thread.start()
    except Exception as error:
        print('accept error:', error)
