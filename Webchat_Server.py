import os
import socket
import threading
import time

print('SERVER STARTED')
HOST = '0.0.0.0'  # listen on all interfaces inside the container
PORT = int(os.environ.get('SOCKET_PORT', 1310))  # internal port your app listens on
RETRY_INTERVAL = float(os.environ.get('MESSAGE_RETRY_INTERVAL', 1.0))
MESSAGE_SEPARATOR = '<!-WEBCHAT-!>'
CLIENT_ID_PREFIX = '<!-WEBCHATID-!>'

print(f'listening on {HOST}:{PORT}')
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((HOST, PORT))
server.listen(10)  # Number of connections accepted at a time

clients = {}
clients_lock = threading.Lock()

# Each item has the requested format: [sent, message].
# The message is kept as a dictionary so the sender, recipient, and text are
# all available when the retry worker attempts delivery.
Unsend_Message = []
unsent_message_lock = threading.Lock()


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
        target = clients.get(send_id)

    if target is None:
        # Keep the item in Unsend_Message and make it eligible for retry.
        with unsent_message_lock:
            pending_message[0] = False
        return False

    payload = (
        message_details['sender_id']
        + MESSAGE_SEPARATOR
        + message_details['message']
    )

    try:
        target.sendall(payload.encode('utf-8'))
    except (ConnectionError, OSError) as error:
        print(f'CLIENT DISCONNECTED while sending to {send_id}: {error}')
        with clients_lock:
            # Only remove the socket if it is still the socket that failed.
            if clients.get(send_id) is target:
                del clients[send_id]
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
    """Continuously retry queued messages without blocking client handlers."""
    while True:
        with unsent_message_lock:
            pending_messages = list(Unsend_Message)

        for pending_message in pending_messages:
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
            data = client.recv(4096)
            if not data:
                break  # client closed the connection
            data = data.decode('utf-8')
            print(data)

            if data.startswith(CLIENT_ID_PREFIX):
                my_id = data.removeprefix(CLIENT_ID_PREFIX)
                with clients_lock:
                    clients[my_id] = client
                print(f'registered id: {my_id}')
            else:
                data_list = data.split(MESSAGE_SEPARATOR)
                if len(data_list) < 3:
                    print('malformed message, ignoring:', data_list)
                    continue

                my_id = data_list[0]
                with clients_lock:
                    clients[my_id] = client
                print(data_list)
                sevto_msg(data_list[0], data_list[1], data_list[2])
    except Exception as error:
        print('connection error:', error)
    finally:
        with clients_lock:
            if my_id is not None and clients.get(my_id) is client:
                del clients[my_id]
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
