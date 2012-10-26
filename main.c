/* Kernel Memcached
 * Anthony Chivetta <anthony@chivetta.org>
 *
 * Please see LICENSE for copyright information.  This file is origional to the
 * kmemcached project.  Some inspiration for this file was taken from
 * http://kernelnewbies.org/Simple_UDP_Server and linux's net/ceph/messenger.c.
 *
 * This file is the main routene for kmemcached.  The initialization code
 * creates a listening socket, initializes the protocol parser and storage
 * enginge, and spins off a kthread which is pulling work from a kthread_worker
 * workqueue.  This workqueue design is necessairy as socket callbacks are
 * called in interrupt context and so should be quick and may not sleep.  The
 * listening socket's data_ready callback is set to callback_listen() which will
 * queue up listen_work() to be executed whenever a new connection is received.
 * listen_work() will accept the connection, create and initialize the
 * per-client data structures and set the callbacks on the socket.
 * callback_{write_space,data_ready,state_change}() handle events on the client
 * sockets adding them to the worqueue as necessairy.
 *
 * A LOT of work still needs to be done here.  Please see the TODOs littered
 * throughout the file for an idea.
 */

#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/errno.h>
#include <linux/types.h>
#include <linux/netdevice.h>
#include <linux/ip.h>
#include <linux/in.h>
#include <linux/delay.h>
#include <net/sock.h>
#include <net/tcp_states.h>

#include "libmp/protocol_handler.h"
#include "libmp/common.h"
#include "storage.h"

#include <linux/kthread.h>
#include <linux/sched.h>

//// Usage of kthread.
// struct task_struct *listen_task;
// listen_task = kthread_create(listen_thread, NULL, "listen thread");
// if (IS_ERR(listen_task)) {
//     error handling.
// }
// wake_up_process(kthread_create);
// kthread_stop(listen_task);

static struct task_struct *listen_task;

/** The port we listen on.
 *
 * TODO: This should be configurable as a parameter passed to the module at load
 * time.  Currently, it must be set here at compile time.
 */
#define DEFAULT_PORT 11212

/** The module name.
 *
 * TODO: This should be moved to a global header somewhere to be consumed by
 * printk() in other files.
 */
#define MODULE_NAME "kmemcached"

/** The number of clients to allow in the accept() queue.
 * TODO: This should also be an option set by a module argument.
 */
#define SOCKET_BACKLOG 100

/* Client States */

/** Client is active.
 *
 * This flag is set when a client is initialized and unset when the client is
 * due to be free()d.  It is used to ignore surpurflious callbacks on dieing
 * clients.
 */
#define STATE_ACTIVE 1 

/** More data to be written.
 *
 * This flag indicates that there is more data in the send queue for the client.
 * This allows us to ignore write_space callbacks when no write is needed.
 */
#define STATE_WRITING 2 

/** This client should be closed.
 *
 * This flag indicates that due to either client hangup or error condition the
 * client's connection should be closed.
 */
#define STATE_CLOSE 3

/** Client data structure. */
typedef struct client_t{
    /** Pointer to socket for this client's connection. */
    struct socket *sock;

    /** Pointer to memcached protocol struct for this client. */
    struct memcached_protocol_client_st *libmp;
    
    struct task_struct *task;

    /** For clients list. */
    struct list_head list;

    /** One or more of the STATE_* macros above. */
    long unsigned int state;
} client_t;

/** List of all clients.
 *
 * Used to free the clients when module is unloaded.
 */
static LIST_HEAD(clients);

static int listen_thread(void*);
static int client_thread(void*);
static void close_listen_socket(void);
static void close_connection(client_t *client);

/** Listening Socket
 *
 * TODO: This is our listening socket.  Ideally, this shouldn't just be a single
 * socket.  In the future, we should adapt this to support multiple listening
 * sockets.
 */
struct socket *listen_socket;

static int listen_thread(void *data)
{
    int ret = 0;
    (void)data;

    printk(KERN_INFO MODULE_NAME": listen_thread started.\n");
    allow_signal(SIGTERM);

    while (!kthread_should_stop()) {
        int err = 0;
        client_t *client = NULL;
        struct socket *new_sock = NULL;

        if ((err = kernel_accept(listen_socket, &new_sock, 0)) < 0){
            if (signal_pending(current)) {
                break;
            }
            printk(KERN_INFO MODULE_NAME": kernel_accept returns %d.\n", -err);
            ret = err;
            break;
        } 

        if (!(client = kmalloc(sizeof(client_t), GFP_KERNEL))){
            printk(KERN_INFO MODULE_NAME": Unable to allocate space for new client_t.\n");
            kernel_sock_shutdown(new_sock, SHUT_RDWR);
            sock_release(new_sock);
            ret = 0;
            continue;
        }

        client->sock = new_sock;
        client->libmp = NULL;
        INIT_LIST_HEAD(&client->list);
        client->state = 0;
        set_bit(STATE_ACTIVE, &client->state);

        client->libmp = memcached_protocol_create_client(client->sock);
        if (client->libmp == NULL){
            printk(KERN_INFO MODULE_NAME": Could not allocate memory for memcached_protocol_client_st.");
            sock_release(client->sock);
            kfree(client);
            continue;
        }

        list_add(&client->list, &clients);
        client->task = kthread_create(client_thread, client, MODULE_NAME" client");
        wake_up_process(client->task);

        printk(KERN_INFO MODULE_NAME": Accepted incoming connection.\n");
        /* TODO: output the IP of the connecting host, see __svc_print_addr */
    }
    close_listen_socket();
    return ret;
}

static int client_thread(void *data)
{
    client_t *client = (client_t*)data;
    memcached_protocol_event_t events;
    int ret=0;
    allow_signal(SIGTERM);

    events = memcached_protocol_client_work(client->libmp);

    if (events & MEMCACHED_PROTOCOL_ERROR_EVENT) {
        ret = -1;
    }
    close_connection(client);
    printk(KERN_INFO MODULE_NAME": client thread for %p stopped.\n", client);
    return ret;
}

/** Open an listening socket */
static int open_listen_socket(void){
    int err;
    struct sockaddr_in listen_address;

    /* create a socket */
    if ( (err = sock_create_kern(AF_INET, SOCK_STREAM , IPPROTO_TCP, &listen_socket)) < 0)
    {
        printk(KERN_INFO MODULE_NAME": Could not create a TCP socket, error = %d\n", -err);
        return -1;
    }

    memset(&listen_address, 0, sizeof(struct sockaddr_in));
    listen_address.sin_family      = AF_INET;
    listen_address.sin_addr.s_addr      = htonl(INADDR_ANY);
    listen_address.sin_port      = htons(DEFAULT_PORT);

    if ( (err = kernel_bind(listen_socket, (struct sockaddr *)&listen_address, sizeof(struct sockaddr_in) ) ) < 0) 
    {
        printk(KERN_INFO MODULE_NAME": Could not bind or connect to socket, error = %d\n", -err);
        return -2;
    }

    if ( ( err = kernel_listen(listen_socket, SOCKET_BACKLOG)) < 0){
        printk(KERN_INFO MODULE_NAME": Could not listen on socket, error = %d\n", -err);
        return -2;
    }

    printk(KERN_INFO MODULE_NAME": Started, listening on port %d.\n", DEFAULT_PORT);
    return 0;
}

/** Close a listening socket 
 *
 * TODO We should ensure that this is all which is needed to listen on that
 * socket again in the future.  Previous attempts to use that listening port
 * again after unloading the module have resulted in errors.
 */
static void close_listen_socket(void){
    printk(KERN_INFO MODULE_NAME": closing listen socket.\n");
    kernel_sock_shutdown(listen_socket, SHUT_RDWR);
    sock_release(listen_socket);
    listen_socket = NULL;
}

/** Close a client 
 */ 
static void close_connection(client_t *client){
    printk(KERN_INFO MODULE_NAME": Closing connection.\n");

    clear_bit(STATE_ACTIVE, &client->state);
    kernel_sock_shutdown(client->sock, SHUT_RDWR);
    sock_release(client->sock);
    client->sock = NULL;
    clear_bit(STATE_CLOSE, &client->state);

    memcached_protocol_client_destroy(client->libmp);
    list_del(&client->list);
    kfree(client);  
}

/** Load the module */
int __init kmemcached_init(void)
{
    int ret;
    
    /* open listening socket */
    if ((ret = open_listen_socket()) < 0){
        if (ret == -2) close_listen_socket();
        return -ENXIO; // FIXME use better error code
    }
    listen_task = kthread_create(listen_thread, NULL, MODULE_NAME " listen");
    if (IS_ERR(listen_task)) {
        long ret = PTR_ERR(listen_task);
        close_listen_socket();
        listen_task = NULL;
        return ret;
    }

    if (initialize_storage() == false){
        printk(KERN_INFO MODULE_NAME": unable to initialize storage engine\n");
        return -ENOMEM;
        // FIXME leak in error condition
    }

    wake_up_process(listen_task);
    return 0;
}

/** Unload the module 
 *
 * TODO This is currently a little clunky.  In particular, we should be really
 * be ensureing that each client closes cleanly including flushing write
 * buffers.  Likely, this involves flushing each client off the workqueue
 * individually as we close their connections.
 */
void __exit kmemcached_exit(void)
{
    send_sig(SIGTERM, listen_task, 1);
    kthread_stop(listen_task);

    while (!list_empty(&clients)) {
        client_t *client = container_of(clients.next, client_t, list);
        send_sig(SIGTERM, client->task, 1);
        kthread_stop(client->task);
    }

    shutdown_storage();

    if (listen_socket != NULL) {
        sock_release(listen_socket);
        listen_socket = NULL;
    }

    rcu_barrier();
    printk(KERN_INFO MODULE_NAME": module unloaded\n");
}

/* init and cleanup functions */
module_init(kmemcached_init);
module_exit(kmemcached_exit);

/* module information */
MODULE_DESCRIPTION("kmemcached");
MODULE_AUTHOR("Anthony Chivetta <anthony@chivetta.org>");
MODULE_LICENSE("Dual BSD/GPL");
