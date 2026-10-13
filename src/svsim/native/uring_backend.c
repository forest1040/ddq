/* Optional liburing shim. No SQPOLL, registered buffers, or background thread. */
#include <liburing.h>
#include <stdint.h>
#include <stdlib.h>
#include <errno.h>
struct sv_ring { struct io_uring ring; };
void *sv_uring_open(unsigned depth, int *error) {
    struct sv_ring *p = calloc(1, sizeof(*p));
    if (!p) { *error = -ENOMEM; return NULL; }
    *error = io_uring_queue_init(depth, &p->ring, 0);
    if (*error < 0) { free(p); return NULL; }
    return p;
}
int sv_uring_queue(void *ctx, int operation, int fd, void *buffer,
                   unsigned length, uint64_t offset, uint64_t token, uint64_t target) {
    struct sv_ring *p = ctx;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&p->ring);
    if (!sqe) return -EAGAIN;
    switch (operation) {
    case 0: io_uring_prep_read(sqe, fd, buffer, length, offset); break;
    case 1: io_uring_prep_write(sqe, fd, buffer, length, offset); break;
    case 2: io_uring_prep_fsync(sqe, fd, 0); break;
    case 3: io_uring_prep_cancel64(sqe, target, 0); break;
    default: /* Python validates operation before entry. */ return -EINVAL;
    }
    io_uring_sqe_set_data64(sqe, token);
    /* Queued is distinct from submitted. Python already owns the request. */
    return 0;
}
int sv_uring_kick(void *ctx) {
    int result;
    do { result = io_uring_submit(&((struct sv_ring *)ctx)->ring); }
    while (result == -EINTR);
    return result;
}
int sv_uring_poll(void *ctx, uint64_t *token, int *result) {
    struct sv_ring *p = ctx;
    struct io_uring_cqe *cqe;
    int status = io_uring_peek_cqe(&p->ring, &cqe);
    if (status == -EAGAIN) return 0;
    if (status < 0) return status;
    *token = io_uring_cqe_get_data64(cqe);
    *result = cqe->res;
    io_uring_cqe_seen(&p->ring, cqe);
    return 1;
}
void sv_uring_close(void *ctx) {
    struct sv_ring *p = ctx;
    io_uring_queue_exit(&p->ring);
    free(p);
}
