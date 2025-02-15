#ifndef _CHOWN_H_
#define _CHOWN_H_

#include "syscalls.h"

struct chown_event_t {
    struct kevent_t event;
    struct process_context_t process;
    struct span_context_t span;
    struct container_context_t container;
    struct syscall_t syscall;
    struct file_t file;
    uid_t uid;
    gid_t gid;
};

int __attribute__((always_inline)) chown_approvers(struct syscall_cache_t *syscall) {
    return basename_approver(syscall, syscall->setattr.dentry, EVENT_CHOWN);
}

int __attribute__((always_inline)) trace__sys_chown(uid_t user, gid_t group) {
    struct policy_t policy = fetch_policy(EVENT_CHOWN);
    if (is_discarded_by_process(policy.mode, EVENT_CHOWN)) {
        return 0;
    }

    struct syscall_cache_t syscall = {
        .type = EVENT_CHOWN,
        .policy = policy,
        .setattr = {
            .user = user,
            .group = group
        }
    };

    cache_syscall(&syscall);

    return 0;
}

SYSCALL_KPROBE3(lchown, const char*, filename, uid_t, user, gid_t, group) {
    return trace__sys_chown(user, group);
}

SYSCALL_KPROBE3(fchown, int, fd, uid_t, user, gid_t, group) {
    return trace__sys_chown(user, group);
}

SYSCALL_KPROBE3(chown, const char*, filename, uid_t, user, gid_t, group) {
    return trace__sys_chown(user, group);
}

SYSCALL_KPROBE3(lchown16, const char*, filename, uid_t, user, gid_t, group) {
    return trace__sys_chown(user, group);
}

SYSCALL_KPROBE3(fchown16, int, fd, uid_t, user, gid_t, group) {
    return trace__sys_chown(user, group);
}

SYSCALL_KPROBE3(chown16, const char*, filename, uid_t, user, gid_t, group) {
    return trace__sys_chown(user, group);
}

SYSCALL_KPROBE4(fchownat, int, dirfd, const char*, filename, uid_t, user, gid_t, group) {
    return trace__sys_chown(user, group);
}

int __attribute__((always_inline)) sys_chown_ret(void *ctx, int retval) {
    struct syscall_cache_t *syscall = pop_syscall(EVENT_CHOWN);
    if (!syscall) {
        return 0;
    }

    if (IS_UNHANDLED_ERROR(retval)) {
        return 0;
    }

    if (is_pipefs_mount_id(syscall->setattr.file.path_key.mount_id)) {
        return 0;
    }

    struct chown_event_t event = {
        .syscall.retval = retval,
        .event.async = 0,
        .file = syscall->setattr.file,
        .uid = syscall->setattr.user,
        .gid = syscall->setattr.group,
    };

    struct proc_cache_t *entry = fill_process_context(&event.process);
    fill_container_context(entry, &event.container);
    fill_span_context(&event.span);

    // dentry resolution in setattr.h

    send_event(ctx, EVENT_CHOWN, event);

    return 0;
}

int __attribute__((always_inline)) kprobe_sys_chown_ret(struct pt_regs *ctx) {
    int retval = PT_REGS_RC(ctx);
    return sys_chown_ret(ctx, retval);
}

SEC("tracepoint/syscalls/sys_exit_lchown")
int tracepoint_syscalls_sys_exit_lchown(struct tracepoint_syscalls_sys_exit_t *args) {
    return sys_chown_ret(args, args->ret);
}

SYSCALL_KRETPROBE(lchown) {
    return kprobe_sys_chown_ret(ctx);
}

SEC("tracepoint/syscalls/sys_exit_fchown")
int tracepoint_syscalls_sys_exit_fchown(struct tracepoint_syscalls_sys_exit_t *args) {
    return sys_chown_ret(args, args->ret);
}

SYSCALL_KRETPROBE(fchown) {
    return kprobe_sys_chown_ret(ctx);
}

SEC("tracepoint/syscalls/sys_exit_chown")
int tracepoint_syscalls_sys_exit_chown(struct tracepoint_syscalls_sys_exit_t *args) {
    return sys_chown_ret(args, args->ret);
}

SYSCALL_KRETPROBE(chown) {
    return kprobe_sys_chown_ret(ctx);
}

SEC("tracepoint/syscalls/sys_exit_lchown16")
int tracepoint_syscalls_sys_exit_lchown16(struct tracepoint_syscalls_sys_exit_t *args) {
    return sys_chown_ret(args, args->ret);
}

SYSCALL_KRETPROBE(lchown16) {
    return kprobe_sys_chown_ret(ctx);
}

SEC("tracepoint/syscalls/sys_exit_fchown16")
int tracepoint_syscalls_sys_exit_fchown16(struct tracepoint_syscalls_sys_exit_t *args) {
    return sys_chown_ret(args, args->ret);
}

SYSCALL_KRETPROBE(fchown16) {
    return kprobe_sys_chown_ret(ctx);
}

SEC("tracepoint/syscalls/sys_exit_chown16")
int tracepoint_syscalls_sys_exit_chown16(struct tracepoint_syscalls_sys_exit_t *args) {
    return sys_chown_ret(args, args->ret);
}

SYSCALL_KRETPROBE(chown16) {
    return kprobe_sys_chown_ret(ctx);
}

SEC("tracepoint/syscalls/sys_exit_fchownat")
int tracepoint_syscalls_sys_exit_fchownat(struct tracepoint_syscalls_sys_exit_t *args) {
    return sys_chown_ret(args, args->ret);
}

SYSCALL_KRETPROBE(fchownat) {
    return kprobe_sys_chown_ret(ctx);
}

SEC("tracepoint/handle_sys_chown_exit")
int tracepoint_handle_sys_chown_exit(struct tracepoint_raw_syscalls_sys_exit_t *args) {
    return sys_chown_ret(args, args->ret);
}

#endif
