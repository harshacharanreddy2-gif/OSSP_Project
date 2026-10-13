/* =====================================================================
 * mas.c  --  Multi-Agent AI Execution and Resource Management System
 *
 * SINGLE-FILE BUILD of the original three-program project
 * (orchestrator.c + agent.c + status_client.c + common.h), merged so
 * the whole system compiles into ONE executable with a plain:
 *
 *     gcc -pthread -std=gnu11 -o mas mas.c -lpthread -lrt
 *
 * WHY ONE BINARY CAN STILL "fork()+exec() a separate agent process"
 * -------------------------------------------------------------------
 * The original design's whole point (CO2) is that each AI agent is an
 * independently scheduled OS process, created by fork() and turned
 * into agent code via exec(). We keep that real fork()/exec() behavior
 * even though there is only one binary on disk: the orchestrator's
 * scheduler thread forks a child, and the child execs
 * "/proc/self/exe" (a symlink the Linux kernel maintains that always
 * points at the currently running binary) with a hidden
 * "--agent <fd>" argument. That new process image runs main() again,
 * sees the --agent flag, and jumps straight into the agent logic --
 * so you still get a genuine, separate, exec()'d process per task,
 * just packaged from a single source file / single binary.
 *
 * USAGE
 * -------------------------------------------------------------------
 *   ./mas                 run the orchestrator shell (this is what
 *                         you normally run; type 'help' inside it)
 *   ./mas --status        run the status-client: connects to a
 *                         running orchestrator's socket and prints
 *                         one snapshot, then exits (run this in a
 *                         second terminal while ./mas is running)
 *   ./mas --agent <fd>    INTERNAL. This is how the orchestrator
 *                         re-execs itself to become an agent process;
 *                         you should not need to type this yourself.
 * ===================================================================== */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
#include <stdarg.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/select.h>
#include <sys/time.h>

/* =====================================================================
 * ---- from common.h -----------------------------------------------
 * Shared definitions used by both the orchestrator side and the agent
 * side of the system. Kept verbatim (just no longer in its own file).
 * ===================================================================== */

/* CO4: Virtual-memory / paging simulation parameters. TOTAL_FRAMES
 * models a small physical memory made of fixed-size frames. Every
 * task declares how many "pages" (frames) of memory it needs; the
 * Resource Manager grants frames from this pool, and blocks the agent
 * (a simulated page fault) when the pool is exhausted. */
#define TOTAL_FRAMES        16
#define MAX_PAGES_PER_TASK   8

/* Maximum number of agent bookkeeping slots kept in shared memory. */
#define MAX_AGENTS           32

/* CO6: counting semaphore capacity -> number of agents allowed to run
 * concurrently, modelling a machine with a fixed number of CPU cores. */
#define CPU_SLOTS             3

/* Names of the named / persistent IPC objects (CO3). */
#define SHM_NAME       "/mas_shm"                /* POSIX shared memory   */
#define FIFO_PATH      "/tmp/mas_results_fifo"   /* named pipe (FIFO)     */
#define SOCK_PATH      "/tmp/mas_status.sock"    /* UNIX domain socket    */
#define LOG_FILE       "mas_agent.log"           /* CO5: append-only log  */
#define RESULTS_FILE   "mas_results.dat"         /* CO5: random-access DB */

/* TaskMsg - sent from the orchestrator's scheduler thread to a freshly
 * forked agent process over an ANONYMOUS PIPE (CO3), just before the
 * child exec()'s into agent code (CO2). */
typedef struct {
    int  task_id;
    char name[32];
    int  duration_sec;   /* how long the agent "executes" for            */
    int  pages_needed;   /* how many memory frames (CO4) it must acquire */
} TaskMsg;

/* AgentSlot - one row of live bookkeeping information about a task/agent,
 * stored inside the shared resource table so every process can see it.
 * status: 0 = empty/free slot, 1 = running, 2 = completed */
typedef struct {
    pid_t pid;
    int   task_id;
    char  name[32];
    int   pages_used;
    int   frames[MAX_PAGES_PER_TASK];  /* which physical frames it owns */
    int   status;
} AgentSlot;

/* ResourceTable - the single block of POSIX shared memory (CO3) used by
 * the orchestrator process and every agent process to see and update
 * the same view of system resources. mutex / frames_available are
 * placed *inside* the shared memory region itself and initialised with
 * PTHREAD_PROCESS_SHARED so that a pthread_mutex_t / pthread_cond_t can
 * correctly synchronise threads that live in DIFFERENT processes. */
typedef struct {
    pthread_mutex_t mutex;              /* protects every field below   */
    pthread_cond_t  frames_available;   /* signalled when frames freed  */

    int frame_owner[TOTAL_FRAMES];      /* -1 = free, else agent index  */
    int free_frames;

    AgentSlot agents[MAX_AGENTS];

    int total_submitted;
    int total_completed;
} ResourceTable;

/* Sent back to the status-client mode over the UNIX domain socket (CO3). */
typedef struct {
    char status[16];
    int  free_frames;
    int  total_frames;
    int  total_submitted;
    int  total_completed;
    int  active_agents;
} StatusSnapshot;

/* ResultRecord - fixed-size structured record written by an agent into
 * RESULTS_FILE at a byte offset computed from task_id, using lseek()
 * for direct/random-access file I/O (CO5). */
typedef struct {
    int  task_id;
    char name[32];
    int  duration_sec;
    int  pages_used;
    long completion_time;   /* time_t stored as long for a stable size */
    int  status;             /* 1 = completed successfully              */
} ResultRecord;

/* =====================================================================
 * ---- Orchestrator-side global state --------------------------------
 * ===================================================================== */
static ResourceTable *g_shm = NULL;     /* CO3/CO4: shared resource table */
static sem_t g_cpu_slots;               /* CO6: counting semaphore        */
static volatile sig_atomic_t g_running = 1;
static volatile int g_outstanding = 0;  /* agents dispatched but not yet reaped */
static pthread_mutex_t g_outstanding_mx = PTHREAD_MUTEX_INITIALIZER;

/* --- Simple in-process task queue (producer: main thread / shell,
 *     consumer: scheduler thread).  Guarded by CO6 mutex + condvar. --- */
#define QUEUE_CAP 64
static TaskMsg   g_queue[QUEUE_CAP];
static int       g_qhead = 0, g_qtail = 0, g_qcount = 0;
static pthread_mutex_t g_qmutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_qcond  = PTHREAD_COND_INITIALIZER;
static int g_next_task_id = 1;

/* =====================================================================
 * Shared-memory / Resource-Manager setup                     (CO3+CO4)
 * ===================================================================== */
static ResourceTable *resource_manager_init(void)
{
    shm_unlink(SHM_NAME); /* start clean in case of a previous crash */

    int fd = shm_open(SHM_NAME, O_CREAT | O_RDWR | O_EXCL, 0666);
    if (fd < 0) { perror("shm_open"); exit(1); }

    if (ftruncate(fd, sizeof(ResourceTable)) < 0) { perror("ftruncate"); exit(1); }

    ResourceTable *rt = mmap(NULL, sizeof(ResourceTable),
                              PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (rt == MAP_FAILED) { perror("mmap"); exit(1); }

    /* Cross-process mutex + condition variable live INSIDE the shared
     * memory region, so both the orchestrator and every agent process
     * synchronise on the very same lock (CO6 primitives, CO3 scope). */
    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&rt->mutex, &mattr);

    pthread_condattr_t cattr;
    pthread_condattr_init(&cattr);
    pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_SHARED);
    pthread_cond_init(&rt->frames_available, &cattr);

    for (int i = 0; i < TOTAL_FRAMES; i++) rt->frame_owner[i] = -1;
    rt->free_frames     = TOTAL_FRAMES;
    rt->total_submitted = 0;
    rt->total_completed = 0;
    memset(rt->agents, 0, sizeof(rt->agents));

    return rt;
}

/* =====================================================================
 * Scheduler thread                                    (CO2 + CO3 + CO6)
 *   - blocks on the task-queue condition variable until work arrives
 *   - blocks on the CPU-slot counting semaphore until a core is free
 *   - fork()s a child, connects it via an anonymous pipe, then the
 *     child exec()s THIS SAME BINARY again with "--agent <fd>" so it
 *     becomes a genuine, separate agent process (see file header).
 * ===================================================================== */
static void *scheduler_thread(void *arg)
{
    (void)arg;
    while (1) {
        pthread_mutex_lock(&g_qmutex);
        while (g_qcount == 0 && g_running) pthread_cond_wait(&g_qcond, &g_qmutex);
        if (g_qcount == 0 && !g_running) { pthread_mutex_unlock(&g_qmutex); break; }

        TaskMsg task = g_queue[g_qhead];
        g_qhead = (g_qhead + 1) % QUEUE_CAP;
        g_qcount--;
        pthread_mutex_unlock(&g_qmutex);

        /* CO6: wait for a free "CPU slot" before launching the agent. */
        sem_wait(&g_cpu_slots);

        int pipefd[2];
        if (pipe(pipefd) < 0) { perror("pipe"); sem_post(&g_cpu_slots); continue; }

        pid_t pid = fork();                              /* CO2 */
        if (pid < 0) {
            perror("fork");
            close(pipefd[0]); close(pipefd[1]);
            sem_post(&g_cpu_slots);
            continue;
        }

        if (pid == 0) {
            /* ---- child: becomes the agent process ---- */
            close(pipefd[1]);
            if (pipefd[0] != 3) {
                dup2(pipefd[0], 3);  /* task descriptor arrives on fd 3 */
                close(pipefd[0]);
            }
            /* CO2 exec: re-exec THIS binary via /proc/self/exe so the
             * child becomes a fresh, independent process image running
             * the agent code path (main() dispatches on "--agent"). */
            execl("/proc/self/exe", "mas-agent", "--agent", "3", (char *)NULL);
            perror("execl self as agent");
            _exit(127);
        }

        /* ---- parent: hand the task descriptor to the child ---- */
        close(pipefd[0]);
        if (write(pipefd[1], &task, sizeof(task)) != (ssize_t)sizeof(task))
            perror("write task to pipe");
        close(pipefd[1]);

        pthread_mutex_lock(&g_shm->mutex);
        g_shm->total_submitted++;
        pthread_mutex_unlock(&g_shm->mutex);

        pthread_mutex_lock(&g_outstanding_mx);
        g_outstanding++;
        pthread_mutex_unlock(&g_outstanding_mx);

        printf("[scheduler] dispatched task #%d \"%s\" -> agent pid=%d "
               "(duration=%ds, pages=%d)\n",
               task.task_id, task.name, pid, task.duration_sec, task.pages_needed);
    }
    return NULL;
}

/* =====================================================================
 * Signal-handling thread                                      (CO3+CO2)
 *   Waits synchronously for SIGCHLD (a child finished -> reap it and
 *   free its CPU slot) and SIGUSR1 (an agent's own "I'm done" ping).
 *   All other threads have these signals blocked, so only this thread
 *   ever receives them (see orchestrator_main(), pthread_sigmask before
 *   thread creation).
 * ===================================================================== */
static void *signal_thread(void *arg)
{
    (void)arg;
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGCHLD);
    sigaddset(&set, SIGUSR1);

    while (1) {
        int sig;
        int rc = sigwait(&set, &sig);
        if (rc != 0) continue;

        if (sig == SIGCHLD) {
            int status;
            pid_t p;
            while ((p = waitpid(-1, &status, WNOHANG)) > 0) {   /* CO2 */
                sem_post(&g_cpu_slots);                          /* CO6 */
                pthread_mutex_lock(&g_outstanding_mx);
                if (g_outstanding > 0) g_outstanding--;
                pthread_mutex_unlock(&g_outstanding_mx);
                printf("[reaper] agent pid=%d exited (status=%d), "
                       "CPU slot freed\n", p, WEXITSTATUS(status));
            }
        } else if (sig == SIGUSR1) {
            printf("[signal] received completion notice (SIGUSR1) from an agent\n");
        }
    }
    return NULL;
}

/* =====================================================================
 * FIFO reader thread                                           (CO3)
 *   Reads completion reports that agents write into the named pipe.
 * ===================================================================== */
static void *fifo_reader_thread(void *arg)
{
    (void)arg;
    mkfifo(FIFO_PATH, 0666);   /* ignore EEXIST */

    char buf[256];
    while (1) {
        int fd = open(FIFO_PATH, O_RDONLY);   /* blocks until a writer opens */
        if (fd < 0) { if (errno == EINTR) continue; continue; }

        ssize_t n;
        while ((n = read(fd, buf, sizeof(buf) - 1)) > 0) {
            buf[n] = '\0';
            printf("[fifo] result report: %s", buf);
            /* NOTE: total_completed is incremented by the agent itself
             * inside release_frames() at the moment it frees its memory
             * frames; this FIFO message is only a confirmation report,
             * so we do not double-count completions here. */
        }
        close(fd);
    }
    return NULL;
}

/* =====================================================================
 * Monitor thread                                               (CO6+CO4)
 *   Periodically prints a live dashboard read straight out of the
 *   shared Resource Table.
 * ===================================================================== */
static void *monitor_thread(void *arg)
{
    (void)arg;
    while (1) {
        sleep(4);

        pthread_mutex_lock(&g_shm->mutex);
        int free_f = g_shm->free_frames;
        int sub = g_shm->total_submitted, comp = g_shm->total_completed;
        printf("\n----------------- Resource Dashboard -----------------\n");
        printf(" Memory frames : %d / %d free\n", free_f, TOTAL_FRAMES);
        printf(" Tasks         : submitted=%d  completed=%d  in-flight=%d\n",
               sub, comp, sub - comp);
        printf(" Agents:\n");
        for (int i = 0; i < MAX_AGENTS; i++) {
            AgentSlot *a = &g_shm->agents[i];
            if (a->status == 0) continue;
            printf("   #%-3d %-12s pid=%-6d pages=%-2d status=%s\n",
                   a->task_id, a->name, a->pid, a->pages_used,
                   a->status == 1 ? "RUNNING" : "DONE");
        }
        printf("--------------------------------------------------------\n\n");
        pthread_mutex_unlock(&g_shm->mutex);
    }
    return NULL;
}

/* =====================================================================
 * UNIX domain socket server thread                              (CO3)
 *   Lets an external "status" invocation (./mas --status) query live
 *   stats without going through the shell.
 * ===================================================================== */
static void *socket_server_thread(void *arg)
{
    (void)arg;
    unlink(SOCK_PATH);

    int sfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); return NULL; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path) - 1);

    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(sfd); return NULL;
    }
    if (listen(sfd, 8) < 0) { perror("listen"); close(sfd); return NULL; }

    while (1) {
        struct timeval tv = {1, 0};
        fd_set rfds; FD_ZERO(&rfds); FD_SET(sfd, &rfds);
        int r = select(sfd + 1, &rfds, NULL, NULL, &tv);
        if (r <= 0) continue;

        int cfd = accept(sfd, NULL, NULL);
        if (cfd < 0) continue;

        StatusSnapshot snap;
        memset(&snap, 0, sizeof(snap));
        strncpy(snap.status, "OK", sizeof(snap.status) - 1);

        pthread_mutex_lock(&g_shm->mutex);
        snap.free_frames     = g_shm->free_frames;
        snap.total_frames    = TOTAL_FRAMES;
        snap.total_submitted = g_shm->total_submitted;
        snap.total_completed = g_shm->total_completed;
        int active = 0;
        for (int i = 0; i < MAX_AGENTS; i++)
            if (g_shm->agents[i].status == 1) active++;
        snap.active_agents = active;
        pthread_mutex_unlock(&g_shm->mutex);

        write(cfd, &snap, sizeof(snap));
        close(cfd);
    }
    close(sfd);
    unlink(SOCK_PATH);
    return NULL;
}

/* =====================================================================
 * Shell helpers                                                 (CO1)
 * ===================================================================== */
static void submit_task(const char *name, int duration, int pages)
{
    if (pages < 1) pages = 2;
    if (pages > MAX_PAGES_PER_TASK) pages = MAX_PAGES_PER_TASK;

    TaskMsg t;
    memset(&t, 0, sizeof(t));
    t.task_id = g_next_task_id++;
    strncpy(t.name, name, sizeof(t.name) - 1);
    t.duration_sec  = duration;
    t.pages_needed  = pages;

    pthread_mutex_lock(&g_qmutex);
    if (g_qcount == QUEUE_CAP) {
        printf("Queue is full, try again shortly.\n");
    } else {
        g_queue[g_qtail] = t;
        g_qtail = (g_qtail + 1) % QUEUE_CAP;
        g_qcount++;
        printf("Queued task #%d \"%s\" (duration=%ds, pages=%d)\n",
               t.task_id, t.name, duration, pages);
    }
    pthread_cond_signal(&g_qcond);
    pthread_mutex_unlock(&g_qmutex);
}

static void print_help(void)
{
    printf(
        "Multi-Agent AI Execution and Resource Management System - Shell\n"
        "Commands:\n"
        "  submit <n> <duration_sec> [pages]   queue a new AI agent task\n"
        "  status                                  show resource dashboard now\n"
        "  list                                    list all known agents\n"
        "  help                                    show this help\n"
        "  exit                                    shut the system down\n");
}

static void cmd_status(void)
{
    pthread_mutex_lock(&g_shm->mutex);
    printf("Frames free : %d / %d\n", g_shm->free_frames, TOTAL_FRAMES);
    printf("Submitted   : %d\n", g_shm->total_submitted);
    printf("Completed   : %d\n", g_shm->total_completed);
    pthread_mutex_unlock(&g_shm->mutex);
}

static void cmd_list(void)
{
    pthread_mutex_lock(&g_shm->mutex);
    printf("%-4s %-12s %-8s %-6s %-8s\n", "ID", "NAME", "PID", "PAGES", "STATUS");
    for (int i = 0; i < MAX_AGENTS; i++) {
        AgentSlot *a = &g_shm->agents[i];
        if (a->status == 0) continue;
        printf("%-4d %-12s %-8d %-6d %-8s\n", a->task_id, a->name, a->pid,
               a->pages_used, a->status == 1 ? "RUNNING" : "DONE");
    }
    pthread_mutex_unlock(&g_shm->mutex);
}

/* =====================================================================
 * orchestrator_main()  (was plain main() in orchestrator.c)     (CO1)
 * ===================================================================== */
static int orchestrator_main(void)
{
    /* Make sure re-exec'ing ourselves as an agent will actually work. */
    if (access("/proc/self/exe", X_OK) != 0) {
        fprintf(stderr, "error: /proc/self/exe not usable on this system "
                         "(needs Linux /proc); cannot launch agents.\n");
        return 1;
    }

    g_shm = resource_manager_init();
    sem_init(&g_cpu_slots, 0, CPU_SLOTS);      /* CO6 counting semaphore */

    /* Block SIGCHLD/SIGUSR1 in this (main) thread BEFORE spawning any
     * other threads, so the mask is inherited everywhere and ONLY
     * signal_thread's sigwait() ever "receives" them (CO3). */
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGCHLD);
    sigaddset(&set, SIGUSR1);
    pthread_sigmask(SIG_BLOCK, &set, NULL);

    pthread_t t_sched, t_sig, t_fifo, t_mon, t_sock;
    pthread_create(&t_sched, NULL, scheduler_thread, NULL);
    pthread_create(&t_sig,   NULL, signal_thread,    NULL);
    pthread_create(&t_fifo,  NULL, fifo_reader_thread, NULL);
    pthread_create(&t_mon,   NULL, monitor_thread,   NULL);
    pthread_create(&t_sock,  NULL, socket_server_thread, NULL);

    printf("Multi-Agent AI Execution and Resource Management System\n");
    printf("Type 'help' for commands.\n\n");

    char line[256];
    while (g_running && fgets(line, sizeof(line), stdin)) {
        char cmd[32] = {0}, a1[64] = {0};
        int a2 = 0, a3 = 0;
        line[strcspn(line, "\n")] = '\0';
        if (line[0] == '\0') continue;

        int n = sscanf(line, "%31s %63s %d %d", cmd, a1, &a2, &a3);

        if (strcmp(cmd, "submit") == 0 && n >= 3) {
            submit_task(a1, a2, n >= 4 ? a3 : 2);
        } else if (strcmp(cmd, "status") == 0) {
            cmd_status();
        } else if (strcmp(cmd, "list") == 0) {
            cmd_list();
        } else if (strcmp(cmd, "help") == 0) {
            print_help();
        } else if (strcmp(cmd, "exit") == 0 || strcmp(cmd, "quit") == 0) {
            g_running = 0;
        } else {
            printf("Unknown command. Type 'help'.\n");
        }
    }

    /* -------- shutdown -------- */
    g_running = 0;
    pthread_mutex_lock(&g_qmutex);
    pthread_cond_broadcast(&g_qcond);
    pthread_mutex_unlock(&g_qmutex);

    pthread_join(t_sched, NULL);

    /* Give already-dispatched agents a chance to finish their pipeline
     * (memory release -> log -> result record -> FIFO report -> signal)
     * before we tear down the threads that service the FIFO/socket, so
     * a still-running agent is never left blocked on open()/write()
     * with no reader on the other end. Bounded so shutdown can never
     * hang forever if something goes wrong. */
    printf("Waiting for %d in-flight agent(s) to finish...\n", g_outstanding);
    for (int waited_ms = 0; g_outstanding > 0 && waited_ms < 30000; waited_ms += 100)
        usleep(100000);
    if (g_outstanding > 0)
        printf("warning: %d agent(s) still outstanding, shutting down anyway\n",
               g_outstanding);

    pthread_cancel(t_sig);     /* blocked in sigwait/open/read/select loops */
    pthread_cancel(t_fifo);
    pthread_cancel(t_mon);
    pthread_cancel(t_sock);

    munmap(g_shm, sizeof(ResourceTable));
    shm_unlink(SHM_NAME);
    sem_destroy(&g_cpu_slots);
    unlink(FIFO_PATH);
    unlink(SOCK_PATH);

    printf("System shut down.\n");
    return 0;
}

/* =====================================================================
 * ---- from agent.c ---------------------------------------------------
 * Each "AI agent" is still a SEPARATE PROCESS -- it is launched by the
 * orchestrator's scheduler_thread with fork()+exec() (CO2), it just
 * happens to exec() a copy of the same binary running in "--agent"
 * mode instead of a distinct agent executable. Everything below is
 * unchanged agent logic, only prefixed with agent_ to avoid clashing
 * with orchestrator-side names in this merged file.
 * ===================================================================== */
static ResourceTable *agent_attach_shared_memory(void)
{
    int fd = shm_open(SHM_NAME, O_RDWR, 0666);
    if (fd < 0) { perror("agent: shm_open"); exit(1); }

    ResourceTable *rt = mmap(NULL, sizeof(ResourceTable),
                              PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (rt == MAP_FAILED) { perror("agent: mmap"); exit(1); }
    return rt;
}

static void agent_log_line(const char *fmt, ...)
{
    int fd = open(LOG_FILE, O_CREAT | O_WRONLY | O_APPEND, 0644);   /* CO5 */
    if (fd < 0) { perror("agent: open log"); return; }

    char buf[256];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    if (n > 0) write(fd, buf, (size_t)n);
    close(fd);
}

/* CO4: acquire `need` frames from the paging pool, blocking (simulated
 * page fault / wait) on the process-shared condition variable until
 * enough frames are free. Returns the index of the AgentSlot used. */
static int agent_acquire_frames(ResourceTable *rt, const TaskMsg *task, int frames_out[])
{
    pthread_mutex_lock(&rt->mutex);

    while (rt->free_frames < task->pages_needed) {
        printf("[agent %d] PAGE FAULT: need %d frames, only %d free -> waiting\n",
               getpid(), task->pages_needed, rt->free_frames);
        pthread_cond_wait(&rt->frames_available, &rt->mutex);
    }

    /* grab free physical frames */
    int got = 0;
    for (int i = 0; i < TOTAL_FRAMES && got < task->pages_needed; i++) {
        if (rt->frame_owner[i] == -1) {
            rt->frame_owner[i] = task->task_id;
            frames_out[got++] = i;
        }
    }
    rt->free_frames -= got;

    /* find a free AgentSlot to register ourselves in */
    int slot = -1;
    for (int i = 0; i < MAX_AGENTS; i++) {
        if (rt->agents[i].status == 0) { slot = i; break; }
    }
    if (slot == -1) slot = task->task_id % MAX_AGENTS; /* fallback, demo-scale */

    AgentSlot *a = &rt->agents[slot];
    a->pid         = getpid();
    a->task_id     = task->task_id;
    strncpy(a->name, task->name, sizeof(a->name) - 1);
    a->pages_used  = got;
    for (int i = 0; i < got; i++) a->frames[i] = frames_out[i];
    a->status      = 1; /* running */
    pthread_mutex_unlock(&rt->mutex);
    return slot;
}

/* CO4: release frames back to the pool and wake any waiting agents. */
static void agent_release_frames(ResourceTable *rt, int slot, int frames[], int count)
{
    pthread_mutex_lock(&rt->mutex);
    for (int i = 0; i < count; i++) rt->frame_owner[frames[i]] = -1;
    rt->free_frames += count;
    rt->agents[slot].status = 2; /* done */
    rt->total_completed++;
    pthread_cond_broadcast(&rt->frames_available);   /* wake waiters */
    pthread_mutex_unlock(&rt->mutex);
}

/* CO5: write a structured, fixed-size result record at a byte offset
 * computed from task_id, using lseek() for direct/random-access I/O. */
static void agent_write_result_record(const TaskMsg *task, int pages_used)
{
    int fd = open(RESULTS_FILE, O_CREAT | O_WRONLY, 0644);
    if (fd < 0) { perror("agent: open results"); return; }

    ResultRecord rec;
    memset(&rec, 0, sizeof(rec));
    rec.task_id         = task->task_id;
    strncpy(rec.name, task->name, sizeof(rec.name) - 1);
    rec.duration_sec    = task->duration_sec;
    rec.pages_used       = pages_used;
    rec.completion_time = (long)time(NULL);
    rec.status           = 1;

    off_t offset = (off_t)task->task_id * (off_t)sizeof(ResultRecord);
    if (lseek(fd, offset, SEEK_SET) == (off_t)-1) { perror("agent: lseek"); close(fd); return; }
    write(fd, &rec, sizeof(rec));
    close(fd);
}

static void agent_report_via_fifo(const TaskMsg *task)
{
    int fd = open(FIFO_PATH, O_WRONLY);      /* CO3: named pipe (FIFO) */
    if (fd < 0) { perror("agent: open fifo"); return; }

    char msg[128];
    int n = snprintf(msg, sizeof(msg), "%d,%s,done\n", task->task_id, task->name);
    write(fd, msg, (size_t)n);
    close(fd);
}

/* agent_main() (was plain main() in agent.c). task_fd is the fd number
 * (as text, e.g. "3") passed after --agent on the command line. */
static int agent_main(const char *task_fd_str)
{
    int task_fd = atoi(task_fd_str);

    /* CO3: receive the task descriptor sent over the anonymous pipe */
    TaskMsg task;
    if (read(task_fd, &task, sizeof(task)) != (ssize_t)sizeof(task)) {
        perror("agent: read task"); close(task_fd); return 1;
    }
    close(task_fd);

    printf("[agent %d] starting task #%d \"%s\" (duration=%ds, pages=%d)\n",
           getpid(), task.task_id, task.name, task.duration_sec, task.pages_needed);

    ResourceTable *rt = agent_attach_shared_memory();

    int frames[MAX_PAGES_PER_TASK];
    int slot = agent_acquire_frames(rt, &task, frames);          /* CO4 */

    agent_log_line("[task #%d \"%s\"] STARTED pid=%d pages=%d\n",
              task.task_id, task.name, getpid(), task.pages_needed);   /* CO5 */

    sleep(task.duration_sec > 0 ? task.duration_sec : 1);   /* "execute" */

    agent_release_frames(rt, slot, frames, task.pages_needed);    /* CO4 */

    agent_log_line("[task #%d \"%s\"] COMPLETED pid=%d\n",
              task.task_id, task.name, getpid());           /* CO5 */
    agent_write_result_record(&task, task.pages_needed);          /* CO5 */
    agent_report_via_fifo(&task);                                  /* CO3 */

    kill(getppid(), SIGUSR1);                                 /* CO3 signal */

    munmap(rt, sizeof(ResourceTable));

    printf("[agent %d] task #%d finished, exiting\n", getpid(), task.task_id);
    return 0;                                                   /* CO2 */
}

/* =====================================================================
 * ---- from status_client.c -------------------------------------------
 * Connects to the orchestrator's UNIX domain socket (CO3) and prints a
 * live snapshot of the Resource Manager, then exits. Run this ("./mas
 * --status") in a second terminal while "./mas" is running.
 * ===================================================================== */
static int status_client_main(void)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect (is ./mas running?)");
        close(fd);
        return 1;
    }

    StatusSnapshot snap;
    ssize_t n = read(fd, &snap, sizeof(snap));
    close(fd);

    if (n != (ssize_t)sizeof(snap)) {
        fprintf(stderr, "status: short read from orchestrator\n");
        return 1;
    }

    printf("Orchestrator status: %s\n", snap.status);
    printf("  Free memory frames : %d / %d\n", snap.free_frames, snap.total_frames);
    printf("  Active agents      : %d\n", snap.active_agents);
    printf("  Tasks submitted    : %d\n", snap.total_submitted);
    printf("  Tasks completed    : %d\n", snap.total_completed);
    return 0;
}

/* =====================================================================
 * real main() -- mode dispatcher
 * ===================================================================== */
int main(int argc, char *argv[])
{
    if (argc >= 3 && strcmp(argv[1], "--agent") == 0) {
        return agent_main(argv[2]);
    } else if (argc >= 2 && strcmp(argv[1], "--status") == 0) {
        return status_client_main();
    } else if (argc >= 2 && (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0)) {
        printf("usage:\n"
               "  %s              run the orchestrator shell\n"
               "  %s --status     print one live status snapshot\n",
               argv[0], argv[0]);
        return 0;
    }
    return orchestrator_main();
}
