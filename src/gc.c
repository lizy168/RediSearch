/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <pthread.h>
#include <assert.h>
#include <stdint.h>
#include <unistd.h>

#include "gc.h"
#include "fork_gc.h"
#include "disk_gc.h"
#include "config.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "module.h"
#include "spec.h"
#include "thpool/thpool.h"
#include "rmutil/rm_assert.h"
#include "util/logging.h"

#define GC_TIMER_ID_RESCHEDULE ((RedisModuleTimerID)UINT64_MAX)

static redisearch_thpool_t *gcThreadpool_g = NULL;
// Guarded by the Redis main thread/GIL. Used only to initialize or start GC
// contexts created while a consistency window is already open; live contexts
// are paused/resumed by walking IndexSpec->gc.
static bool gcSchedulingPauseActiveForConsistency_g = false;

typedef struct GCDebugTask {
  GCContext* gc;
  RedisModuleBlockedClient* bClient;
} GCDebugTask;

static GCDebugTask *GCDebugTaskCreate(GCContext *gc, RedisModuleBlockedClient* bClient) {
  GCDebugTask *task = rm_new(GCDebugTask);
  task->gc = gc;
  task->bClient = bClient;
  return task;
}

static bool GCContext_SchedulingPausedForConsistency(GCContext *gc) {
  return RS_AtomicBoolLoadRelaxed(&gc->schedulingPausedForConsistency);
}

static void GCContext_SetSchedulingPausedForConsistency(GCContext *gc, bool paused) {
  RS_AtomicBoolStoreRelaxed(&gc->schedulingPausedForConsistency, paused);
}

static bool GCContext_HasArmedTimer(GCContext *gc) {
  return gc->timerID != 0 && gc->timerID != GC_TIMER_ID_RESCHEDULE;
}

GCContext* GCContext_CreateGC(StrongRef spec_ref, uint32_t gcPolicy) {
  GCContext* ret = rm_calloc(1, sizeof(GCContext));
  GCContext_SetSchedulingPausedForConsistency(ret, gcSchedulingPauseActiveForConsistency_g);
  switch (gcPolicy) {
    case GCPolicy_Fork:
      ret->gcCtx = FGC_Create(spec_ref, &ret->callbacks);
      break;
    case GCPolicy_Disk:
      ret->gcCtx = DiskGC_Create(spec_ref, &ret->callbacks);
      break;
    default:
      RS_LOG_ASSERT(false, "Invalid GC policy");
      break;
  }
  return ret;
}

static void timerCallback(RedisModuleCtx* ctx, void* data);

static long long getNextPeriod(GCContext* gc) {
  struct timespec interval = gc->callbacks.getInterval(gc->gcCtx);
  long long ms = interval.tv_sec * 1000 + interval.tv_nsec / 1000000;  // convert to millisecond

  // add randomness to avoid congestion by multiple GCs from different shards
  ms += (rand() % interval.tv_sec) * 1000;

  return ms;
}

static RedisModuleTimerID scheduleNext(GCContext *gc) {
  if (RS_IsMock) return 0;

  long long period = getNextPeriod(gc);
  return RedisModule_CreateTimer(RSDummyContext, period, timerCallback, gc);
}

static void GCContext_ScheduleNextIfEnabled(GCContext *gc) {
  if (gc->timerID) {
    gc->timerID = scheduleNext(gc);
    if (gc->timerID == 0) {
      RedisModule_Log(RSDummyContext, "warning", "GC did not schedule next collection");
    }
  } else {
    // ... unless the GC was stopped by a debug command or a consistency window.
    RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_DEBUG,
                    "GC %p: Not scheduling next collection", gc);
  }
}

static void GCContext_ScheduleNextAfterRun(GCContext *gc) {
  while (RedisModule_ThreadSafeContextTryLock(RSDummyContext) != REDISMODULE_OK) {
    if (GCContext_SchedulingPausedForConsistency(gc)) {
      return;
    }
    usleep(1);
  }

  if (GCContext_SchedulingPausedForConsistency(gc)) {
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    return;
  }
  GCContext_ScheduleNextIfEnabled(gc);
  RedisModule_ThreadSafeContextUnlock(RSDummyContext);
}

static void taskCallback(void* data) {
  GCContext* gc = data;

  bool ret = gc->callbacks.periodicCallback(gc->gcCtx, false);

  if (ret) { // The common case
    // The index was not freed. We need to reschedule the task.
    GCContext_ScheduleNextAfterRun(gc);
  } else {
    // The index was freed. There is no need to reschedule the task.
    // We need to free the task and the GC.
    RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_VERBOSE, "GC %p: Self-Terminating. Index was freed.", gc);
    gc->callbacks.onTerm(gc->gcCtx);
    rm_free(gc);
  }
}

static void debugTaskCallback(void* data) {
  GCDebugTask *task = data;
  GCContext* gc = task->gc;
  RedisModuleBlockedClient* bc = task->bClient;

  gc->callbacks.periodicCallback(gc->gcCtx, true);

  // if GC was invoke by debug command, we release the client
  // and terminate without rescheduling the task again.
  if (bc) RedisModule_UnblockClient(bc, NULL);
  rm_free(task);
}

static void timerCallback(RedisModuleCtx* ctx, void* data) {
  GCContext* gc = data;
  if (GCContext_SchedulingPausedForConsistency(gc)) {
    return;
  }

  if (RedisModule_AvoidReplicaTraffic && RedisModule_AvoidReplicaTraffic()) {
    // If slave traffic is not allowed it means that there is a state machine running
    // we do not want to run any GC which might cause a FORK process to start for example.
    // Its better to just avoid it.
    gc->timerID = scheduleNext(gc);
    return;
  }
  gc->timerID = GC_TIMER_ID_RESCHEDULE;
  redisearch_thpool_add_work(gcThreadpool_g, taskCallback, data, THPOOL_PRIORITY_HIGH);
}

void GCContext_StartNow(GCContext* gc) {
  RS_LOG_ASSERT_FMT(gc->timerID == 0, "GC %p: StartNow called while GC is already running", gc);
  if (GCContext_SchedulingPausedForConsistency(gc)) {
    gc->timerID = GC_TIMER_ID_RESCHEDULE;
    return;
  }
  gc->timerID = GC_TIMER_ID_RESCHEDULE; // Set to non-zero value to indicate the GC to reschedule itself.
  redisearch_thpool_add_work(gcThreadpool_g, taskCallback, gc, THPOOL_PRIORITY_HIGH);
}

void GCContext_Start(GCContext* gc) {
  if (GCContext_SchedulingPausedForConsistency(gc)) {
    gc->timerID = GC_TIMER_ID_RESCHEDULE;
    return;
  }
  gc->timerID = scheduleNext(gc);
  if (gc->timerID == 0) {
    RedisModule_Log(RSDummyContext, "warning", "GC did not schedule next collection");
  }
}

void GCContext_StopFutureRuns(GCContext* gc) {
  if (!gc) {
    return;
  }
  if (GCContext_HasArmedTimer(gc)) {
    RedisModule_StopTimer(RSDummyContext, gc->timerID, NULL);
  }
  gc->timerID = 0;
}

void GCContext_PauseSchedulingForConsistency(GCContext* gc) {
  if (!gc) {
    return;
  }
  GCContext_SetSchedulingPausedForConsistency(gc, true);
  if (GCContext_HasArmedTimer(gc)) {
    RedisModule_StopTimer(RSDummyContext, gc->timerID, NULL);
  }
  if (gc->timerID) {
    gc->timerID = GC_TIMER_ID_RESCHEDULE;
  }
}

void GCContext_ResumeSchedulingAfterConsistency(GCContext* gc) {
  if (!gc || !GCContext_SchedulingPausedForConsistency(gc)) {
    return;
  }
  bool resume = gc->timerID != 0;
  gc->timerID = 0;
  GCContext_SetSchedulingPausedForConsistency(gc, false);
  if (resume) {
    GCContext_Start(gc);
  }
}

void GCContext_StopMock(GCContext* gc) {
  gc->callbacks.onTerm(gc->gcCtx);
  rm_free(gc);
}

void GCContext_RenderStats(GCContext* gc, RedisModule_Reply* reply) {
  gc->callbacks.renderStats(reply, gc->gcCtx);
}

void GCContext_RenderStatsForInfo(GCContext* gc, RedisModuleInfoCtx* ctx) {
  gc->callbacks.renderStatsForInfo(ctx, gc->gcCtx);
}

void GCContext_OnDelete(GCContext* gc) {
  if (gc->callbacks.onDelete) {
    gc->callbacks.onDelete(gc->gcCtx);
  }
}

void GCContext_OnWrite(GCContext* gc) {
  if (gc->callbacks.onWrite) {
    gc->callbacks.onWrite(gc->gcCtx);
  }
}

void GCContext_OnUpdate(GCContext* gc) {
  if (gc->callbacks.onUpdate) {
    gc->callbacks.onUpdate(gc->gcCtx);
  }
}

void GCContext_GetStats(GCContext* gc, InfoGCStats* out) {
  gc->callbacks.getStats(gc->gcCtx, out);
}

void GCContext_CommonForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc) {
  GCDebugTask *task = GCDebugTaskCreate(gc, bc);
  redisearch_thpool_add_work(gcThreadpool_g, debugTaskCallback, task, THPOOL_PRIORITY_HIGH);
}

void GCContext_ForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc) {
  GCContext_CommonForceInvoke(gc, bc);
}

void GCContext_ForceBGInvoke(GCContext* gc) {
  GCContext_CommonForceInvoke(gc, NULL);
}

static void GCContext_UnblockClient(void* data) {
  RedisModuleBlockedClient *bc = data;
  RedisModule_BlockedClientMeasureTimeEnd(bc);
  RedisModule_UnblockClient(bc, NULL);
}

void GCContext_WaitForAllOperations(RedisModuleBlockedClient* bc) {
  redisearch_thpool_add_work(gcThreadpool_g, GCContext_UnblockClient, bc, THPOOL_PRIORITY_HIGH);
}

void GC_ThreadPoolStart() {
  if (gcThreadpool_g == NULL) {
    gcThreadpool_g = redisearch_thpool_create(GC_THREAD_POOL_SIZE, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD, LogCallback, "gc");
  }
}

void GC_ThreadPoolDestroy() {
  if (gcThreadpool_g != NULL) {
    if (redisearch_thpool_paused(gcThreadpool_g)) {
      gcSchedulingPauseActiveForConsistency_g = false;
      redisearch_thpool_resume_threads(gcThreadpool_g);
    }
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    redisearch_thpool_destroy(gcThreadpool_g);
    gcThreadpool_g = NULL;
    RedisModule_ThreadSafeContextLock(RSDummyContext);
  }
}

void GC_ThreadPoolPauseForConsistency(void) {
  gcSchedulingPauseActiveForConsistency_g = true;
  if (gcThreadpool_g) {
    redisearch_thpool_pause_threads_no_wait(gcThreadpool_g);
  }
}

void GC_ThreadPoolWaitForPause(void) {
  if (gcThreadpool_g) {
    redisearch_thpool_pause_threads(gcThreadpool_g);
  }
}

void GC_ThreadPoolResumeAfterConsistency(void) {
  gcSchedulingPauseActiveForConsistency_g = false;
  if (gcThreadpool_g && redisearch_thpool_paused(gcThreadpool_g)) {
    redisearch_thpool_resume_threads(gcThreadpool_g);
  }
}
