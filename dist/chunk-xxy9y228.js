// @bun
import {
  loadPluginConfig,
  mergeAgentConfigs,
  normalizeFallbackModelsField,
  readFallbackModels,
  createFallbackState,
  isModelInCooldown,
  planFallback,
  commitFallback,
  getErrorMessage,
  isRetryableError,
  isProviderWideRateLimitError,
  FALLBACK_CONTINUATION_PROMPT
} from "./chunk-7t89ck77.js";
import {
  PLUGIN_NAME,
  DEFAULT_CONFIG,
  logInfo,
  logError
} from "./chunk-0wvy6bk9.js";
import {
  fallbackRpc2
} from "./chunk-dg3f0bg1.js";

// v2.ts
import { setTimeout as sleep } from "timers/promises";

// v2-background-task.ts
var BACKGROUND_RUNTIME = Object.freeze({ revision: "final-outcome-v2", moduleID: crypto.randomUUID() });
var RECOVERY_TIMING = { pollMs: 1000, graceMs: 30000 };
var MAX_DELIVERY_FAILURES = 5;
var REGISTRY = Symbol.for("opencode-fallback.v2.background-jobs");
function managerFor(ctx) {
  const registry = globalThis;
  const managers = registry[REGISTRY] ??= new Map;
  const key = JSON.stringify([ctx.location.directory, ctx.location.workspaceID ?? null]);
  let manager = managers.get(key);
  if (!manager) {
    manager = { waiters: new Set };
    managers.set(key, manager);
  }
  return manager;
}
function wake(manager) {
  const waiters = [...manager.waiters];
  manager.waiters.clear();
  for (const resolve of waiters)
    resolve();
}
async function currentLease(manager) {
  for (;; ) {
    const lease = manager.lease;
    if (lease?.active)
      return lease;
    await new Promise((resolve) => manager.waiters.add(resolve));
  }
}
function createBackgroundLease(ctx, coordination) {
  const manager = managerFor(ctx);
  manager.lease?.end();
  let endLease;
  const ended = new Promise((resolve) => {
    endLease = resolve;
  });
  const lease = {
    ctx,
    coordination,
    active: true,
    ended,
    end() {
      lease.active = false;
      endLease();
    }
  };
  manager.lease = lease;
  wake(manager);
  return () => {
    if (manager.lease === lease)
      manager.lease = undefined;
    lease.end();
  };
}
async function startBackgroundSubagent(ctx, coordination, native, input, context) {
  const manager = managerFor(ctx);
  const origin = manager.lease;
  const detached = new AbortController;
  let childSessionID;
  let handedOff = false;
  let resolveStarted;
  const started = new Promise((resolve) => {
    resolveStarted = resolve;
  });
  const nativeRun = native.execute({ ...input, background: false }, {
    ...context,
    signal: detached.signal,
    async progress(update) {
      if (typeof update.sessionID === "string" && !childSessionID) {
        childSessionID = update.sessionID;
        resolveStarted(update.sessionID);
      }
      if (!handedOff && !context.signal.aborted)
        await context.progress(update);
    }
  });
  nativeRun.catch(() => {});
  const settled = nativeRun.then(() => {
    return;
  }, () => {
    return;
  });
  let onParentAbort = () => {};
  const parentAborted = new Promise((resolve) => {
    onParentAbort = () => resolve("aborted");
  });
  if (context.signal.aborted)
    onParentAbort();
  else
    context.signal.addEventListener("abort", onParentAbort, { once: true });
  const handoff = await Promise.race([
    started.then((sessionID) => ({ kind: "started", sessionID })),
    settled.then(() => ({ kind: "settled" })),
    parentAborted.then(() => ({ kind: "aborted" }))
  ]);
  context.signal.removeEventListener("abort", onParentAbort);
  if (handoff.kind === "settled")
    return nativeRun;
  if (handoff.kind === "aborted" || context.signal.aborted) {
    detached.abort();
    await settled;
    if (childSessionID)
      cancelChild(ctx, coordination, childSessionID);
    const failure = await nativeRun.then(() => {
      return;
    }, (error) => ({ error }));
    if (failure)
      throw failure.error;
    throw new DOMException("Subagent cancelled", "AbortError");
  }
  handedOff = true;
  const child = handoff.sessionID;
  const description = stringField(input, "description");
  const agent = stringField(input, "agent") ?? null;
  const notificationID = `msg_${crypto.randomUUID().replace(/-/g, "")}`;
  deliverOutcome(manager, { parentID: context.sessionID, nativeRun, child, detached, description, agent, notificationID, origin });
  const attrs = `sessionID="${child}" state="running" description="${escapeXMLAttribute(description ?? "")}"`;
  return {
    output: { sessionID: child, status: "running", output: "" },
    content: `<subagent ${attrs}>
Running in background.
</subagent>`,
    metadata: { sessionID: child, status: "running" }
  };
}
function escapeXMLAttribute(value) {
  return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&apos;");
}
async function readV2ForegroundSubagentOutput(ctx, sessionID) {
  const messages = await ctx.session.context({ sessionID });
  for (const message of [...messages].reverse()) {
    if (message.type !== "assistant")
      continue;
    if (message.error)
      return;
    const text = message.content.filter((part) => part.type === "text").map((part) => part.text).join(`
`);
    return text.trim() ? text : undefined;
  }
  return;
}
function stringField(input, key) {
  const value = input?.[key];
  return typeof value === "string" ? value : undefined;
}
function cancelChild(ctx, coordination, child) {
  coordination.cancelFallback(child);
  ctx.session.interrupt({ sessionID: child, resume: false }).catch(() => {});
}
function isNativeCancellation(error) {
  if (error instanceof DOMException && error.name === "AbortError")
    return true;
  return error instanceof Error && /^Subagent cancelled\b/.test(error.message);
}
async function deliverOutcome(manager, options) {
  const { parentID, nativeRun, child, detached, description, agent, notificationID } = options;
  try {
    const finished = await finalOutcome(manager, nativeRun, child, detached.signal, options.origin);
    if (!finished)
      return;
    const state = finished.state;
    const body = finished.state === "completed" ? finished.output : finished.state === "error" ? finished.message : "Subagent cancelled.";
    const attrs = `sessionID="${child}" state="${state}" description="${escapeXMLAttribute(description ?? "")}"`;
    await deliver(manager, {
      sessionID: parentID,
      id: notificationID,
      text: `<subagent ${attrs}>
${body}
</subagent>`,
      ...description !== undefined ? { description } : {},
      metadata: { source: "subagent", childID: child, agent, state, "opencode-fallback": true },
      resume: true
    });
  } catch (error) {
    logError("V2 background subagent notification failed", { sessionID: child, error: String(error) });
  }
}
async function deliver(manager, input) {
  let liveFailures = 0;
  for (;; ) {
    const lease = await currentLease(manager);
    try {
      await lease.ctx.session.synthetic(input);
      return;
    } catch (error) {
      if (error?._tag === "ConflictError")
        return;
      if (!lease.active)
        continue;
      liveFailures++;
      if (liveFailures >= MAX_DELIVERY_FAILURES)
        throw error;
      await new Promise((resolve) => setTimeout(resolve, RECOVERY_TIMING.pollMs));
    }
  }
}
async function finalOutcome(manager, nativeRun, child, signal, origin) {
  let nativeError;
  try {
    const native = await nativeRun;
    if (native?.output?.status === "running")
      return;
    return { state: "completed", output: native?.output?.output ?? "" };
  } catch (error) {
    nativeError = error;
  }
  const message = nativeError instanceof Error ? nativeError.message : String(nativeError);
  const active = manager.lease?.active ? manager.lease : undefined;
  if (active?.coordination.isCancelled?.(child))
    return { state: "cancelled" };
  if (active && active === origin && !active.coordination.ownsFallback(child)) {
    return isNativeCancellation(nativeError) ? { state: "cancelled" } : { state: "error", message };
  }
  return recoverChild(manager, child, signal, message);
}
async function recoverChild(manager, child, signal, message) {
  let lease = await currentLease(manager);
  let observed;
  let observedAt = 0;
  for (;; ) {
    if (signal.aborted)
      return { state: "cancelled" };
    if (lease.coordination.ownsFallback(child)) {
      let output;
      try {
        output = await Promise.race([
          lease.coordination.waitForFallback(child, signal),
          lease.ended.then(() => {
            return;
          })
        ]);
      } catch (error) {
        if (!lease.active) {
          lease = await currentLease(manager);
          continue;
        }
        return lease.coordination.isCancelled?.(child) ? { state: "cancelled" } : { state: "error", message };
      }
      if (signal.aborted)
        return { state: "cancelled" };
      if (!lease.active) {
        lease = await currentLease(manager);
        continue;
      }
      if (!output?.trim())
        return lease.coordination.isCancelled?.(child) ? { state: "cancelled" } : { state: "error", message };
      return { state: "completed", output };
    }
    try {
      const info = await lease.ctx.session.get({ sessionID: child });
      if (info.time.idle === undefined) {
        observed = undefined;
      } else if (info.outcome === "succeeded") {
        const output = await readV2ForegroundSubagentOutput(lease.ctx, child);
        return output?.trim() ? { state: "completed", output } : { state: "error", message: "Subagent completed without output." };
      } else if (info.outcome === "failed" || info.outcome === "interrupted") {
        const mark = `${info.outcome}:${info.time.updated}:${info.time.idle}`;
        const now = Date.now();
        if (mark !== observed) {
          observed = mark;
          observedAt = now;
        } else if (now - observedAt >= RECOVERY_TIMING.graceMs) {
          return info.outcome === "failed" ? { state: "error", message } : { state: "cancelled" };
        }
      }
    } catch (error) {
      if (!lease.active) {
        lease = await currentLease(manager);
        observed = undefined;
        continue;
      }
      logError("V2 background recovery poll failed", { sessionID: child, error: String(error) });
    }
    await new Promise((resolve) => setTimeout(resolve, RECOVERY_TIMING.pollMs));
    if (!lease.active) {
      lease = await currentLease(manager);
      observed = undefined;
    }
  }
}

// v2-foreground-task.ts
async function installV2ForegroundTaskFallback(ctx, coordination) {
  let disposed = false;
  const releaseLease = createBackgroundLease(ctx, coordination);
  const registration = await ctx.tool.transform((editor) => {
    const nativeSubagent = editor.list().find((tool) => tool.name === "subagent" || tool.id === "subagent");
    if (!nativeSubagent)
      return;
    editor.update(nativeSubagent.id, (tool) => {
      const native = tool;
      const execute = native.execute;
      native.execute = async (input, context) => {
        if (input?.background === true) {
          return startBackgroundSubagent(ctx, coordination, { execute }, input, context);
        }
        let childSessionID;
        const wrappedContext = {
          ...context,
          async progress(update) {
            if (typeof update.sessionID === "string")
              childSessionID = update.sessionID;
            await context.progress(update);
          }
        };
        try {
          return await execute(input, wrappedContext);
        } catch (error) {
          if (!childSessionID || !coordination.ownsFallback(childSessionID))
            throw error;
          const child = childSessionID;
          const cancelChild = () => {
            coordination.cancelFallback(child);
            ctx.session.interrupt({ sessionID: child, resume: false }).catch(() => {});
          };
          if (context.signal.aborted) {
            cancelChild();
            throw error;
          }
          let output;
          context.signal.addEventListener("abort", cancelChild, { once: true });
          try {
            output = await coordination.waitForFallback(childSessionID, context.signal);
          } catch {
            throw error;
          } finally {
            context.signal.removeEventListener("abort", cancelChild);
          }
          if (context.signal.aborted || !output?.trim())
            throw error;
          const result = { sessionID: childSessionID, status: "completed", output };
          return {
            output: result,
            content: `<subagent sessionID="${childSessionID}" state="completed">
${output}
</subagent>`,
            metadata: { sessionID: childSessionID, status: "completed" }
          };
        }
      };
    });
  });
  return {
    dispose: async () => {
      if (disposed)
        return;
      disposed = true;
      releaseLease();
      await registration.dispose();
    }
  };
}

// v2-notifications.ts
async function registerFallbackNotifications(ctx, options = ctx.options) {
  const noop = { notify: async () => {}, dispose: async () => {} };
  const enabled = options.notify_on_fallback ?? DEFAULT_CONFIG.notify_on_fallback;
  if (!enabled)
    return noop;
  let registration;
  try {
    registration = await ctx.rpc.register(fallbackRpc2, {});
  } catch (error) {
    logError("V2 fallback notifications unavailable", { directory: ctx.location.directory, error: String(error) });
    return noop;
  }
  let disposed = false;
  const notify = async (input) => {
    if (disposed)
      return;
    try {
      await registration.events.emit("notification", {
        id: crypto.randomUUID(),
        directory: ctx.location.directory,
        ...ctx.location.workspaceID ? { workspaceID: ctx.location.workspaceID } : {},
        sessionID: input.sessionID,
        from: input.from,
        to: input.to,
        kind: input.kind
      });
      logInfo("V2 fallback notification emitted", { sessionID: input.sessionID, from: input.from, to: input.to, kind: input.kind });
    } catch (error) {
      logError("V2 fallback notification failed", { sessionID: input.sessionID, error: String(error) });
    }
  };
  const dispose = async () => {
    if (disposed)
      return;
    disposed = true;
    try {
      await registration.dispose();
    } catch (error) {
      logError("V2 fallback notification dispose failed", { error: String(error) });
    }
  };
  return { notify, dispose };
}

// v2-compaction.ts
var CHILD_PROMPT = "Produce the compaction summary.";
var SUMMARY_TEMPLATE = [
  "You MUST use this format for your response (you may omit sections that aren't applicable). Do not include the <template> tags in your response.",
  "<template>",
  "## Objective",
  "- [one or two brief sentences describing what the user is trying to accomplish]",
  "## Requirements",
  '- [constraints, preferences, requirements, and scope boundaries stated by the user, or "(none)"]',
  "## Decisions",
  '- [decisions already made and why, or "(none)"]',
  "## Work State",
  "Break the objective into smaller goals and report which are completed, which are being worked on, and which are blocked.",
  "### Completed",
  '- [goals that have been completed; otherwise "(none)"]',
  "### Active",
  '- [goals currently being worked on; otherwise "(none)"]',
  "### Blocked",
  '- [anything blocking progress, and why; otherwise "(none)"]',
  "## Next Move",
  '1. [ordered list of next actions, or "(none)"]',
  "## Relevant Files",
  'List the files and directories, other than the current working directory, that another agent would need to open to continue this work. Include at most 15, most important first. Do not list every file that was read or changed. Include paths outside the current working directory when relevant. If none, write "(none)".',
  "- `[file or directory path]`: [brief reason it matters]",
  "## Important Context",
  '- [facts the next agent cannot continue without and cannot easily find on its own; or "(none)"]',
  "</template>"
].join(`
`);
var SUMMARY_INSTRUCTION = [
  "You MUST summarize the conversation above into a structured summary that will be given to another agent to resume the work.",
  "If the conversation above contains an existing <conversation-checkpoint>, update that checkpoint into one consolidated summary instead of treating it as ordinary history. Newer history always takes precedence over the existing checkpoint. Preserve its information unless newer history clearly contradicts, supersedes, resolves, or makes it stale, and keep its requirements, decisions, and open questions. Incorporate newer requirements, decisions, progress, and context, and reconcile Work State and Next Move: move completed work out of Active, remove resolved blockers and answered questions, and preserve unresolved or pending work. Do not reproduce the <conversation-checkpoint>, <summary>, or <recent-context> wrapper tags.",
  "Summarize only what the user and the assistant said and did. Leave out instructions and setup the assistant was given rather than told by the user: repository conventions, instruction files such as AGENTS.md, and environment details like the session ID. The next agent receives current versions of all of these separately.",
  SUMMARY_TEMPLATE,
  "Rules:",
  "- Keep each section concise. Use terse, single-line bullets, not prose paragraphs or nested lists.",
  "- Prefer short references over detailed restatement. It is fine to leave out information the next agent can recover from the code or the files listed above.",
  "- Preserve concrete facts, goals, constraints, completed progress, pending work, critical context, file paths, and next steps.",
  "- Preserve exact file paths, symbols, commands, error strings, URLs, and identifiers.",
  "- Carry forward only user questions or requests that remain unanswered or require further action. Do not repeat ones that newer history has answered or resolved. Preserve exact wording when carrying one forward.",
  "- Preserve consequential workflow state, including whether changes are uncommitted, committed, pushed, under review, or merged.",
  "- Do not mention the summary process or that context was compacted.",
  "Do not continue the task or call tools.",
  "Return only the structured summary in the requested format. Do not include a preamble, explanation, or other commentary."
].join(`
`);
var REMOVED_CHILD_TTL_MS = 30000;
var DENY_ALL = [{ action: "*", resource: "*", effect: "deny" }];
var noop = () => {};
function modelName(model) {
  return `${model.providerID}/${model.id}${model.variant && model.variant !== "default" ? `#${model.variant}` : ""}`;
}
function parseModel(value) {
  const slash = value.indexOf("/");
  if (slash < 1 || slash === value.length - 1)
    throw new Error(`Invalid fallback model: ${value}`);
  const [id, variant] = value.slice(slash + 1).split("#");
  if (!id)
    throw new Error(`Invalid fallback model: ${value}`);
  return { providerID: value.slice(0, slash), id, ...variant ? { variant } : {} };
}
function createAttempt(model) {
  let stop;
  const stopped = new Promise((_, reject) => {
    stop = reject;
  });
  stopped.catch(noop);
  return { model, cancelled: false, timedOut: false, firstToken: false, stopped, stop: (error) => stop(error) };
}

class CompactionCancelled extends Error {
  constructor() {
    super("Compaction fallback cancelled");
  }
}
function cancellation() {
  return new CompactionCancelled;
}
function exhausted(cause) {
  return new Error(`Compaction fallback failed: ${getErrorMessage(cause)}`, { cause });
}
async function installV2CompactionFallback(ctx, options) {
  const { config } = options;
  const children = new Map;
  const removed = new Map;
  const runs = new Map;
  const active = new Set;
  const failures = new Map;
  const registrations = [];
  let disposed = false;
  const attemptFail = (attempt, error, timedOut = false) => {
    if (attempt.error !== undefined)
      return;
    attempt.error = error;
    attempt.timedOut = timedOut;
    attempt.stop(error);
  };
  const cancelAttempt = (attempt) => {
    attempt.cancelled = true;
    attemptFail(attempt, cancellation());
    if (attempt.child) {
      const child = attempt.child;
      ctx.session.interrupt({ sessionID: child, resume: false }).catch((error) => {
        if (!disposed)
          logError("V2 compaction child interrupt failed", { sessionID: child, error: String(error) });
      });
    }
  };
  const cancelRun = (run, mode) => {
    if (run.cancel === "hard" || run.cancel === mode)
      return;
    const first = run.cancel === undefined;
    run.cancel = mode;
    if (first && run.attempt)
      cancelAttempt(run.attempt);
  };
  const cancel = (sessionID) => {
    const run = runs.get(sessionID);
    if (run)
      cancelRun(run, "hard");
  };
  const cancelAll = () => {
    for (const run of runs.values())
      cancelRun(run, "soft");
  };
  const race = (attempt, promise) => Promise.race([promise, attempt.stopped]);
  const markFirstToken = (attempt) => {
    attempt.firstToken = true;
    clearTimeout(attempt.timer);
    attempt.timer = undefined;
  };
  const isInternal = (sessionID) => {
    if (children.has(sessionID))
      return true;
    const expires = removed.get(sessionID);
    if (expires === undefined)
      return false;
    if (expires > Date.now())
      return true;
    removed.delete(sessionID);
    return false;
  };
  const rememberRemoved = (sessionID) => {
    const now = Date.now();
    for (const [id, expires] of removed)
      if (expires <= now)
        removed.delete(id);
    removed.set(sessionID, now + REMOVED_CHILD_TTL_MS);
  };
  const discard = async (child, interrupt) => {
    if (interrupt) {
      try {
        await ctx.session.interrupt({ sessionID: child, resume: false });
      } catch (error) {
        if (!disposed)
          logError("V2 compaction child interrupt failed", { sessionID: child, error: String(error) });
      }
    }
    try {
      await ctx.session.remove({ sessionID: child });
    } catch (error) {
      if (!disposed)
        logError("V2 compaction child removal failed", { sessionID: child, error: String(error) });
    }
  };
  const armFirstTokenTimer = (attempt, parentID, state, chain) => {
    attempt.timer = setTimeout(() => {
      if (attempt.firstToken || attempt.cancelled || attempt.error !== undefined)
        return;
      if (!planFallback(parentID, state, chain, config).success)
        return;
      logInfo("V2 compaction first-token window expired", { sessionID: parentID, model: attempt.model });
      attemptFail(attempt, new Error(`First token timeout after ${config.timeout_seconds}s`), true);
    }, config.timeout_seconds * 1000);
    attempt.timer.unref?.();
  };
  async function runAttempt(parentID, run, parentAgent, model, snapshot, state, chain, from) {
    if (run.cancel !== undefined)
      throw cancellation();
    const attempt = createAttempt(model);
    run.attempt = attempt;
    let child;
    let record;
    let prompt;
    let succeeded = false;
    try {
      const admission = ctx.session.create({
        parentID,
        title: "Compaction fallback",
        agent: options.childAgent?.(parentAgent) ?? parentAgent,
        model: parseModel(model),
        location: { directory: ctx.location.directory },
        metadata: { [PLUGIN_NAME]: true, compaction: true },
        permissions: DENY_ALL
      });
      let created;
      try {
        created = await race(attempt, admission);
      } catch (error) {
        if (attempt.cancelled) {
          const orphan = await admission.then((session) => session.id, () => {
            return;
          });
          if (orphan) {
            rememberRemoved(orphan);
            await discard(orphan, true);
          }
        }
        throw error;
      }
      child = created.id;
      record = { attempt, snapshot, state, chain, original: model === state.originalModel, contextApplied: false };
      children.set(child, record);
      if (attempt.cancelled)
        throw cancellation();
      if (from !== undefined)
        notifyFallback(parentID, from, model);
      if (config.timeout_seconds > 0)
        armFirstTokenTimer(attempt, parentID, state, chain);
      prompt = ctx.session.prompt({
        sessionID: child,
        text: CHILD_PROMPT,
        resume: true,
        metadata: { [PLUGIN_NAME]: true }
      });
      await race(attempt, prompt);
      if (attempt.cancelled)
        throw cancellation();
      await race(attempt, ctx.session.wait({ sessionID: child }).catch((error) => {
        attemptFail(attempt, error);
      }));
      if (attempt.error !== undefined)
        return { ok: false, error: attempt.error, forceFallback: attempt.timedOut };
      if (!record.contextApplied) {
        return { ok: false, error: new Error("Compaction child was not scoped by the context hook"), forceFallback: true };
      }
      const messages = await race(attempt, ctx.session.context({ sessionID: child }));
      const assistant = [...messages].reverse().find((message) => message.type === "assistant");
      if (!assistant)
        return { ok: false, error: new Error("Compaction child produced no output"), forceFallback: true };
      if (assistant.error)
        return { ok: false, error: assistant.error, forceFallback: false };
      const summary = assistant.content.filter((part) => part.type === "text").map((part) => part.text).join(`
`).trim();
      if (!summary)
        return { ok: false, error: new Error("Compaction child produced an empty summary"), forceFallback: true };
      const providerState = model === state.originalModel ? assistant.providerState : undefined;
      succeeded = true;
      return {
        ok: true,
        result: {
          summary,
          ...providerState ? { providerState } : {},
          ...assistant.tokens ? { tokens: assistant.tokens } : {},
          metadata: { [PLUGIN_NAME]: true, model, fallback: model !== state.originalModel, chain: chain.length }
        }
      };
    } catch (error) {
      if (attempt.cancelled)
        throw cancellation();
      if (attempt.error !== undefined)
        return { ok: false, error: attempt.error, forceFallback: attempt.timedOut };
      return { ok: false, error, forceFallback: false };
    } finally {
      clearTimeout(attempt.timer);
      attempt.timer = undefined;
      run.attempt = undefined;
      if (prompt)
        await prompt.then(noop, noop);
      if (child) {
        await discard(child, !succeeded);
        children.delete(child);
        rememberRemoved(child);
      }
    }
  }
  const notifyFallback = (sessionID, from, to) => {
    logInfo("V2 compaction fallback admitted", { sessionID, from, to });
    Promise.resolve().then(() => options.notify?.({ sessionID, from, to, kind: "compaction" })).catch((error) => logError("V2 compaction notification failed", { error: String(error) }));
  };
  const execute = async (run, event, snapshot) => {
    const chain = await options.chain(event.agent);
    if (run.cancel !== undefined)
      throw cancellation();
    if (!chain.length)
      return;
    const state = createFallbackState(modelName(event.model));
    const stored = failures.get(event.sessionID);
    if (stored)
      state.failedModels = new Map(stored);
    let first;
    let previous;
    try {
      for (;; ) {
        const outcome = await runAttempt(event.sessionID, run, event.agent, state.currentModel, snapshot, state, chain, previous);
        if (run.cancel !== undefined)
          throw cancellation();
        if (outcome.ok) {
          event.result = outcome.result;
          return;
        }
        first ??= outcome.error;
        if (!outcome.forceFallback && !isRetryableError(outcome.error, config.retry_on_errors, config.retryable_error_patterns))
          throw exhausted(first);
        const blocked = isProviderWideRateLimitError(outcome.error) ? state.currentModel.split("/")[0] : undefined;
        const next = planFallback(event.sessionID, state, chain, config, blocked);
        if (!next.success)
          throw exhausted(first);
        commitFallback(state, next);
        previous = next.failedModel;
        logInfo("V2 compaction fallback selected", { sessionID: event.sessionID, from: next.failedModel, to: next.newModel });
      }
    } finally {
      if (run.cancel === undefined)
        storeFailures(event.sessionID, state);
    }
  };
  const storeFailures = (sessionID, state) => {
    const now = Date.now();
    const cooldownMs = config.cooldown_seconds * 1000;
    const live = new Map([...state.failedModels].filter(([, failedAt]) => now - failedAt < cooldownMs));
    if (live.size)
      failures.set(sessionID, live);
    else
      failures.delete(sessionID);
  };
  const compaction = (event) => {
    if (disposed || event.result || options.streamHealthy?.() === false)
      return;
    options.clearMainTimer?.(event.sessionID);
    const snapshot = { system: event.system, messages: [...event.messages], options: event.options };
    const previous = runs.get(event.sessionID);
    if (previous)
      cancelRun(previous, "hard");
    const run = { sessionID: event.sessionID, startedAt: Date.now() };
    runs.set(event.sessionID, run);
    const work = execute(run, event, snapshot).catch((error) => {
      if (run.cancel === "soft" && error instanceof CompactionCancelled)
        return;
      throw error;
    }).finally(() => {
      if (runs.get(event.sessionID) === run)
        runs.delete(event.sessionID);
    });
    const tracked = work.then(noop, noop);
    active.add(tracked);
    tracked.then(() => {
      active.delete(tracked);
    });
    return work;
  };
  const context = (event) => {
    const record = children.get(event.sessionID);
    if (!record)
      return;
    const copy = { ...record.snapshot, messages: [...record.snapshot.messages] };
    event.system = copy.system;
    const messages = copy.messages;
    messages.push({ role: "user", content: [{ type: "text", text: SUMMARY_INSTRUCTION }] });
    event.messages = messages;
    delete event.tools;
    if (record.original)
      event.options = copy.options;
    record.contextApplied = true;
  };
  const httpRequest = async (event) => {
    if (!children.has(event.sessionID))
      return;
    const request = event.request;
    if (!request.headers.get("content-type")?.toLowerCase().includes("json"))
      return;
    let body;
    try {
      body = await request.clone().json();
    } catch {
      return;
    }
    if (!body || typeof body !== "object" || Array.isArray(body))
      return;
    const payload = body;
    if (!Array.isArray(payload.tools) || payload.tools.length !== 0)
      return;
    const sanitized = { ...payload };
    delete sanitized.tools;
    const headers = new Headers(request.headers);
    headers.delete("content-length");
    event.request = new Request(request, { headers, body: JSON.stringify(sanitized) });
  };
  const retry = (event) => {
    const record = children.get(event.sessionID);
    if (!record || !isRetryableError(event.error, config.retry_on_errors, config.retryable_error_patterns))
      return;
    const blocked = isProviderWideRateLimitError(event.error) ? record.state.currentModel.split("/")[0] : undefined;
    if (planFallback(event.sessionID, record.state, record.chain, config, blocked).success) {
      event.decision = { retry: false };
    }
  };
  const handleEvent = (event) => {
    const data = event.data;
    const sessionID = data.sessionID;
    if (!sessionID)
      return;
    const record = children.get(sessionID);
    if (record) {
      const attempt = record.attempt;
      if (event.type === "session.text.delta" || event.type === "session.reasoning.delta") {
        if (data.delta?.length)
          markFirstToken(attempt);
      } else if (event.type === "session.execution.failed") {
        attemptFail(attempt, data.error ?? new Error("Compaction child failed"));
      } else if (event.type === "session.execution.interrupted" || event.type === "session.deleted") {
        attemptFail(attempt, new Error(`Compaction child ${event.type}`));
      }
      return;
    }
    if (isInternal(sessionID))
      return;
    if (event.type === "session.deleted") {
      failures.delete(sessionID);
      cancel(sessionID);
      return;
    }
    if (event.type === "session.execution.interrupted") {
      const created = event.created;
      const run = runs.get(sessionID);
      if (run && created !== undefined && created < run.startedAt)
        return;
      cancel(sessionID);
    }
  };
  const registerHooks = async () => {
    registrations.push(await ctx.session.hook("context", context));
    registrations.push(await ctx.session.hook("http.request", httpRequest));
    registrations.push(await ctx.session.hook("retry", retry));
    registrations.push(await ctx.session.hook("compaction", compaction));
  };
  try {
    await registerHooks();
  } catch (error) {
    await Promise.all(registrations.map((registration) => registration.dispose()));
    throw error;
  }
  return {
    async dispose() {
      if (disposed)
        return;
      disposed = true;
      cancelAll();
      await Promise.all([...active]);
      failures.clear();
      removed.clear();
      await Promise.all(registrations.map((registration) => registration.dispose()));
    },
    isInternal,
    handleEvent,
    cancel,
    cancelAll
  };
}

// v2.ts
var V2_RUNTIME2 = Object.freeze({
  revision: "streaming-ttft-v3",
  moduleID: crypto.randomUUID(),
  buildID: "2026-10-26T13:49:17.190Z"
});
function parseModelRef2(value) {
  const slash = value.indexOf("/");
  if (slash < 1 || slash === value.length - 1)
    throw new Error(`Invalid fallback model: ${value}`);
  const [id, variant] = value.slice(slash + 1).split("#");
  if (!id)
    throw new Error(`Invalid fallback model: ${value}`);
  return { providerID: value.slice(0, slash), id, ...variant ? { variant } : {} };
}
function modelName2(model) {
  return `${model.providerID}/${model.id}${model.variant && model.variant !== "default" ? `#${model.variant}` : ""}`;
}
async function withSignal(promise, signal) {
  if (signal.aborted)
    throw signal.reason;
  let abort;
  try {
    return await Promise.race([promise, new Promise((_, reject) => {
      abort = () => reject(signal.reason);
      signal.addEventListener("abort", abort, { once: true });
    })]);
  } finally {
    signal.removeEventListener("abort", abort);
  }
}
async function setupV22(ctx) {
  const file = loadPluginConfig(ctx.location.directory);
  const options = ctx.options;
  const config = { ...DEFAULT_CONFIG, ...file, ...options };
  if (!config.enabled)
    return;
  const agents = { ...file.agents, ...options.agents };
  const resolvedAgents = {};
  const normalizeModels = (value) => normalizeFallbackModelsField(value).map((model) => modelName2(parseModelRef2(model)));
  const globalModels = normalizeModels(config.fallback_models);
  for (const model of [...globalModels, ...Object.values(agents).flatMap((agent) => normalizeFallbackModelsField(agent.fallback_models))]) {
    parseModelRef2(model);
  }
  const states = new Map;
  const controller = new AbortController;
  const registrations = [];
  let disposed = false;
  let streamHealthy = true;
  let compaction;
  let inventory;
  let inventoryUpdatedAt = 0;
  let inventoryRequest;
  const refreshInventory = async (force = false) => {
    const ttl = inventory ? 30000 : 5000;
    if (!force && inventoryUpdatedAt > 0 && Date.now() - inventoryUpdatedAt < ttl)
      return;
    if (inventoryRequest)
      return inventoryRequest;
    const request = (async () => {
      try {
        const lookupSignal = AbortSignal.any([controller.signal, AbortSignal.timeout(5000)]);
        const { data } = await withSignal(ctx.model.list({ location: { directory: ctx.location.directory } }), lookupSignal);
        inventory = new Map(data.filter((model) => model.enabled).map((model) => [
          `${model.providerID}/${model.modelID}`,
          new Set(model.variants.map((variant) => variant.id))
        ]));
      } catch (error) {
        inventory = undefined;
        logError("V2 model inventory unavailable", { error: String(error) });
      } finally {
        inventoryUpdatedAt = Date.now();
      }
    })();
    inventoryRequest = request;
    try {
      await request;
    } finally {
      if (inventoryRequest === request)
        inventoryRequest = undefined;
    }
  };
  const available = (name) => {
    if (!inventory)
      return false;
    const { providerID, id, variant } = parseModelRef2(name);
    const variants = inventory.get(`${providerID}/${id}`);
    return variants !== undefined && (!variant || variant === "default" || variants.has(variant));
  };
  const resolveAgentFallback = async (agentID) => {
    await refreshInventory();
    try {
      const lookupSignal = AbortSignal.any([controller.signal, AbortSignal.timeout(5000)]);
      const { data: agent } = await withSignal(ctx.agent.get({ agentID, location: { directory: ctx.location.directory } }), lookupSignal);
      const models = normalizeFallbackModelsField(agent.request.body?.fallback_models).flatMap((model) => {
        try {
          return [modelName2(parseModelRef2(model))];
        } catch (error) {
          logError("V2 invalid legacy fallback model ignored", { agent: agentID, model, error: String(error) });
          return [];
        }
      });
      const value = { model: agent.model && modelName2(agent.model), fallback_models: models };
      if (JSON.stringify(resolvedAgents[agentID]) !== JSON.stringify(value)) {
        resolvedAgents[agentID] = value;
        logInfo("V2 agent fallback configuration loaded", { directory: ctx.location.directory, agent: agentID, ...value });
      }
    } catch (error) {
      logError("V2 agent fallback configuration unavailable", { agent: agentID, error: String(error) });
    }
  };
  const notifications = await registerFallbackNotifications(ctx, config);
  registrations.push(notifications);
  const configuredChain = (agent) => {
    const merged = mergeAgentConfigs(resolvedAgents, agents);
    const models = normalizeModels(readFallbackModels(agent, merged));
    if (!models.length)
      return globalModels;
    const primary = merged?.[agent]?.model;
    if (primary && !models.includes(primary))
      models.unshift(primary);
    return models;
  };
  const chain = (agent) => configuredChain(agent).filter(available);
  const clearTimer = (state) => {
    clearTimeout(state.timer);
    state.timer = undefined;
  };
  const clearPending = (state) => {
    state.pending = undefined;
    state.pendingBlockedProvider = undefined;
  };
  const track = (sessionID, agent, model) => {
    const name = modelName2(model);
    let state = states.get(sessionID);
    if (!state || !state.pending && state.fallback.currentModel !== name) {
      if (state)
        clearTimer(state);
      state = { fallback: createFallbackState(name), agent, cancelled: false, expectedInterrupts: 0, active: false, awaitingExecution: false, lastAccess: Date.now() };
      states.set(sessionID, state);
    }
    state.agent = agent;
    state.lastAccess = Date.now();
    return state;
  };
  const plan = (sessionID, state, error, timeout = false) => {
    if (disposed || state.cancelled || !streamHealthy)
      return;
    if (state.requestKind !== "primary")
      return;
    if (!timeout && !isRetryableError(error, config.retry_on_errors, config.retryable_error_patterns))
      return;
    if (state.pending)
      return state.pending;
    const blockedProvider = isProviderWideRateLimitError(error) ? state.fallback.currentModel.split("/")[0] : undefined;
    const result = planFallback(sessionID, state.fallback, chain(state.agent), config, blockedProvider);
    if (!result.success)
      return;
    state.pending = result;
    state.pendingBlockedProvider = blockedProvider;
    clearTimer(state);
    return result;
  };
  const dispatch = (sessionID, state, interrupt = false) => {
    if (!state.pending || state.cancelled || disposed)
      return;
    state.pendingReady = true;
    state.pendingInterrupt ||= interrupt;
    if (state.dispatch)
      return;
    let pending = state.pending;
    state.recoveryFailed = false;
    const shouldInterrupt = state.pendingInterrupt;
    state.pendingReady = false;
    state.pendingInterrupt = false;
    state.dispatch = (async () => {
      if (shouldInterrupt) {
        state.expectedInterrupts++;
        try {
          const result = await ctx.session.interrupt({ sessionID, resume: false }, { signal: controller.signal });
          if (!result.interrupted)
            state.expectedInterrupts = Math.max(0, state.expectedInterrupts - 1);
        } catch (error) {
          state.expectedInterrupts = Math.max(0, state.expectedInterrupts - 1);
          throw error;
        }
      }
      await ctx.session.wait({ sessionID }, { signal: controller.signal });
      if (disposed || state.cancelled || state.pending !== pending)
        return;
      await refreshInventory(true);
      if (disposed || state.cancelled || state.pending !== pending)
        return;
      if (!available(pending.newModel)) {
        const refreshed = planFallback(sessionID, state.fallback, chain(state.agent), config, state.pendingBlockedProvider);
        if (!refreshed.success) {
          clearPending(state);
          state.active = false;
          state.awaitingExecution = false;
          state.recoveryFailed = true;
          return;
        }
        pending = refreshed;
        state.pending = refreshed;
      }
      await ctx.session.switchModel({ sessionID, model: parseModelRef2(pending.newModel) }, { signal: controller.signal });
      if (disposed || state.cancelled || state.pending !== pending)
        return;
      commitFallback(state.fallback, pending);
      state.ownedExecution = true;
      clearPending(state);
      state.messageID = undefined;
      state.active = false;
      state.requestToken = undefined;
      state.awaitingExecution = true;
      state.requestStartedAt = Date.now();
      await ctx.session.prompt({
        sessionID,
        text: FALLBACK_CONTINUATION_PROMPT,
        resume: true,
        metadata: { [PLUGIN_NAME]: true }
      }, { signal: controller.signal });
      if (state.cancelled || state.parentCancelled) {
        await withSignal(ctx.session.interrupt({ sessionID, resume: false }), controller.signal);
        return;
      }
      logInfo("V2 fallback dispatched", { sessionID, from: pending.failedModel, model: pending.newModel });
      notifications.notify({ sessionID, from: pending.failedModel, to: pending.newModel, kind: "primary" });
    })().catch((error) => {
      if (!disposed)
        logError("V2 fallback dispatch failed", { sessionID, error: String(error) });
      if (!state.pending || state.pending === pending) {
        clearPending(state);
        state.active = false;
        state.awaitingExecution = false;
        state.recoveryFailed = true;
      }
    }).finally(() => {
      state.dispatch = undefined;
      if (state.pendingReady)
        dispatch(sessionID, state, state.pendingInterrupt);
    });
  };
  const startRequest = (sessionID, state) => {
    clearTimer(state);
    state.messageID = undefined;
    state.cancelled = false;
    state.active = true;
    state.awaitingExecution = false;
    state.recoveryFailed = false;
    state.requestKind = "primary";
    state.requestStartedAt = Date.now();
    const token = Symbol("request");
    state.requestToken = token;
    if (config.timeout_seconds <= 0 || !chain(state.agent).length)
      return;
    state.timer = setTimeout(() => {
      if (state.requestToken !== token || state.cancelled)
        return;
      logInfo("V2 first-token window expired", { sessionID, messageID: state.messageID });
      if (plan(sessionID, state, undefined, true))
        dispatch(sessionID, state, true);
    }, config.timeout_seconds * 1000);
    state.timer.unref();
  };
  const retry = (event) => {
    const state = states.get(event.sessionID);
    if (!state || state.requestKind !== "primary" || state.agent !== event.agent || state.fallback.currentModel !== modelName2(event.model))
      return;
    if (plan(event.sessionID, state, event.error))
      event.decision = { retry: false };
    else if (!disposed && !state.cancelled && streamHealthy && state.ownedExecution && isRetryableError(event.error, config.retry_on_errors, config.retryable_error_patterns)) {
      event.decision = { retry: false };
    }
  };
  const prompt = async (event) => {
    if (event.metadata?.[PLUGIN_NAME])
      return;
    const state = states.get(event.sessionID);
    if (state)
      state.parentCancelled = false;
    if (!state || state.active || state.dispatch)
      return;
    try {
      clearTimer(state);
      clearPending(state);
      state.cancelled = false;
      state.parentCancelled = false;
      state.lastAccess = Date.now();
      const current = await ctx.session.get({ sessionID: event.sessionID });
      if (current.model && modelName2(current.model) !== state.fallback.currentModel) {
        states.delete(event.sessionID);
        return;
      }
      const original = state.fallback.originalModel;
      if (original !== state.fallback.currentModel && !isModelInCooldown(original, state.fallback, config.cooldown_seconds)) {
        await ctx.session.switchModel({ sessionID: event.sessionID, model: parseModelRef2(original) });
        state.fallback = createFallbackState(original);
      } else {
        state.fallback.attemptCount = 0;
        state.fallback.fallbackIndex = -1;
      }
    } catch (error) {
      logError("V2 cooldown recovery failed", { sessionID: event.sessionID, error: String(error) });
    }
  };
  const handleEvent = (event) => {
    if (event.location && (event.location.directory !== ctx.location.directory || ("workspaceID" in event.location) && event.location.workspaceID !== ctx.location.workspaceID))
      return;
    if (event.type === "model.updated" || event.type === "provider.updated") {
      refreshInventory(true);
      return;
    }
    compaction?.handleEvent(event);
    const data = event.data;
    if (!("sessionID" in data))
      return;
    const sessionID = data.sessionID;
    if (compaction?.isInternal(sessionID))
      return;
    if (event.type === "session.step.started") {
      if (!event.location && !states.has(sessionID))
        return;
      const state = track(sessionID, event.data.agent, event.data.model);
      if (state.messageID === event.data.assistantMessageID)
        return;
      if (!state.requestToken)
        startRequest(sessionID, state);
      state.messageID = event.data.assistantMessageID;
      return;
    }
    const state = states.get(sessionID);
    if (!state)
      return;
    state.lastAccess = Date.now();
    if ("assistantMessageID" in data && data.assistantMessageID !== state.messageID)
      return;
    if (event.type === "session.text.delta" || event.type === "session.reasoning.delta" || event.type === "session.tool.input.delta") {
      if (event.data.delta.length)
        clearTimer(state);
    } else if (event.type === "session.text.started" || event.type === "session.reasoning.started" || event.type === "session.tool.input.started" || event.type === "session.tool.called" || event.type === "session.step.ended") {
      clearTimer(state);
    } else if (event.type === "session.step.failed") {
      clearTimer(state);
      plan(sessionID, state, event.data.error);
    } else if (event.type === "session.execution.failed") {
      clearTimer(state);
      state.active = false;
      state.awaitingExecution = false;
      state.requestToken = undefined;
      if (plan(sessionID, state, event.data.error))
        dispatch(sessionID, state);
      else {
        state.ownedExecution = false;
        if (!state.dispatch)
          clearPending(state);
      }
    } else if (event.type === "session.execution.interrupted" || event.type === "session.deleted") {
      if (event.type === "session.execution.interrupted" && state.expectedInterrupts > 0) {
        state.expectedInterrupts--;
        return;
      }
      clearTimer(state);
      state.active = false;
      state.cancelled = true;
      state.ownedExecution = false;
      state.awaitingExecution = false;
      clearPending(state);
      state.pendingReady = false;
      if (event.type === "session.deleted")
        states.delete(sessionID);
    } else if (event.type === "session.idle" || event.type === "session.execution.succeeded") {
      if (state.dispatch || state.awaitingExecution || state.requestStartedAt !== undefined && event.created < state.requestStartedAt)
        return;
      clearTimer(state);
      state.active = false;
      state.requestToken = undefined;
      clearPending(state);
      state.pendingReady = false;
      state.expectedInterrupts = 0;
      state.ownedExecution = false;
    } else if (event.type === "session.compaction.failed") {
      clearTimer(state);
      state.requestKind = "compaction";
      clearPending(state);
    }
  };
  try {
    compaction = await installV2CompactionFallback(ctx, {
      config,
      childAgent: () => "compaction",
      streamHealthy: () => streamHealthy,
      chain: async (agent) => {
        await resolveAgentFallback("compaction");
        if (Object.hasOwn(agents, "compaction") || normalizeFallbackModelsField(readFallbackModels("compaction", resolvedAgents)).length)
          return chain("compaction");
        if (agent !== "compaction")
          await resolveAgentFallback(agent);
        return chain(agent);
      },
      notify: notifications.notify,
      clearMainTimer: (sessionID) => {
        const state = states.get(sessionID);
        if (state) {
          clearTimer(state);
          state.requestKind = "compaction";
        }
      }
    });
    registrations.push(compaction);
    if (config.foreground_task_fallback) {
      registrations.push(await installV2ForegroundTaskFallback(ctx, {
        isCancelled: (sessionID) => states.get(sessionID)?.cancelled === true,
        cancelFallback: (sessionID) => {
          compaction?.cancel(sessionID);
          const state = states.get(sessionID);
          if (!state)
            return;
          clearTimer(state);
          state.cancelled = true;
          state.parentCancelled = true;
          clearPending(state);
          state.pendingReady = false;
          state.active = false;
          state.awaitingExecution = false;
          state.requestToken = undefined;
        },
        ownsFallback: (sessionID) => {
          const state = states.get(sessionID);
          return Boolean(state && !state.cancelled && (state.pending || state.dispatch || state.fallback.attemptCount > 0 || state.active && chain(state.agent).length));
        },
        async waitForFallback(sessionID, signal) {
          const abort = AbortSignal.any([signal, controller.signal]);
          while (!abort.aborted) {
            const state = states.get(sessionID);
            if (!state || state.cancelled || state.recoveryFailed || disposed)
              return;
            if (!state.pending && !state.dispatch && !state.awaitingExecution) {
              const current = await withSignal(ctx.session.get({ sessionID }), abort);
              if (!state.pending && !state.dispatch && !state.awaitingExecution && !state.cancelled && !abort.aborted) {
                const hostIdle = current.time?.idle !== undefined && current.time.idle >= (state.requestStartedAt ?? 0);
                const terminal = hostIdle || !state.active && current.time?.idle === undefined;
                if (state.fallback.attemptCount > 0 && current.outcome === "succeeded" && terminal) {
                  return withSignal(readV2ForegroundSubagentOutput(ctx, sessionID), abort);
                }
                if (current.outcome && terminal)
                  return;
              }
            }
            try {
              await sleep(50, undefined, { signal: abort });
            } catch {
              return;
            }
          }
        }
      }));
    }
    registrations.push(await ctx.session.hook("context", async (event) => {
      if (disposed || compaction?.isInternal(event.sessionID))
        return;
      if (states.get(event.sessionID)?.parentCancelled) {
        ctx.session.interrupt({ sessionID: event.sessionID, resume: false }).catch((error) => {
          if (!disposed)
            logError("V2 cancelled child interrupt failed", { sessionID: event.sessionID, error: String(error) });
        });
        return;
      }
      const state = track(event.sessionID, event.agent, event.model);
      clearTimer(state);
      state.cancelled = false;
      state.active = true;
      state.awaitingExecution = false;
      state.requestStartedAt = Date.now();
      const token = Symbol("context");
      state.requestToken = token;
      await resolveAgentFallback(event.agent);
      if (!disposed && !state.cancelled && states.get(event.sessionID) === state && state.requestToken === token) {
        startRequest(event.sessionID, state);
      }
    }));
    registrations.push(await ctx.session.hook("model.request", (event) => {
      const state = states.get(event.sessionID);
      if (state && state.fallback.currentModel === modelName2(event.model)) {
        if (state.active && event.kind !== "primary" && event.kind !== "compaction")
          return;
        state.requestKind = event.kind;
        if (event.kind === "compaction")
          clearTimer(state);
      }
    }));
    registrations.push(await ctx.session.hook("retry", retry));
    registrations.push(await ctx.session.hook("prompt", prompt));
  } catch (error) {
    await Promise.all(registrations.map((registration) => registration.dispose()));
    throw error;
  }
  (async () => {
    while (!disposed) {
      try {
        for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
          streamHealthy = true;
          handleEvent(event);
        }
      } catch (error) {
        if (!disposed)
          logError("V2 fallback event subscription failed", { error: String(error) });
      }
      streamHealthy = false;
      compaction?.cancelAll();
      for (const state of states.values()) {
        clearTimer(state);
        state.active = false;
        state.awaitingExecution = false;
        if (!state.dispatch)
          clearPending(state);
      }
      try {
        await sleep(1000, undefined, { signal: controller.signal });
      } catch {
        break;
      }
    }
  })();
  const cleanupInterval = setInterval(() => {
    for (const [id, state] of states) {
      if (!state.active && !state.awaitingExecution && !state.dispatch && !state.timer && Date.now() - state.lastAccess > 30 * 60 * 1000)
        states.delete(id);
    }
  }, 5 * 60 * 1000);
  cleanupInterval.unref();
  logInfo("V2 fallback plugin initialized", {
    directory: ctx.location.directory,
    models: globalModels,
    runtime: V2_RUNTIME2,
    backgroundRuntime: BACKGROUND_RUNTIME,
    generationID: crypto.randomUUID()
  });
  return async () => {
    disposed = true;
    controller.abort();
    clearInterval(cleanupInterval);
    for (const state of states.values()) {
      state.cancelled = true;
      clearTimer(state);
    }
    states.clear();
    await Promise.all(registrations.map((registration) => registration.dispose()));
  };
}
var definition = { id: PLUGIN_NAME, setup: setupV22 };
var v2_default = {
  ...definition,
  async server(...args) {
    return (await import("./v1.js")).default(...args);
  }
};

export { V2_RUNTIME2, parseModelRef2, setupV22, v2_default };
