// @bun
import {
  PLUGIN_NAME,
  DEFAULT_CONFIG,
  withLogContext,
  logInfo,
  logError
} from "./chunk-0wvy6bk9.js";
import {
  loadPluginConfig,
  mergeAgentConfigs,
  normalizeFallbackModelsField,
  resolveAgentForSession,
  getFallbackModelsForSession,
  createFallbackState,
  isModelInCooldown,
  planFallback,
  commitFallback,
  recoverToOriginal,
  getErrorMessage,
  extractStatusCode,
  extractErrorName,
  classifyErrorType,
  extractAutoRetrySignal,
  containsErrorContent,
  detectErrorInTextParts,
  extractErrorContentFromParts,
  isRetryableError,
  isProviderWideRateLimitError,
  FALLBACK_CONTINUATION_PROMPT
} from "./chunk-7t89ck77.js";

// retry-lock.ts
function acquireRetryLock(deps, sessionID) {
  const token = Symbol(sessionID);
  const owners = deps.sessionRetryLockTokens ??= new Map;
  owners.set(sessionID, token);
  deps.sessionRetryInFlight.add(sessionID);
  return token;
}
function releaseRetryLock(deps, sessionID, token) {
  if (deps.sessionRetryLockTokens?.get(sessionID) !== token)
    return;
  deps.sessionRetryLockTokens?.delete(sessionID);
  deps.sessionRetryInFlight.delete(sessionID);
}

// message-replay.ts
var TIER_2_TYPES = new Set(["text", "image"]);
function filterPartsByTier(parts, tier) {
  switch (tier) {
    case 1:
      return parts;
    case 2:
      return parts.filter((p) => TIER_2_TYPES.has(p.type));
    case 3:
      return parts.filter((p) => p.type === "text");
  }
}
async function replayWithDegradation(allParts, sendFn) {
  if (allParts.length === 0) {
    return { success: false, error: "No parts to replay" };
  }
  const tiers = [1, 2, 3];
  let lastError;
  let previousLength = -1;
  for (const tier of tiers) {
    const filtered = filterPartsByTier(allParts, tier);
    if (filtered.length === 0)
      continue;
    if (filtered.length === previousLength)
      continue;
    previousLength = filtered.length;
    try {
      await sendFn(filtered);
      const sentTypes = new Set(filtered.map((p) => p.type));
      const allTypes = new Set(allParts.map((p) => p.type));
      const droppedTypes = [...allTypes].filter((t) => !sentTypes.has(t));
      return {
        success: true,
        tier,
        sentParts: filtered,
        droppedTypes
      };
    } catch (err) {
      lastError = err;
    }
  }
  return {
    success: false,
    error: lastError instanceof Error ? lastError.message : String(lastError)
  };
}

// auto-retry.ts
var SESSION_TTL_MS = 30 * 60 * 1000;
var POST_ABORT_DELAY_MS = 150;
function summarizeParts(parts) {
  if (!parts || parts.length === 0) {
    return { count: 0, types: [], textChars: 0, hasToolCall: false };
  }
  const typeSet = new Set;
  let textChars = 0;
  let hasToolCall = false;
  for (const part of parts) {
    typeSet.add(part.type);
    const textValue = part.text;
    if (part.type === "text" && typeof textValue === "string") {
      textChars += textValue.length;
    }
    if (part.type === "tool_call") {
      hasToolCall = true;
    }
  }
  return {
    count: parts.length,
    types: Array.from(typeSet),
    textChars,
    hasToolCall
  };
}
function createAutoRetryHelpers(deps) {
  const {
    ctx,
    config,
    sessionStates,
    sessionLastAccess,
    sessionRetryInFlight,
    sessionAwaitingFallbackResult,
    sessionFallbackTimeouts
  } = deps;
  const childFallbackBridges = new Map;
  const compactionDispatchOwners = new Map;
  let nextCompactionDispatchGeneration = 0;
  const sessionDispatchModels = deps.sessionDispatchModels ??= new Map;
  const ownsCompactionDispatch = (sessionID, owner) => Boolean(owner && compactionDispatchOwners.get(sessionID) === owner);
  const clearCompactionDispatch = (sessionID, owner) => {
    if (!ownsCompactionDispatch(sessionID, owner))
      return false;
    compactionDispatchOwners.delete(sessionID);
    deps.sessionCompactionInFlight.delete(sessionID);
    return true;
  };
  const registerChildFallbackBridge = (childID, parentID) => {
    if (childFallbackBridges.has(childID))
      return;
    childFallbackBridges.set(childID, {
      parentID,
      startedAt: Date.now(),
      claimedByToolHook: false
    });
    logInfo("Registered child fallback bridge", { childSession: childID, parentSession: parentID });
  };
  const clearChildFallbackBridge = (childID) => {
    const bridge = childFallbackBridges.get(childID);
    if (!bridge)
      return;
    childFallbackBridges.delete(childID);
    logInfo("Cleared child fallback bridge", { childSession: childID, parentSession: bridge.parentID });
  };
  const claimChildFallbackBridge = (childID, parentID) => {
    const bridge = childFallbackBridges.get(childID);
    if (!bridge || bridge.parentID !== parentID)
      return;
    bridge.claimedByToolHook = true;
    return bridge.startedAt;
  };
  const getParentSessionID = async (sessionID) => {
    const cached = deps.sessionParentID.get(sessionID);
    if (cached !== undefined)
      return cached;
    try {
      const sessionInfo = await ctx.client.session.get({ path: { id: sessionID } });
      const sessionData = sessionInfo?.data ?? sessionInfo;
      const parentID = typeof sessionData?.parentID === "string" && sessionData.parentID.length > 0 ? sessionData.parentID : null;
      deps.sessionParentID.set(sessionID, parentID);
      if (parentID) {
        logInfo("Detected child session", { sessionID, parentID });
      }
      return parentID;
    } catch {
      logError("Failed to look up parentID", { sessionID });
      return null;
    }
  };
  const abortSessionRequest = async (sessionID, source) => {
    try {
      await ctx.client.session.abort({ path: { id: sessionID } });
      deps.sessionSelfAbortTimestamp.set(sessionID, Date.now());
      logInfo(`Aborted in-flight session request (${source})`, { sessionID });
    } catch (error) {
      logError(`Failed to abort in-flight session request (${source})`, {
        sessionID,
        error: String(error)
      });
    }
  };
  const clearSessionFallbackTimeout = (sessionID) => {
    const timer = sessionFallbackTimeouts.get(sessionID);
    if (timer) {
      clearTimeout(timer);
      sessionFallbackTimeouts.delete(sessionID);
    }
  };
  const scheduleSessionFallbackTimeout = (sessionID, resolvedAgent, compactionOwner) => {
    clearSessionFallbackTimeout(sessionID);
    const timeoutMs = config.timeout_seconds * 1000;
    if (timeoutMs <= 0)
      return;
    const timer = setTimeout(async () => {
      if (sessionFallbackTimeouts.get(sessionID) !== timer)
        return;
      if (compactionOwner && !ownsCompactionDispatch(sessionID, compactionOwner))
        return;
      sessionFallbackTimeouts.delete(sessionID);
      if (compactionOwner && !deps.sessionCompactionInFlight.has(sessionID))
        return;
      if (deps.sessionFirstTokenReceived.get(sessionID)) {
        logInfo("Timeout fired but first token already received, skipping abort", {
          sessionID
        });
        return;
      }
      const state = sessionStates.get(sessionID);
      if (!state)
        return;
      if (sessionRetryInFlight.has(sessionID)) {
        logInfo("Timeout fired but retry already in flight, deferring", { sessionID });
        return;
      }
      if (compactionOwner) {
        clearCompactionDispatch(sessionID, compactionOwner);
        sessionAwaitingFallbackResult.delete(sessionID);
      } else {
        deps.sessionCompactionInFlight.delete(sessionID);
      }
      await abortSessionRequest(sessionID, "session.timeout");
      if (state.pendingFallbackModel) {
        state.pendingFallbackModel = undefined;
      }
      const fallbackModels = getFallbackModelsForSession(sessionID, resolvedAgent, deps.agentConfigs, deps.globalFallbackModels);
      if (fallbackModels.length === 0)
        return;
      if (sessionRetryInFlight.has(sessionID)) {
        logInfo("Timeout recovery yielded to a retry started during abort", { sessionID });
        return;
      }
      logInfo("Session fallback timeout reached", {
        sessionID,
        timeoutSeconds: config.timeout_seconds,
        currentModel: state.currentModel
      });
      const retryLockToken = acquireRetryLock(deps, sessionID);
      try {
        const plan = planFallback(sessionID, state, fallbackModels, config);
        if (plan.success) {
          await autoRetryWithFallback(sessionID, plan.newModel, resolvedAgent, "session.timeout", plan);
        }
      } finally {
        releaseRetryLock(deps, sessionID, retryLockToken);
      }
    }, timeoutMs);
    sessionFallbackTimeouts.set(sessionID, timer);
  };
  const autoRetryWithFallback = async (sessionID, newModel, resolvedAgent, source, plan) => {
    let deferredToOtherHandler = false;
    let compactionOwner;
    let normalDispatchModel;
    const retryLockToken = deps.sessionRetryLockTokens?.get(sessionID);
    const preCheckState = sessionStates.get(sessionID);
    if (plan) {
      if (preCheckState && preCheckState.currentModel !== plan.failedModel) {
        logInfo(`Skipping stale autoRetryWithFallback (${source}): state already at ${preCheckState.currentModel}, expected failed model ${plan.failedModel}`, {
          sessionID,
          staleModel: newModel,
          currentModel: preCheckState.currentModel
        });
        deferredToOtherHandler = true;
        return false;
      }
    } else if (preCheckState && preCheckState.currentModel !== newModel) {
      logInfo(`Skipping stale autoRetryWithFallback (${source}): state already at ${preCheckState.currentModel}, wanted ${newModel}`, {
        sessionID,
        staleModel: newModel,
        currentModel: preCheckState.currentModel
      });
      deferredToOtherHandler = true;
      return false;
    }
    const modelParts = newModel.split("/");
    if (modelParts.length < 2) {
      logInfo(`Invalid model format (missing provider prefix): ${newModel}`);
      const state = sessionStates.get(sessionID);
      if (state?.pendingFallbackModel) {
        state.pendingFallbackModel = undefined;
      }
      return false;
    }
    const fallbackModelObj = {
      providerID: modelParts[0],
      modelID: modelParts.slice(1).join("/")
    };
    if (resolvedAgent === "compaction") {
      if (deps.sessionCompactionInFlight.has(sessionID)) {
        logInfo(`Skipping duplicate compaction fallback (${source})`, { sessionID });
        deferredToOtherHandler = true;
        return false;
      }
      deps.sessionCompactionInFlight.add(sessionID);
      compactionOwner = { generation: ++nextCompactionDispatchGeneration, model: newModel };
      compactionDispatchOwners.set(sessionID, compactionOwner);
    }
    if (resolvedAgent === "compaction") {
      const compactionConfig = deps.agentConfigs?.compaction;
      const pinnedModel = typeof compactionConfig === "object" && compactionConfig !== null && typeof compactionConfig.model === "string" ? compactionConfig.model : undefined;
      const pinnedModelInCooldown = Boolean(pinnedModel && pinnedModel === newModel && preCheckState && isModelInCooldown(pinnedModel, preCheckState, config.cooldown_seconds));
      if (pinnedModel && (pinnedModel !== newModel || pinnedModelInCooldown)) {
        clearCompactionDispatch(sessionID, compactionOwner);
        if (sessionAwaitingFallbackResult.has(sessionID)) {
          logInfo(`Skipping pinned compaction fallback (${source}); another handler owns the result`, {
            sessionID,
            pinnedModel,
            requestedModel: newModel
          });
          deferredToOtherHandler = true;
          return false;
        }
        const reason = pinnedModelInCooldown ? `Pinned compaction model ${pinnedModel} is still in fallback cooldown.` : `Compaction is pinned to ${pinnedModel}, so it cannot switch to ${newModel}.`;
        const action = pinnedModelInCooldown ? "Wait for the fallback cooldown or unset agent.compaction.model." : "Unset agent.compaction.model to allow switching compaction models.";
        logError(reason, {
          sessionID,
          pinnedModel,
          requestedModel: newModel,
          action
        });
        if (preCheckState)
          preCheckState.pendingFallbackModel = undefined;
        sessionAwaitingFallbackResult.delete(sessionID);
        clearSessionFallbackTimeout(sessionID);
        if (config.notify_on_fallback) {
          await ctx.client.tui.showToast({
            body: {
              title: "Compaction Fallback Blocked",
              message: `${reason} ${action}`,
              variant: "warning",
              duration: 1e4
            }
          }).catch(() => {});
        }
        return false;
      }
    }
    const modelAlreadyStopped = source === "session.error" || source === "message.updated" || source === "task.empty-result";
    const callerAlreadyAborted = source === "session.timeout";
    const mayHaveRecentAbort = source === "session.idle.silent-failure";
    const childParentID = await getParentSessionID(sessionID);
    if (childParentID)
      registerChildFallbackBridge(sessionID, childParentID);
    if (modelAlreadyStopped) {
      logInfo(`Skipping abort \u2014 model already stopped (${source}), waiting for session to settle`, {
        sessionID,
        newModel
      });
      await new Promise((resolve) => setTimeout(() => resolve(), POST_ABORT_DELAY_MS));
    } else if (callerAlreadyAborted || mayHaveRecentAbort) {
      const selfAbortTs = deps.sessionSelfAbortTimestamp.get(sessionID);
      const msSinceAbort = selfAbortTs ? Date.now() - selfAbortTs : undefined;
      if (selfAbortTs && msSinceAbort !== undefined && msSinceAbort < POST_ABORT_DELAY_MS * 2) {
        logInfo(`Waiting for recent abort propagation (${source})`, {
          sessionID,
          msSinceAbort
        });
        const remainingMs = Math.max(0, POST_ABORT_DELAY_MS - msSinceAbort);
        if (remainingMs > 0) {
          await new Promise((resolve) => setTimeout(() => resolve(), remainingMs));
        }
      } else if (callerAlreadyAborted) {
        logInfo(`Caller already aborted (${source}), waiting for propagation`, {
          sessionID
        });
        await new Promise((resolve) => setTimeout(() => resolve(), POST_ABORT_DELAY_MS));
      }
    } else {
      await abortSessionRequest(sessionID, `pre-fallback.${source}`);
      await new Promise((resolve) => setTimeout(() => resolve(), POST_ABORT_DELAY_MS));
    }
    deps.sessionFirstTokenReceived.set(sessionID, false);
    let retryDispatched = false;
    try {
      if (resolvedAgent === "compaction") {
        const failedModel = plan?.failedModel;
        logInfo(`Compaction fallback: abort + summarize on fallback (${source})`, {
          sessionID,
          failedModel,
          newModel
        });
        if (failedModel && plan) {
          const currentState = sessionStates.get(sessionID);
          if (currentState) {
            if (!currentState.failedModels.has(failedModel)) {
              currentState.failedModels.set(failedModel, Date.now());
            }
          }
        }
        try {
          await abortSessionRequest(sessionID, "compaction-fallback");
        } catch {
          logError(`Failed to abort session for compaction fallback (${source})`, { sessionID });
        }
        await new Promise((resolve) => setTimeout(resolve, 500));
        try {
          const messagesResp = await ctx.client.session.messages({
            path: { id: sessionID },
            query: { directory: ctx.directory }
          });
          const msgs = messagesResp.data ?? [];
          const deleteIDs = [];
          for (let i = msgs.length - 1;i >= 0; i--) {
            const msg = msgs[i];
            const msgRole = msg.info?.role;
            const msgError = msg.info?.error;
            const msgID = msg.info?.id;
            const parts = msg.parts ?? [];
            const isCompactionMsg = parts.length > 0 && parts.every((p) => p.type === "compaction");
            if (!msgID)
              continue;
            if (msgRole === "assistant" && msgError) {
              deleteIDs.push(msgID);
              continue;
            }
            if (isCompactionMsg) {
              deleteIDs.push(msgID);
              break;
            }
          }
          const rawClient = ctx.client.session?._client;
          if (rawClient && deleteIDs.length > 0) {
            for (const msgID of deleteIDs) {
              logInfo(`Deleting compaction message (${source})`, {
                sessionID,
                messageID: msgID
              });
              try {
                await rawClient.delete({
                  url: "/session/{id}/message/{messageID}",
                  path: { id: sessionID, messageID: msgID }
                });
                logInfo(`Deleted compaction message (${source})`, {
                  sessionID,
                  messageID: msgID
                });
              } catch (delErr) {
                logError(`Failed to delete compaction message (${source})`, {
                  sessionID,
                  messageID: msgID,
                  error: String(delErr)
                });
              }
            }
          } else if (deleteIDs.length > 0) {
            logError(`Cannot access raw SDK client for message deletion (${source})`, {
              sessionID,
              messageCount: deleteIDs.length
            });
          }
        } catch (msgErr) {
          logError(`Failed during compaction message cleanup (${source})`, {
            sessionID,
            error: String(msgErr)
          });
        }
        await new Promise((resolve) => setTimeout(resolve, 200));
        if (!deps.sessionCompactionInFlight.has(sessionID)) {
          logInfo(`Compaction already completed while preparing fallback (${source})`, {
            sessionID,
            failedModel,
            newModel
          });
          return false;
        }
        const handleSummarizeFailure = async (summarizeErr) => {
          if (!ownsCompactionDispatch(sessionID, compactionOwner))
            return;
          logError(`session.summarize failed (${source})`, {
            sessionID,
            model: newModel,
            error: String(summarizeErr)
          });
          sessionAwaitingFallbackResult.delete(sessionID);
          if (plan) {
            const currentState = sessionStates.get(sessionID);
            if (currentState && commitFallback(currentState, plan)) {
              logInfo(`Committed compaction fallback state as last resort (${source})`, {
                sessionID,
                from: plan.failedModel,
                to: plan.newModel
              });
            }
          }
          clearCompactionDispatch(sessionID, compactionOwner);
          clearSessionFallbackTimeout(sessionID);
          sessionAwaitingFallbackResult.delete(sessionID);
          if (config.notify_on_fallback) {
            const fromName = (failedModel || "primary").split("/").pop();
            const toName = newModel.split("/").pop() || newModel;
            await ctx.client.tui.showToast({
              body: {
                title: "Compaction Failed",
                message: `${fromName} can't compact \u2014 try /compact after switching to ${toName}`,
                variant: "warning",
                duration: 1e4
              }
            }).catch(() => {});
          }
        };
        try {
          if (sessionAwaitingFallbackResult.has(sessionID)) {
            logInfo(`Skipping duplicate compaction summarize (${source})`, { sessionID });
            clearCompactionDispatch(sessionID, compactionOwner);
            deferredToOtherHandler = true;
            return false;
          }
          sessionAwaitingFallbackResult.add(sessionID);
          logInfo(`Dispatching session.summarize on fallback model (${source})`, {
            sessionID,
            providerID: fallbackModelObj.providerID,
            modelID: fallbackModelObj.modelID
          });
          if (plan) {
            const stateToCommit = sessionStates.get(sessionID);
            if (stateToCommit && !commitFallback(stateToCommit, plan)) {
              logInfo(`Compaction fallback state advanced before dispatch (${source})`, {
                sessionID,
                from: plan.failedModel,
                to: plan.newModel,
                currentModel: stateToCommit.currentModel
              });
              deferredToOtherHandler = true;
              await abortSessionRequest(sessionID, `stale-compaction.${source}`);
              return false;
            }
            if (stateToCommit) {
              logInfo(`Committed fallback at compaction dispatch (${source})`, {
                sessionID,
                from: plan.failedModel,
                to: plan.newModel,
                attemptCount: stateToCommit.attemptCount
              });
            }
          }
          const summarizePromise = ctx.client.session.summarize({
            path: { id: sessionID },
            body: {
              providerID: fallbackModelObj.providerID,
              modelID: fallbackModelObj.modelID
            },
            query: { directory: ctx.directory }
          });
          retryDispatched = true;
          if (ownsCompactionDispatch(sessionID, compactionOwner) && deps.sessionCompactionInFlight.has(sessionID) && sessionAwaitingFallbackResult.has(sessionID) && !deps.sessionFirstTokenReceived.get(sessionID)) {
            scheduleSessionFallbackTimeout(sessionID, resolvedAgent, compactionOwner);
          }
          releaseRetryLock(deps, sessionID, retryLockToken);
          const summarizeResult = await summarizePromise;
          if (!ownsCompactionDispatch(sessionID, compactionOwner)) {
            deferredToOtherHandler = true;
            return true;
          }
          if (!deps.sessionCompactionInFlight.has(sessionID) || !sessionAwaitingFallbackResult.has(sessionID)) {
            clearCompactionDispatch(sessionID, compactionOwner);
            deferredToOtherHandler = true;
            return true;
          }
          logInfo(`session.summarize response (${source})`, {
            sessionID,
            model: newModel,
            response: (JSON.stringify(summarizeResult) ?? "undefined").slice(0, 500)
          });
          const summarizeError = summarizeResult?.error;
          if (summarizeError) {
            throw new Error(`session.summarize returned an error: ${JSON.stringify(summarizeError).slice(0, 300)}`);
          }
          if (config.notify_on_fallback) {
            const fromName = (failedModel || "primary").split("/").pop();
            const toName = newModel.split("/").pop() || newModel;
            await ctx.client.tui.showToast({
              body: {
                title: "Compaction Fallback",
                message: `${fromName} failed \u2014 retrying compaction on ${toName}`,
                variant: "warning",
                duration: 5000
              }
            }).catch(() => {});
          }
          logInfo(`Compaction re-dispatched via summarize (${source})`, {
            sessionID,
            model: newModel
          });
          return true;
        } catch (summarizeErr) {
          await handleSummarizeFailure(summarizeErr);
          deferredToOtherHandler = true;
          return false;
        }
      }
      const messagesResp = await ctx.client.session.messages({
        path: { id: sessionID },
        query: { directory: ctx.directory }
      });
      const msgs = messagesResp.data;
      if (!msgs || msgs.length === 0) {
        logError(`No messages found in session for auto-retry (${source})`, { sessionID });
      }
      let lastUserPartsRaw;
      let lastNonAssistantPartsRaw;
      for (let i = (msgs?.length ?? 0) - 1;i >= 0; i--) {
        const m = msgs?.[i];
        const role = (m?.info?.role ?? m?.role ?? "").toLowerCase();
        const parts = m?.parts ?? m?.info?.parts;
        if (!parts || parts.length === 0)
          continue;
        const hasOnlyCompactionParts = parts.every((p) => p.type === "compaction");
        if (hasOnlyCompactionParts)
          continue;
        if (!lastNonAssistantPartsRaw && role !== "assistant") {
          lastNonAssistantPartsRaw = parts;
        }
        if (role === "user") {
          lastUserPartsRaw = parts;
          break;
        }
      }
      const replayPartsRaw = lastUserPartsRaw ?? lastNonAssistantPartsRaw;
      const replaySource = lastUserPartsRaw ? "last-user" : lastNonAssistantPartsRaw ? "last-non-assistant" : "none";
      if (replayPartsRaw && replayPartsRaw.length > 0) {
        const postCheckState = sessionStates.get(sessionID);
        const expectedCurrentModel = plan ? plan.failedModel : newModel;
        if (postCheckState && postCheckState.currentModel !== expectedCurrentModel) {
          logInfo(`Skipping stale autoRetryWithFallback (${source}): state already at ${postCheckState.currentModel}, expected failed model ${expectedCurrentModel}`, {
            sessionID,
            staleModel: newModel,
            currentModel: postCheckState.currentModel
          });
          deferredToOtherHandler = true;
          return false;
        }
        if (sessionAwaitingFallbackResult.has(sessionID)) {
          logInfo(`Skipping duplicate fallback dispatch \u2014 another handler already dispatched (${source})`, {
            sessionID,
            model: newModel
          });
          deferredToOtherHandler = true;
          return false;
        }
        sessionAwaitingFallbackResult.add(sessionID);
        logInfo(`Auto-retrying with fallback model (${source})`, {
          sessionID,
          model: newModel,
          agent: resolvedAgent,
          replaySource
        });
        const allParts = replayPartsRaw.filter((p) => typeof p.type === "string" && p.type !== "compaction");
        const continuationParts = [
          { type: "text", text: FALLBACK_CONTINUATION_PROMPT }
        ];
        logInfo(`Prepared replay payload (${source})`, {
          sessionID,
          model: newModel,
          agent: resolvedAgent,
          replaySource,
          payload: summarizeParts(continuationParts)
        });
        if (allParts.length > 0) {
          const sendFn = async (parts) => {
            logInfo(`Dispatching fallback replay (${source})`, {
              sessionID,
              model: newModel,
              agent: resolvedAgent,
              payload: summarizeParts(parts)
            });
            const prompt = childParentID ? ctx.client.session.prompt ?? ctx.client.session.promptAsync : ctx.client.session.promptAsync;
            normalDispatchModel = newModel;
            sessionDispatchModels?.set(sessionID, newModel);
            await prompt.call(ctx.client.session, {
              path: { id: sessionID },
              body: {
                ...resolvedAgent ? { agent: resolvedAgent } : {},
                model: fallbackModelObj,
                parts
              },
              query: { directory: ctx.directory }
            });
            logInfo(`Fallback replay accepted by host (${source})`, {
              sessionID,
              model: newModel,
              agent: resolvedAgent
            });
          };
          const replayResult = await replayWithDegradation(continuationParts, sendFn);
          if (replayResult.success) {
            let commitSucceeded = true;
            if (plan) {
              const stateToCommit = sessionStates.get(sessionID);
              if (stateToCommit) {
                const committed = commitFallback(stateToCommit, plan);
                if (committed) {
                  logInfo(`Committed fallback state after successful dispatch (${source})`, {
                    sessionID,
                    newModel: plan.newModel,
                    failedModel: plan.failedModel,
                    attemptCount: stateToCommit.attemptCount
                  });
                } else {
                  logInfo(`Fallback state already committed by another handler \u2014 aborting duplicate replay (${source})`, {
                    sessionID,
                    newModel: plan.newModel
                  });
                  commitSucceeded = false;
                  await abortSessionRequest(sessionID, `duplicate-replay.${source}`);
                }
              }
            }
            if (!commitSucceeded) {
              deferredToOtherHandler = true;
              return false;
            }
            if (sessionDispatchModels?.get(sessionID) === newModel) {
              sessionDispatchModels.delete(sessionID);
            }
            normalDispatchModel = undefined;
            if (deps.sessionFirstTokenReceived.get(sessionID)) {
              const state = sessionStates.get(sessionID);
              if (state?.pendingFallbackModel === newModel)
                state.pendingFallbackModel = undefined;
              sessionAwaitingFallbackResult.delete(sessionID);
              clearSessionFallbackTimeout(sessionID);
            } else if (sessionAwaitingFallbackResult.has(sessionID)) {
              scheduleSessionFallbackTimeout(sessionID, resolvedAgent);
            }
            retryDispatched = true;
            logInfo(`Fallback replay succeeded (${source})`, {
              sessionID,
              tier: replayResult.tier,
              sentPartsCount: replayResult.sentParts?.length,
              droppedTypes: replayResult.droppedTypes,
              replaySource
            });
            if (replayResult.droppedTypes && replayResult.droppedTypes.length > 0) {
              const droppedStr = replayResult.droppedTypes.join(", ");
              await ctx.client.tui.showToast({
                body: {
                  title: "Message Replay",
                  message: `Some message parts were dropped for compatibility: ${droppedStr}`,
                  variant: "warning",
                  duration: 5000
                }
              }).catch(() => {});
            }
          } else {
            logError(`All replay tiers failed (${source})`, {
              sessionID,
              error: replayResult.error
            });
          }
        }
      } else {
        logInfo(`No replayable non-assistant message found for auto-retry (${source})`, {
          sessionID,
          model: newModel,
          agent: resolvedAgent
        });
      }
    } catch (retryError) {
      const ownsCurrentDispatch = compactionOwner ? ownsCompactionDispatch(sessionID, compactionOwner) : !sessionDispatchModels?.has(sessionID) || sessionDispatchModels.get(sessionID) === normalDispatchModel;
      if (!ownsCurrentDispatch) {
        deferredToOtherHandler = true;
        return false;
      }
      logError(`Auto-retry failed (${source})`, {
        sessionID,
        error: String(retryError)
      });
      sessionAwaitingFallbackResult.delete(sessionID);
      if (compactionOwner)
        clearCompactionDispatch(sessionID, compactionOwner);
      clearSessionFallbackTimeout(sessionID);
    } finally {
      if (normalDispatchModel && sessionDispatchModels?.get(sessionID) === normalDispatchModel) {
        sessionDispatchModels.delete(sessionID);
      }
      const ownsCurrentDispatch = compactionOwner ? ownsCompactionDispatch(sessionID, compactionOwner) : !sessionDispatchModels?.has(sessionID) || sessionDispatchModels.get(sessionID) === normalDispatchModel;
      if (!retryDispatched && !deferredToOtherHandler && ownsCurrentDispatch) {
        sessionAwaitingFallbackResult.delete(sessionID);
        if (compactionOwner)
          clearCompactionDispatch(sessionID, compactionOwner);
        deps.sessionProviderRetrying?.delete(sessionID);
        clearSessionFallbackTimeout(sessionID);
        const state = sessionStates.get(sessionID);
        if (state?.pendingFallbackModel) {
          state.pendingFallbackModel = undefined;
        }
      }
      if (childParentID && !retryDispatched) {
        clearChildFallbackBridge(sessionID);
      }
    }
    return retryDispatched;
  };
  const retryEmptyChildTask = async (sessionID, eventAgent) => {
    const state = sessionStates.get(sessionID);
    if (!state || sessionRetryInFlight.has(sessionID) || sessionAwaitingFallbackResult.has(sessionID)) {
      return false;
    }
    const resolvedAgent = resolveAgentForSession(sessionID, eventAgent);
    const fallbackModels = getFallbackModelsForSession(sessionID, resolvedAgent, deps.agentConfigs, deps.globalFallbackModels);
    const plan = planFallback(sessionID, state, fallbackModels, config);
    if (!plan.success)
      return false;
    const retryLockToken = acquireRetryLock(deps, sessionID);
    try {
      return await autoRetryWithFallback(sessionID, plan.newModel, resolvedAgent, "task.empty-result", plan);
    } finally {
      releaseRetryLock(deps, sessionID, retryLockToken);
    }
  };
  const resolveAgentForSessionFromContext = async (sessionID, eventAgent) => {
    const resolved = resolveAgentForSession(sessionID, eventAgent);
    if (resolved)
      return resolved;
    try {
      const messagesResp = await ctx.client.session.messages({
        path: { id: sessionID },
        query: { directory: ctx.directory }
      });
      const msgs = messagesResp.data;
      if (!msgs || msgs.length === 0)
        return;
      for (let i = msgs.length - 1;i >= 0; i--) {
        const info = msgs[i]?.info;
        const infoAgent = typeof info?.agent === "string" ? info.agent : undefined;
        if (infoAgent && infoAgent.trim().length > 0) {
          return infoAgent.trim().toLowerCase();
        }
      }
    } catch {
      logError("Failed to resolve agent from messages", { sessionID });
    }
    try {
      const sessionInfo = await ctx.client.session.get({ path: { id: sessionID } });
      const sessionData = sessionInfo?.data ?? sessionInfo;
      const sdkAgent = typeof sessionData?.agent === "string" ? sessionData.agent : undefined;
      if (sdkAgent && sdkAgent.trim().length > 0) {
        const normalized = sdkAgent.trim().toLowerCase();
        logInfo("Resolved agent from session.get", { sessionID, agent: normalized });
        return normalized;
      }
    } catch {
      logError("Failed to resolve agent from session.get", { sessionID });
    }
    return;
  };
  const cleanupStaleSessions = () => {
    const now = Date.now();
    let cleanedCount = 0;
    for (const [sessionID, lastAccess] of sessionLastAccess.entries()) {
      if (now - lastAccess > SESSION_TTL_MS) {
        clearChildFallbackBridge(sessionID);
        sessionStates.delete(sessionID);
        sessionLastAccess.delete(sessionID);
        sessionRetryInFlight.delete(sessionID);
        deps.sessionRetryLockTokens?.delete(sessionID);
        sessionAwaitingFallbackResult.delete(sessionID);
        deps.sessionFirstTokenReceived.delete(sessionID);
        deps.sessionRequestMessages?.delete(sessionID);
        deps.sessionActiveRequests?.delete(sessionID);
        sessionDispatchModels.delete(sessionID);
        compactionDispatchOwners.delete(sessionID);
        deps.sessionSelfAbortTimestamp.delete(sessionID);
        deps.sessionParentID.delete(sessionID);
        deps.sessionIdleResolvers.delete(sessionID);
        deps.sessionLastMessageTime.delete(sessionID);
        deps.sessionCompactionInFlight.delete(sessionID);
        clearSessionFallbackTimeout(sessionID);
        cleanedCount++;
      }
    }
    if (cleanedCount > 0) {
      logInfo(`Cleaned up ${cleanedCount} stale session states`);
    }
  };
  return {
    getParentSessionID,
    abortSessionRequest,
    clearSessionFallbackTimeout,
    scheduleSessionFallbackTimeout,
    autoRetryWithFallback,
    retryEmptyChildTask,
    claimChildFallbackBridge,
    clearChildFallbackBridge,
    resolveAgentForSessionFromContext,
    cleanupStaleSessions
  };
}

// message-update-handler.ts
function hasVisibleAssistantResponse(extractAutoRetrySignalFn) {
  return async (ctx, sessionID, _info) => {
    try {
      const messagesResp = await ctx.client.session.messages({
        path: { id: sessionID },
        query: { directory: ctx.directory }
      });
      const msgs = messagesResp.data;
      if (!msgs || msgs.length === 0)
        return false;
      const lastAssistant = [...msgs].reverse().find((m) => m.info?.role === "assistant");
      if (!lastAssistant)
        return false;
      if (lastAssistant.info?.error)
        return false;
      const parts = lastAssistant.parts ?? lastAssistant.info?.parts;
      const hasToolCall = (parts ?? []).some((p) => p.type === "tool_call");
      const textFromParts = (parts ?? []).filter((p) => p.type === "text" && typeof p.text === "string").map((p) => p.text.trim()).filter((text) => text.length > 0).join(`
`);
      if (hasToolCall)
        return true;
      if (!textFromParts)
        return false;
      if (extractAutoRetrySignalFn({ message: textFromParts }))
        return false;
      return true;
    } catch {
      return false;
    }
  };
}
async function checkLastAssistantForErrorContent(ctx, sessionID) {
  try {
    const messagesResp = await ctx.client.session.messages({
      path: { id: sessionID },
      query: { directory: ctx.directory }
    });
    const msgs = messagesResp.data;
    if (!msgs || msgs.length === 0)
      return;
    const lastAssistant = [...msgs].reverse().find((m) => m.info?.role === "assistant");
    if (!lastAssistant)
      return;
    const parts = lastAssistant.parts ?? lastAssistant.info?.parts;
    const result = extractErrorContentFromParts(parts);
    if (result.hasError)
      return result.errorMessage;
    const textResult = detectErrorInTextParts(parts);
    if (textResult.hasError)
      return textResult.errorMessage;
    return;
  } catch {
    return;
  }
}
function createMessageUpdateHandler(deps, helpers) {
  const {
    ctx,
    config,
    sessionStates,
    sessionLastAccess,
    sessionRetryInFlight,
    sessionAwaitingFallbackResult
  } = deps;
  const checkVisibleResponse = hasVisibleAssistantResponse(extractAutoRetrySignal);
  const requestMessages = deps.sessionRequestMessages ??= new Map;
  const activeRequests = deps.sessionActiveRequests ??= new Map;
  return async (props) => {
    const info = props?.info;
    const sessionID = info?.sessionID;
    const retrySignalResult = extractAutoRetrySignal(info);
    const retrySignal = retrySignalResult?.signal;
    const timeoutEnabled = config.timeout_seconds > 0;
    const parts = props?.parts;
    const errorContentResult = containsErrorContent(parts);
    let error = info?.error ?? (retrySignal && timeoutEnabled ? { name: "ProviderRateLimitError", message: retrySignal } : undefined) ?? (errorContentResult.hasError ? {
      name: "MessageContentError",
      message: errorContentResult.errorMessage || "Message contains error content"
    } : undefined);
    const role = info?.role;
    const messageID = typeof info?.id === "string" ? info.id : undefined;
    const completed = !!info?.finish || !!info?.time?.completed;
    const hasConcreteContent = parts?.some((p) => p.type === "text" && typeof p.text === "string" && p.text.trim().length > 0 || p.type === "tool_call" || p.type === "tool");
    const model = info?.model ?? (typeof info?.providerID === "string" && typeof info?.modelID === "string" ? `${info.providerID}/${info.modelID}` : undefined);
    if (sessionID && role === "assistant") {
      if (messageID && !error && !completed && requestMessages.get(sessionID) !== messageID) {
        const state = sessionStates.get(sessionID);
        const expectedModel = state?.pendingFallbackModel ?? state?.currentModel;
        if (state && sessionAwaitingFallbackResult.has(sessionID) && !deps.sessionDispatchModels?.has(sessionID) && !deps.sessionCompactionInFlight.has(sessionID) && (deps.sessionFirstTokenReceived.get(sessionID) || model && model !== expectedModel && !state.failedModels.has(model))) {
          sessionAwaitingFallbackResult.delete(sessionID);
        }
        requestMessages.set(sessionID, messageID);
        activeRequests.set(sessionID, messageID);
        deps.sessionFirstTokenReceived.delete(sessionID);
        if (!sessionAwaitingFallbackResult.has(sessionID) && !deps.sessionDispatchModels?.has(sessionID)) {
          helpers.clearSessionFallbackTimeout(sessionID);
          const state = sessionStates.get(sessionID);
          if (state && model && !state.pendingFallbackModel)
            state.currentModel = model;
        }
      }
      if (completed && (!messageID || activeRequests.get(sessionID) === messageID)) {
        activeRequests.delete(sessionID);
        if (!sessionAwaitingFallbackResult.has(sessionID))
          helpers.clearSessionFallbackTimeout(sessionID);
      }
      deps.sessionLastMessageTime.set(sessionID, Date.now());
      logInfo("message.updated received", {
        sessionID,
        model,
        hasInfoError: !!info?.error,
        errorType: info?.error ? classifyErrorType(info.error) : undefined
      });
      if (deps.sessionProviderRetrying?.has(sessionID)) {
        if (hasConcreteContent) {
          deps.sessionProviderRetrying.delete(sessionID);
        } else if (retrySignal || !info?.error) {
          return;
        } else {
          deps.sessionProviderRetrying.delete(sessionID);
        }
      }
    }
    const requestAtStart = sessionID ? requestMessages.get(sessionID) : undefined;
    const isCurrentRequest = () => !sessionID || requestMessages.get(sessionID) === requestAtStart;
    if (sessionID && role === "assistant" && !error) {
      const errorContent = await checkLastAssistantForErrorContent(ctx, sessionID);
      if (!isCurrentRequest())
        return;
      if (errorContent) {
        logInfo("Detected error content in message parts", {
          sessionID,
          errorContent: errorContent.slice(0, 200)
        });
        error = { name: "ContentError", message: errorContent };
      }
    }
    if (sessionID && role === "assistant" && !error) {
      if (hasConcreteContent) {
        deps.sessionProviderRetrying?.delete(sessionID);
      }
      if (!sessionAwaitingFallbackResult.has(sessionID)) {
        const eventHasContent = hasConcreteContent;
        if (model && !sessionStates.has(sessionID)) {
          const state = createFallbackState(model);
          sessionStates.set(sessionID, state);
          sessionLastAccess.set(sessionID, Date.now());
        }
        if (eventHasContent) {
          deps.sessionFirstTokenReceived.set(sessionID, true);
        }
        const needsTimeout = model && !completed && config.timeout_seconds > 0 && !deps.sessionFirstTokenReceived.get(sessionID) && !deps.sessionFallbackTimeouts.has(sessionID) && !deps.sessionProviderRetrying?.has(sessionID) && !eventHasContent;
        if (needsTimeout) {
          const agent = info?.agent;
          helpers.resolveAgentForSessionFromContext(sessionID, agent).then((resolvedAgent) => {
            if (deps.sessionFirstTokenReceived.get(sessionID) || deps.sessionFallbackTimeouts.has(sessionID) || messageID && activeRequests.get(sessionID) !== messageID)
              return;
            const fallbackModels = getFallbackModelsForSession(sessionID, resolvedAgent, deps.agentConfigs, deps.globalFallbackModels);
            if (fallbackModels.length > 0) {
              helpers.scheduleSessionFallbackTimeout(sessionID, resolvedAgent);
              logInfo("Scheduled primary model TTFT timeout", {
                sessionID,
                model,
                timeoutSeconds: config.timeout_seconds
              });
            }
          }).catch(() => {});
        }
        return;
      }
      const hasVisible = await checkVisibleResponse(ctx, sessionID, info);
      if (!isCurrentRequest())
        return;
      if (!hasVisible) {
        const eventHasActivity = parts?.some((p) => p.type === "text" && typeof p.text === "string" && p.text.trim().length > 0 || p.type === "tool_call");
        if (eventHasActivity) {
          deps.sessionFirstTokenReceived.set(sessionID, true);
        }
        logError("Assistant update observed without visible final response; keeping fallback timeout", { sessionID, model, firstTokenReceived: deps.sessionFirstTokenReceived.get(sessionID) ?? false });
        return;
      }
      deps.sessionFirstTokenReceived.set(sessionID, true);
      sessionAwaitingFallbackResult.delete(sessionID);
      helpers.clearSessionFallbackTimeout(sessionID);
      const state = sessionStates.get(sessionID);
      if (state?.pendingFallbackModel) {
        state.pendingFallbackModel = undefined;
      }
      logInfo("Assistant response observed; cleared fallback timeout", {
        sessionID,
        model
      });
      return;
    }
    if (sessionID && role === "assistant" && error) {
      if (deps.sessionCompactionInFlight.has(sessionID)) {
        logInfo("Ignoring message.updated error during compaction in-flight", {
          sessionID,
          model,
          errorName: extractErrorName(error)
        });
        return;
      }
      const currentState = sessionStates.get(sessionID);
      const eventAgent = info?.agent?.trim().toLowerCase();
      const isCompactionError = eventAgent === "compaction";
      if (currentState && model && model !== currentState.currentModel) {
        if (isCompactionError) {
          logInfo("Compaction error on failed model \u2014 will retry on current fallback", {
            sessionID,
            failedModel: model,
            currentModel: currentState.currentModel,
            errorName: extractErrorName(error)
          });
        } else {
          const isAlreadyFailed = currentState.failedModels.has(model);
          const retryableStaleError = isRetryableError(error, config.retry_on_errors, config.retryable_error_patterns);
          const canResyncToErrorModel = retryableStaleError && !isAlreadyFailed && !currentState.pendingFallbackModel && !sessionAwaitingFallbackResult.has(sessionID);
          if (canResyncToErrorModel) {
            logInfo("Resyncing state to error model before fallback planning", {
              sessionID,
              previousModel: currentState.currentModel,
              errorModel: model,
              errorName: extractErrorName(error)
            });
            currentState.currentModel = model;
            sessionLastAccess.set(sessionID, Date.now());
          } else {
            logInfo("Ignoring stale error from previous model", {
              sessionID,
              staleModel: model,
              currentModel: currentState.currentModel,
              errorName: extractErrorName(error),
              isAlreadyFailed
            });
            return;
          }
        }
      }
      const SELF_ABORT_WINDOW_MS = 2000;
      const errorName = extractErrorName(error);
      const selfAbortTs = deps.sessionSelfAbortTimestamp.get(sessionID);
      if (errorName === "MessageAbortedError" && selfAbortTs && Date.now() - selfAbortTs < SELF_ABORT_WINDOW_MS) {
        logInfo("Ignoring self-inflicted MessageAbortedError (abort initiated by plugin)", {
          sessionID,
          model,
          msSinceAbort: Date.now() - selfAbortTs,
          awaitingFallback: sessionAwaitingFallbackResult.has(sessionID),
          retryInFlight: sessionRetryInFlight.has(sessionID)
        });
        return;
      }
      if (sessionRetryInFlight.has(sessionID) && !retrySignal) {
        logInfo("message.updated fallback skipped (retry in flight)", {
          sessionID
        });
        return;
      }
      if (retrySignal && sessionRetryInFlight.has(sessionID) && timeoutEnabled) {
        logError("Overriding in-flight retry due to provider auto-retry signal", { sessionID, model });
        await helpers.abortSessionRequest(sessionID, "message.updated.retry-signal");
        sessionRetryInFlight.delete(sessionID);
      }
      const retryLock = acquireRetryLock(deps, sessionID);
      sessionAwaitingFallbackResult.delete(sessionID);
      try {
        if (retrySignal && timeoutEnabled) {
          logInfo("Detected provider auto-retry signal", { sessionID, model });
        }
        if (!retrySignal) {
          helpers.clearSessionFallbackTimeout(sessionID);
        }
        logInfo("message.updated with assistant error", {
          sessionID,
          model,
          statusCode: extractStatusCode(error, config.retry_on_errors),
          errorName: extractErrorName(error),
          errorType: classifyErrorType(error)
        });
        let state = sessionStates.get(sessionID);
        const agent = info?.agent;
        const resolvedAgent = await helpers.resolveAgentForSessionFromContext(sessionID, agent);
        if (isCompactionError && state && state.currentModel !== model && state.currentModel !== state.originalModel) {
          logInfo("Compaction failed on stale model \u2014 re-dispatching on current fallback", {
            sessionID,
            failedModel: model,
            currentFallbackModel: state.currentModel
          });
          if (config.notify_on_fallback) {
            const fromName = (model || "primary").split("/").pop();
            const toName = state.currentModel.split("/").pop() || state.currentModel;
            deps.ctx.client.tui.showToast({
              body: {
                title: "Compaction Fallback",
                message: `${fromName} failed \u2014 retrying compaction on ${toName}`,
                variant: "warning",
                duration: 5000
              }
            }).catch(() => {});
          }
          await helpers.autoRetryWithFallback(sessionID, state.currentModel, "compaction", "message.updated.compaction-stale", undefined);
          return;
        }
        const fallbackModels = getFallbackModelsForSession(sessionID, resolvedAgent, deps.agentConfigs, deps.globalFallbackModels);
        if (fallbackModels.length === 0) {
          return;
        }
        if (state && state.pendingFallbackModel && model !== state.pendingFallbackModel) {
          logInfo("Skipping duplicate fallback trigger (already in progress for different model)", {
            sessionID,
            pendingFallbackModel: state.pendingFallbackModel,
            errorModel: model
          });
          return;
        }
        const isRetryable = isRetryableError(error, config.retry_on_errors, config.retryable_error_patterns);
        const inFallbackChain = state && state.currentModel !== state.originalModel;
        if (!isRetryable && !inFallbackChain) {
          logError("message.updated error not retryable and not in fallback chain, skipping", {
            sessionID,
            statusCode: extractStatusCode(error, config.retry_on_errors),
            errorName: extractErrorName(error),
            errorType: classifyErrorType(error)
          });
          return;
        }
        if (!isRetryable && inFallbackChain) {
          logInfo("message.updated non-retryable error but in fallback chain, continuing", {
            sessionID,
            currentModel: state?.currentModel,
            originalModel: state?.originalModel,
            errorName: extractErrorName(error)
          });
        }
        if (!state) {
          let initialModel = model;
          if (!initialModel) {
            const agentConfig = resolvedAgent && deps.agentConfigs ? deps.agentConfigs[resolvedAgent] : undefined;
            const agentModel = agentConfig?.model;
            if (agentModel) {
              logError("Derived model from agent config for message.updated", {
                sessionID,
                agent: resolvedAgent,
                model: agentModel
              });
              initialModel = agentModel;
            }
          }
          if (!initialModel) {
            logError("message.updated missing model info, cannot fallback", {
              sessionID,
              errorName: extractErrorName(error),
              errorType: classifyErrorType(error)
            });
            return;
          }
          state = createFallbackState(initialModel);
          sessionStates.set(sessionID, state);
          sessionLastAccess.set(sessionID, Date.now());
        } else {
          sessionLastAccess.set(sessionID, Date.now());
          if (state.pendingFallbackModel && retrySignal && timeoutEnabled) {
            logError("Clearing pending fallback due to provider auto-retry signal", {
              sessionID,
              pendingFallbackModel: state.pendingFallbackModel
            });
            state.pendingFallbackModel = undefined;
          }
        }
        const blockedProvider = isProviderWideRateLimitError(error) ? state.currentModel.split("/")[0] : undefined;
        const plan = planFallback(sessionID, state, fallbackModels, config, blockedProvider);
        if (plan.success) {
          if (config.notify_on_fallback) {
            deps.ctx.client.tui.showToast({
              body: {
                title: "Model Fallback",
                message: `Switching to ${plan.newModel?.split("/").pop() || plan.newModel} for next request`,
                variant: "warning",
                duration: 5000
              }
            }).catch(() => {});
          }
          await helpers.autoRetryWithFallback(sessionID, plan.newModel, resolvedAgent, "message.updated", plan);
        }
      } finally {
        releaseRetryLock(deps, sessionID, retryLock);
      }
    }
  };
}

// event-handler.ts
function createEventHandler(deps, helpers) {
  const {
    config,
    sessionStates,
    sessionLastAccess,
    sessionRetryInFlight,
    sessionAwaitingFallbackResult,
    sessionFallbackTimeouts
  } = deps;
  const checkVisibleResponse = hasVisibleAssistantResponse(extractAutoRetrySignal);
  const handleActivity = async (sessionID, activityModel) => {
    if (sessionAwaitingFallbackResult.has(sessionID)) {
      const state = sessionStates.get(sessionID);
      if (activityModel && state && state.failedModels.has(activityModel)) {
        logInfo("Ignoring activity from already-failed model", {
          sessionID,
          activityModel,
          currentModel: state.currentModel
        });
        return;
      }
      if (!activityModel) {
        const fallbackCommitted = state && state.currentModel !== state.originalModel && state.failedModels.size > 0;
        if (!fallbackCommitted) {
          logInfo("Ignoring unattributed activity during fallback (pre-commit, no model on event)", {
            sessionID,
            currentModel: state?.currentModel
          });
        } else {
          logInfo("Unattributed activity after fallback commit \u2014 resetting timeout only, NOT marking first token (no model on event)", {
            sessionID,
            currentModel: state?.currentModel
          });
        }
        const resolvedAgent = resolveAgentForSession(sessionID, undefined);
        helpers.scheduleSessionFallbackTimeout(sessionID, resolvedAgent);
        return;
      }
      if (!deps.sessionFirstTokenReceived.get(sessionID)) {
        deps.sessionFirstTokenReceived.set(sessionID, true);
      }
      const resolvedAgent = resolveAgentForSession(sessionID, undefined);
      helpers.scheduleSessionFallbackTimeout(sessionID, resolvedAgent);
      logInfo("Resetting fallback timeout due to activity", { sessionID, activityModel });
      return;
    }
    if (!deps.sessionFirstTokenReceived.get(sessionID)) {
      deps.sessionFirstTokenReceived.set(sessionID, true);
    }
    if (sessionAwaitingFallbackResult.size === 0) {
      return;
    }
    const cachedParentID = deps.sessionParentID.get(sessionID);
    const parentID = cachedParentID !== undefined ? cachedParentID : await helpers.getParentSessionID(sessionID);
    if (parentID && sessionAwaitingFallbackResult.has(parentID)) {
      const resolvedAgent = resolveAgentForSession(parentID, undefined);
      helpers.scheduleSessionFallbackTimeout(parentID, resolvedAgent);
      logInfo("Resetting parent fallback timeout due to child activity", {
        sessionID,
        parentID
      });
    }
  };
  const handleSessionCreated = (props) => {
    const sessionInfo = props?.info;
    const sessionID = sessionInfo?.id;
    if (!sessionID)
      return;
    const parentID = sessionInfo?.parentID;
    if (typeof parentID === "string" && parentID.length > 0) {
      deps.sessionParentID.set(sessionID, parentID);
    }
    logInfo("Session created, state will be created on-demand", { sessionID });
  };
  const handleSessionDeleted = (props) => {
    const sessionInfo = props?.info;
    const sessionID = sessionInfo?.id;
    if (sessionID) {
      logInfo("Cleaning up session state", { sessionID });
      helpers.clearChildFallbackBridge?.(sessionID);
      sessionStates.delete(sessionID);
      sessionLastAccess.delete(sessionID);
      sessionRetryInFlight.delete(sessionID);
      sessionAwaitingFallbackResult.delete(sessionID);
      deps.sessionFirstTokenReceived.delete(sessionID);
      deps.sessionRequestMessages?.delete(sessionID);
      deps.sessionActiveRequests?.delete(sessionID);
      deps.sessionDispatchModels?.delete(sessionID);
      deps.sessionRetryLockTokens?.delete(sessionID);
      deps.sessionSelfAbortTimestamp.delete(sessionID);
      deps.sessionParentID.delete(sessionID);
      deps.sessionCompactionInFlight.delete(sessionID);
      deps.sessionProviderRetrying?.delete(sessionID);
      deps.sessionIdleResolvers.delete(sessionID);
      deps.sessionLastMessageTime.delete(sessionID);
      helpers.clearSessionFallbackTimeout(sessionID);
    }
  };
  const handleSessionStop = async (props) => {
    const sessionID = props?.sessionID;
    if (!sessionID)
      return;
    deps.sessionActiveRequests?.delete(sessionID);
    deps.sessionDispatchModels?.delete(sessionID);
    deps.sessionRetryLockTokens?.delete(sessionID);
    helpers.clearSessionFallbackTimeout(sessionID);
    if (sessionRetryInFlight.has(sessionID) || sessionAwaitingFallbackResult.has(sessionID)) {
      await helpers.abortSessionRequest(sessionID, "session.stop");
    }
    sessionRetryInFlight.delete(sessionID);
    sessionAwaitingFallbackResult.delete(sessionID);
    deps.sessionCompactionInFlight.delete(sessionID);
    deps.sessionSelfAbortTimestamp.delete(sessionID);
    deps.sessionProviderRetrying?.delete(sessionID);
    const state = sessionStates.get(sessionID);
    if (state?.pendingFallbackModel) {
      state.pendingFallbackModel = undefined;
    }
    logInfo("Cleared fallback retry state on session.stop", { sessionID });
  };
  const handleSessionIdle = async (props) => {
    const sessionID = props?.sessionID;
    if (!sessionID)
      return;
    deps.sessionProviderRetrying?.delete(sessionID);
    deps.sessionActiveRequests?.delete(sessionID);
    const idleResolvers = deps.sessionIdleResolvers.get(sessionID);
    if (idleResolvers && idleResolvers.length > 0) {
      logInfo("session.idle resolving waiters", {
        sessionID,
        waiterCount: idleResolvers.length
      });
      for (const resolve of idleResolvers)
        resolve();
      deps.sessionIdleResolvers.delete(sessionID);
    }
    if (deps.sessionRetryLockTokens?.has(sessionID) && !sessionAwaitingFallbackResult.has(sessionID))
      return;
    if (sessionAwaitingFallbackResult.has(sessionID)) {
      if (deps.sessionCompactionInFlight.has(sessionID)) {
        logInfo("session.idle during compaction in-flight \u2014 not a silent failure, waiting for session.compacted", {
          sessionID
        });
        return;
      }
      const firstTokenReceived = deps.sessionFirstTokenReceived.get(sessionID);
      if (!firstTokenReceived) {
        const state = sessionStates.get(sessionID);
        if (state) {
          logInfo("session.idle detected silent model failure (no first token received)", {
            sessionID,
            currentModel: state.currentModel,
            attemptCount: state.attemptCount
          });
          if (sessionRetryInFlight.has(sessionID)) {
            logInfo("session.idle silent failure \u2014 retry already in flight, skipping", {
              sessionID
            });
            return;
          }
          const retryLock = acquireRetryLock(deps, sessionID);
          sessionAwaitingFallbackResult.delete(sessionID);
          helpers.clearSessionFallbackTimeout(sessionID);
          try {
            const resolvedAgent = await helpers.resolveAgentForSessionFromContext(sessionID, undefined);
            const fallbackModels = getFallbackModelsForSession(sessionID, resolvedAgent, deps.agentConfigs, deps.globalFallbackModels);
            if (fallbackModels.length === 0) {
              logInfo("session.idle silent failure \u2014 no fallback models configured", {
                sessionID
              });
              return;
            }
            const plan = planFallback(sessionID, state, fallbackModels, config);
            if (plan.success) {
              await helpers.autoRetryWithFallback(sessionID, plan.newModel, resolvedAgent, "session.idle.silent-failure", plan);
            } else {
              logInfo("session.idle silent failure \u2014 no more fallback models available", {
                sessionID,
                error: plan.error
              });
            }
          } finally {
            releaseRetryLock(deps, sessionID, retryLock);
          }
          return;
        }
      }
      logInfo("session.idle with first token received \u2014 fallback model completed", {
        sessionID
      });
      sessionAwaitingFallbackResult.delete(sessionID);
      helpers.clearSessionFallbackTimeout(sessionID);
      if (!deps.sessionRetryLockTokens?.has(sessionID))
        sessionRetryInFlight.delete(sessionID);
      return;
    }
    const hadTimeout = sessionFallbackTimeouts.has(sessionID);
    helpers.clearSessionFallbackTimeout(sessionID);
    sessionRetryInFlight.delete(sessionID);
    const state = sessionStates.get(sessionID);
    if (state?.pendingFallbackModel) {
      state.pendingFallbackModel = undefined;
    }
    if (hadTimeout) {
      logInfo("Cleared fallback timeout after session completion", { sessionID });
    }
  };
  const handleSessionStatus = async (props) => {
    const sessionID = props?.sessionID;
    const status = props?.status;
    if (!sessionID || !status)
      return;
    if (status.type === "idle") {
      await handleSessionIdle(props);
      return;
    }
    if (status.type !== "retry")
      return;
    if (deps.sessionCompactionInFlight.has(sessionID))
      return;
    if (deps.sessionFirstTokenReceived.get(sessionID) && !sessionAwaitingFallbackResult.has(sessionID) && await checkVisibleResponse(deps.ctx, sessionID, undefined)) {
      logInfo("Ignoring stale provider retry after response activity", { sessionID });
      return;
    }
    const parentID = await helpers.getParentSessionID(sessionID);
    if (parentID) {
      deps.sessionProviderRetrying?.add(sessionID);
      helpers.clearSessionFallbackTimeout(sessionID);
      logInfo("Leaving foreground child retry attached to parent task", {
        sessionID,
        parentID,
        nextRetryMs: status.next
      });
      if (config.notify_on_fallback) {
        await deps.ctx.client.tui.showToast({
          body: {
            title: "Subagent Provider Retrying",
            variant: "warning",
            duration: 8000,
            message: "Keeping the subagent attached; automatic fallback would cancel its parent task."
          }
        }).catch(() => {});
      }
      return;
    }
    if (sessionRetryInFlight.has(sessionID)) {
      logInfo("session.status skipped -- retry lock already held", { sessionID });
      return;
    }
    if (deps.sessionCompactionInFlight.has(sessionID))
      return;
    const retryLock = acquireRetryLock(deps, sessionID);
    try {
      const resolvedAgent = await helpers.resolveAgentForSessionFromContext(sessionID, undefined);
      if (deps.sessionCompactionInFlight.has(sessionID))
        return;
      if (deps.sessionFirstTokenReceived.get(sessionID) && !sessionAwaitingFallbackResult.has(sessionID) && await checkVisibleResponse(deps.ctx, sessionID, undefined)) {
        logInfo("Ignoring stale provider retry after response activity", { sessionID });
        return;
      }
      const fallbackModels = getFallbackModelsForSession(sessionID, resolvedAgent, deps.agentConfigs, deps.globalFallbackModels);
      logInfo("Provider retry detected", {
        sessionID,
        attempt: status.attempt,
        message: status.message,
        nextRetryMs: status.next,
        resolvedAgent,
        totalFallbackModels: fallbackModels.length,
        fallbackModels
      });
      if (fallbackModels.length === 0) {
        if (config.notify_on_fallback) {
          await deps.ctx.client.tui.showToast({
            body: {
              title: "Provider Retrying",
              variant: "info",
              duration: 3000,
              message: `${status.message || "retrying..."} (no fallback models configured)`
            }
          }).catch(() => {});
        }
        return;
      }
      const nextRetryMs = status.next;
      if (typeof nextRetryMs === "number" && nextRetryMs > 0) {
        const now = Date.now();
        const timeoutMs = config.timeout_seconds * 1000;
        if (nextRetryMs > now + timeoutMs) {
          logInfo("Provider retry is beyond timeout, triggering immediate fallback", {
            sessionID,
            nextRetryMs,
            now,
            timeoutMs,
            diffSeconds: Math.round((nextRetryMs - now) / 1000)
          });
          await triggerImmediateFallback(sessionID, resolvedAgent, fallbackModels, status);
          return;
        }
      }
      let state = sessionStates.get(sessionID);
      if (!state) {
        const agentConfig = resolvedAgent && deps.agentConfigs ? deps.agentConfigs[resolvedAgent] : undefined;
        const initialModel = agentConfig?.model ?? findFirstAgentModel();
        if (!initialModel) {
          logInfo("No model info for session.status fallback", { sessionID });
          return;
        }
        logInfo("Creating on-demand state for session.status", {
          sessionID,
          model: initialModel,
          agent: resolvedAgent
        });
        state = createFallbackState(initialModel);
        sessionStates.set(sessionID, state);
        sessionLastAccess.set(sessionID, Date.now());
      } else {
        sessionLastAccess.set(sessionID, Date.now());
      }
      sessionAwaitingFallbackResult.delete(sessionID);
      helpers.clearSessionFallbackTimeout(sessionID);
      const blockedProvider = isProviderWideRateLimitError(status.message) ? state.currentModel.split("/")[0] : undefined;
      const plan = planFallback(sessionID, state, fallbackModels, config, blockedProvider);
      if (plan.success) {
        if (config.notify_on_fallback) {
          const modelName = plan.newModel?.split("/").pop() || plan.newModel;
          deps.ctx.client.tui.showToast({
            body: {
              title: "Retry Detected -- Switching Model",
              variant: "warning",
              duration: 5000,
              message: `${status.message || "Provider retrying"} -> ${modelName} (attempt ${state.attemptCount + 1} of ${fallbackModels.length})`
            }
          }).catch(() => {});
        }
        await helpers.autoRetryWithFallback(sessionID, plan.newModel, resolvedAgent, "session.status", plan);
      } else if (!plan.success) {
        logError("session.status fallback failed", {
          sessionID,
          error: plan.error
        });
        if (plan.maxAttemptsReached && config.notify_on_fallback) {
          await deps.ctx.client.tui.showToast({
            body: {
              title: "All Fallbacks Exhausted",
              variant: "error",
              duration: 8000,
              message: `All ${fallbackModels.length} fallback models exhausted after ${state.attemptCount} attempts`
            }
          }).catch(() => {});
        }
      }
    } finally {
      releaseRetryLock(deps, sessionID, retryLock);
    }
  };
  const handleSessionError = async (props) => {
    const sessionID = props?.sessionID;
    const error = props?.error;
    const agent = props?.agent;
    const errorModel = props?.model;
    if (!sessionID) {
      logInfo("session.error without sessionID, skipping");
      return;
    }
    deps.sessionProviderRetrying?.delete(sessionID);
    if (deps.sessionCompactionInFlight.has(sessionID)) {
      logInfo("Ignoring session.error during compaction in-flight", {
        sessionID,
        errorName: extractErrorName(error)
      });
      return;
    }
    const SELF_ABORT_WINDOW_MS = 2000;
    const selfAbortTs = deps.sessionSelfAbortTimestamp.get(sessionID);
    const errorName = extractErrorName(error);
    if (errorName === "MessageAbortedError" && selfAbortTs && Date.now() - selfAbortTs < SELF_ABORT_WINDOW_MS) {
      logInfo("Ignoring self-inflicted MessageAbortedError in session.error", {
        sessionID,
        msSinceAbort: Date.now() - selfAbortTs,
        awaitingFallback: sessionAwaitingFallbackResult.has(sessionID),
        retryInFlight: sessionRetryInFlight.has(sessionID)
      });
      return;
    }
    const currentState = sessionStates.get(sessionID);
    if (currentState?.pendingFallbackModel) {
      logInfo("Ignoring session.error while fallback replay is pending", {
        sessionID,
        pendingFallbackModel: currentState.pendingFallbackModel,
        currentModel: currentState.currentModel,
        errorName: extractErrorName(error)
      });
      return;
    }
    if (currentState && errorModel && errorModel !== currentState.currentModel) {
      logInfo("Ignoring stale session.error from previous model", {
        sessionID,
        staleModel: errorModel,
        currentModel: currentState.currentModel,
        errorName: extractErrorName(error)
      });
      return;
    }
    if (currentState && errorModel && currentState.failedModels.has(errorModel)) {
      logInfo("Ignoring session.error from already-failed model", {
        sessionID,
        errorModel,
        currentModel: currentState.currentModel,
        errorName: extractErrorName(error)
      });
      return;
    }
    if (sessionAwaitingFallbackResult.has(sessionID)) {
      logInfo("Ignoring session.error while awaiting fallback result (likely stale abort)", {
        sessionID,
        currentModel: currentState?.currentModel,
        errorName: extractErrorName(error)
      });
      return;
    }
    if (sessionRetryInFlight.has(sessionID)) {
      logInfo("session.error skipped -- retry in flight (early lock)", {
        sessionID,
        retryInFlight: true
      });
      return;
    }
    const retryLock = acquireRetryLock(deps, sessionID);
    try {
      const resolvedAgent = await helpers.resolveAgentForSessionFromContext(sessionID, agent);
      if (deps.sessionCompactionInFlight.has(sessionID)) {
        logInfo("session.error skipping \u2014 compaction already being handled by message.updated", {
          sessionID,
          resolvedAgent,
          errorName: extractErrorName(error)
        });
        return;
      }
      helpers.clearSessionFallbackTimeout(sessionID);
      const stateAfterAwait = sessionStates.get(sessionID);
      if (stateAfterAwait?.pendingFallbackModel) {
        logInfo("Ignoring session.error \u2014 fallback replay became pending during agent resolution", {
          sessionID,
          pendingFallbackModel: stateAfterAwait.pendingFallbackModel,
          currentModel: stateAfterAwait.currentModel,
          errorName: extractErrorName(error)
        });
        return;
      }
      logInfo("session.error received", {
        sessionID,
        agent,
        resolvedAgent,
        statusCode: extractStatusCode(error, config.retry_on_errors),
        errorName: extractErrorName(error),
        errorType: classifyErrorType(error)
      });
      const isRetryable = isRetryableError(error, config.retry_on_errors, config.retryable_error_patterns);
      let state = sessionStates.get(sessionID);
      const fallbackModels = getFallbackModelsForSession(sessionID, resolvedAgent, deps.agentConfigs, deps.globalFallbackModels);
      if (fallbackModels.length === 0) {
        logInfo("No fallback models configured", { sessionID, agent });
        return;
      }
      const inFallbackChain = state && state.currentModel !== state.originalModel;
      if (!isRetryable && !inFallbackChain) {
        logInfo("Error not retryable and not in fallback chain, skipping", {
          sessionID,
          retryable: false,
          inFallbackChain: false,
          statusCode: extractStatusCode(error, config.retry_on_errors),
          errorName: extractErrorName(error),
          errorType: classifyErrorType(error)
        });
        return;
      }
      if (!isRetryable && inFallbackChain) {
        logInfo("Non-retryable error but in fallback chain, continuing to next fallback", {
          sessionID,
          retryable: false,
          inFallbackChain: true,
          currentModel: state?.currentModel,
          originalModel: state?.originalModel,
          errorName: extractErrorName(error)
        });
      }
      if (!state) {
        const currentModel = props?.model;
        if (currentModel) {
          state = createFallbackState(currentModel);
          sessionStates.set(sessionID, state);
          sessionLastAccess.set(sessionID, Date.now());
        } else if (!errorModel && sessionRetryInFlight.has(sessionID)) {
          logInfo("Deferring to message.updated handler (no state, no errorModel, retry in flight)", {
            sessionID,
            errorName: extractErrorName(error)
          });
          return;
        } else {
          const agentConfig = resolvedAgent && deps.agentConfigs ? deps.agentConfigs[resolvedAgent] : undefined;
          const agentModel = agentConfig?.model;
          if (agentModel) {
            logInfo("Derived model from agent config", {
              sessionID,
              agent: resolvedAgent,
              model: agentModel
            });
            state = createFallbackState(agentModel);
            sessionStates.set(sessionID, state);
            sessionLastAccess.set(sessionID, Date.now());
          } else {
            const firstModel = findFirstAgentModel();
            if (firstModel) {
              logInfo("Using first available agent model for state creation", {
                sessionID,
                model: firstModel
              });
              state = createFallbackState(firstModel);
              sessionStates.set(sessionID, state);
              sessionLastAccess.set(sessionID, Date.now());
            } else {
              logInfo("No model info available, cannot fallback", { sessionID });
              return;
            }
          }
        }
      } else {
        sessionLastAccess.set(sessionID, Date.now());
      }
      const blockedProvider = isProviderWideRateLimitError(error) ? state.currentModel.split("/")[0] : undefined;
      const plan = planFallback(sessionID, state, fallbackModels, config, blockedProvider);
      if (plan.success) {
        if (config.notify_on_fallback) {
          const modelName = plan.newModel?.split("/").pop() || plan.newModel;
          const attemptInfo = `attempt ${state.attemptCount + 1} of ${fallbackModels.length}`;
          deps.ctx.client.tui.showToast({
            body: {
              title: "Model Fallback",
              message: `Switching to ${modelName} (${attemptInfo})`,
              variant: "warning",
              duration: 5000
            }
          }).catch(() => {});
        }
        await helpers.autoRetryWithFallback(sessionID, plan.newModel, resolvedAgent, "session.error", plan);
      } else {
        logError("Fallback preparation failed", {
          sessionID,
          error: plan.error
        });
      }
    } finally {
      releaseRetryLock(deps, sessionID, retryLock);
    }
  };
  const handleSessionCompacted = (props) => {
    const sessionID = props?.sessionID;
    if (!sessionID)
      return;
    const hadAwaiting = sessionAwaitingFallbackResult.has(sessionID);
    const hadCompaction = deps.sessionCompactionInFlight.has(sessionID);
    sessionAwaitingFallbackResult.delete(sessionID);
    sessionRetryInFlight.delete(sessionID);
    deps.sessionFirstTokenReceived.delete(sessionID);
    deps.sessionCompactionInFlight.delete(sessionID);
    helpers.clearSessionFallbackTimeout(sessionID);
    if (hadAwaiting || hadCompaction) {
      logInfo("Compaction completed, clearing fallback state", { sessionID });
    }
  };
  function findFirstAgentModel() {
    if (!deps.agentConfigs)
      return;
    for (const agentName of Object.keys(deps.agentConfigs)) {
      const agentConfig = deps.agentConfigs[agentName];
      const model = agentConfig?.model;
      if (model)
        return model;
    }
    return;
  }
  async function triggerImmediateFallback(sessionID, resolvedAgent, fallbackModels, status) {
    let state = sessionStates.get(sessionID);
    if (!state) {
      const agentConfig = resolvedAgent && deps.agentConfigs ? deps.agentConfigs[resolvedAgent] : undefined;
      const initialModel = agentConfig?.model ?? findFirstAgentModel();
      if (!initialModel) {
        logError("Cannot trigger immediate fallback - no model info", { sessionID });
        return;
      }
      state = createFallbackState(initialModel);
      sessionStates.set(sessionID, state);
      sessionLastAccess.set(sessionID, Date.now());
    } else {
      sessionLastAccess.set(sessionID, Date.now());
    }
    sessionAwaitingFallbackResult.delete(sessionID);
    helpers.clearSessionFallbackTimeout(sessionID);
    const plan = planFallback(sessionID, state, fallbackModels, config);
    if (plan.success) {
      if (config.notify_on_fallback) {
        const modelName = plan.newModel?.split("/").pop() || plan.newModel;
        deps.ctx.client.tui.showToast({
          body: {
            title: "Provider Retry Too Slow - Switching Model",
            variant: "warning",
            duration: 5000,
            message: `${status.message || "Provider retrying"} -> ${modelName} (immediate fallback)`
          }
        }).catch(() => {});
      }
      await helpers.autoRetryWithFallback(sessionID, plan.newModel, resolvedAgent, "session.status.immediate", plan);
    } else if (!plan.success) {
      logError("Immediate fallback preparation failed", {
        sessionID,
        error: plan.error
      });
      if (plan.maxAttemptsReached && config.notify_on_fallback) {
        await deps.ctx.client.tui.showToast({
          body: {
            title: "All Fallbacks Exhausted",
            variant: "error",
            duration: 8000,
            message: `All ${fallbackModels.length} fallback models exhausted`
          }
        }).catch(() => {});
      }
    }
  }
  return {
    handleEvent: async ({ event }) => {
      if (!config.enabled)
        return;
      const props = event.properties;
      if (event.type === "session.created") {
        handleSessionCreated(props);
        return;
      }
      if (event.type === "session.deleted") {
        handleSessionDeleted(props);
        return;
      }
      if (event.type === "session.stop") {
        await handleSessionStop(props);
        return;
      }
      if (event.type === "session.idle") {
        await handleSessionIdle(props);
        return;
      }
      if (event.type === "session.error") {
        await handleSessionError(props);
        return;
      }
      if (event.type === "session.status") {
        await handleSessionStatus(props);
        return;
      }
      if (event.type === "session.compacted") {
        handleSessionCompacted(props);
        return;
      }
    },
    handleActivity
  };
}

// chat-message-handler.ts
function createChatMessageHandler(deps, helpers) {
  const {
    ctx,
    config,
    sessionStates,
    sessionLastAccess,
    sessionRetryInFlight,
    sessionAwaitingFallbackResult
  } = deps;
  return async (input, output) => {
    if (!config.enabled)
      return;
    const { sessionID } = input;
    let state = sessionStates.get(sessionID);
    if (!state) {
      return;
    }
    sessionLastAccess.set(sessionID, Date.now());
    const requestedModel = input.model ? `${input.model.providerID}/${input.model.modelID}` : undefined;
    if (requestedModel && requestedModel === state.currentModel && state.currentModel !== state.originalModel && !deps.sessionCompactionInFlight.has(sessionID) && !sessionRetryInFlight.has(sessionID) && !sessionAwaitingFallbackResult.has(sessionID)) {
      logInfo("Adopting current model as new primary (user confirmed manual selection)", {
        sessionID,
        model: requestedModel,
        previousOriginal: state.originalModel
      });
      state.originalModel = requestedModel;
      state.failedModels.clear();
      state.fallbackIndex = -1;
      state.attemptCount = 0;
      return;
    }
    if (state.currentModel !== state.originalModel) {
      if (!sessionRetryInFlight.has(sessionID) && !sessionAwaitingFallbackResult.has(sessionID)) {
        const recovered = recoverToOriginal(state, config.cooldown_seconds);
        if (recovered) {
          logInfo("Recovered to primary model", {
            sessionID,
            model: state.originalModel
          });
          if (config.notify_on_fallback) {
            const modelName = state.originalModel.split("/").pop() || state.originalModel;
            ctx.client.tui.showToast({
              body: {
                title: "Model Recovered",
                message: `Recovered to ${modelName}`,
                variant: "info",
                duration: 3000
              }
            }).catch(() => {});
          }
        }
      }
    }
    if (requestedModel && requestedModel !== state.currentModel) {
      if (state.pendingFallbackModel && state.pendingFallbackModel === requestedModel) {
        state.pendingFallbackModel = undefined;
        return;
      }
      if (sessionRetryInFlight.has(sessionID) || sessionAwaitingFallbackResult.has(sessionID)) {
        logInfo("Ignoring model mismatch during active fallback management", {
          sessionID,
          requestedModel,
          currentModel: state.currentModel,
          retryInFlight: sessionRetryInFlight.has(sessionID),
          awaitingResult: sessionAwaitingFallbackResult.has(sessionID)
        });
        return;
      }
      logError("Detected manual model change, resetting fallback state", {
        sessionID,
        from: state.currentModel,
        to: requestedModel
      });
      helpers.clearSessionFallbackTimeout(sessionID);
      sessionAwaitingFallbackResult.delete(sessionID);
      deps.sessionFirstTokenReceived.delete(sessionID);
      if (sessionRetryInFlight.has(sessionID)) {
        await helpers.abortSessionRequest(sessionID, "manual-model-change");
        sessionRetryInFlight.delete(sessionID);
      }
      state = createFallbackState(requestedModel);
      sessionStates.set(sessionID, state);
      return;
    }
    if (state.currentModel === state.originalModel)
      return;
    const activeModel = state.currentModel;
    logInfo("Applying fallback model override", {
      sessionID,
      from: input.model,
      to: activeModel
    });
    if (output.message && activeModel) {
      const parts = activeModel.split("/");
      if (parts.length >= 2) {
        output.message.model = {
          providerID: parts[0],
          modelID: parts.slice(1).join("/")
        };
      }
    }
    deps.sessionCompactionInFlight.delete(sessionID);
  };
}

// subagent-result-sync.ts
function isEmptyTaskResult(output) {
  return /<task_result>\s*<\/task_result>/.test(output);
}
function isCancelledTaskResult(output, metadata) {
  let metadataText = "";
  if (typeof metadata === "string") {
    metadataText = metadata;
  } else {
    try {
      metadataText = JSON.stringify(metadata ?? "") ?? "";
    } catch {}
  }
  return /task\s+cancelled/i.test(output) || /task\s+cancelled/i.test(metadataText);
}
var TASK_ID_REGEX = /["']?(?:task_id|sessionID|sessionId)["']?\s*:\s*["']?(ses_[a-zA-Z0-9]+)/;
function extractChildSessionID(output) {
  if (!output)
    return null;
  const match = output.match(TASK_ID_REGEX);
  return match ? match[1] : null;
}
function getSessionStatusType(sessionData) {
  const status = sessionData?.status;
  if (!status)
    return;
  if (typeof status === "string")
    return status;
  if (typeof status === "object" && status !== null && "type" in status) {
    return status.type;
  }
  return;
}
function waitForSessionIdle(deps, sessionID, inactivityMs, pollIntervalMs = 2000) {
  return new Promise((resolve) => {
    let settled = false;
    let pollTimer;
    let timeoutTimer;
    let lastSeenMessageTime = deps.sessionLastMessageTime.get(sessionID) ?? Date.now();
    const settle = (result) => {
      if (settled)
        return;
      settled = true;
      if (pollTimer)
        clearInterval(pollTimer);
      if (timeoutTimer)
        clearTimeout(timeoutTimer);
      const resolvers = deps.sessionIdleResolvers.get(sessionID);
      if (resolvers) {
        const idx = resolvers.indexOf(onIdleRef);
        if (idx >= 0)
          resolvers.splice(idx, 1);
        if (resolvers.length === 0)
          deps.sessionIdleResolvers.delete(sessionID);
      }
      resolve(result);
    };
    const onIdleRef = () => settle(true);
    let resolvers = deps.sessionIdleResolvers.get(sessionID);
    if (!resolvers) {
      resolvers = [];
      deps.sessionIdleResolvers.set(sessionID, resolvers);
    }
    resolvers.push(onIdleRef);
    const resetTimeout = () => {
      if (timeoutTimer)
        clearTimeout(timeoutTimer);
      timeoutTimer = setTimeout(() => settle(false), inactivityMs);
    };
    resetTimeout();
    const pollStatus = () => {
      if (settled)
        return;
      const currentMessageTime = deps.sessionLastMessageTime.get(sessionID);
      if (currentMessageTime && currentMessageTime > lastSeenMessageTime) {
        lastSeenMessageTime = currentMessageTime;
        resetTimeout();
      }
      deps.ctx.client.session.get({ path: { id: sessionID } }).then((sessionInfo) => {
        if (settled)
          return;
        const statusType = getSessionStatusType(sessionInfo?.data ?? sessionInfo);
        if (statusType === "idle") {
          logInfo(`[subagent-sync] Polling detected child ${sessionID} idle`);
          settle(true);
        }
      }).catch(() => {});
    };
    pollStatus();
    pollTimer = setInterval(pollStatus, pollIntervalMs);
  });
}
async function waitForChildFallbackResult(deps, childSessionID, options) {
  const maxWaitMs = options?.maxWaitMs ?? Math.min((deps.config.timeout_seconds || 120) * 1000, 120000);
  const pollIntervalMs = options?.pollIntervalMs ?? 500;
  const startTime = Date.now();
  logInfo(`[subagent-sync] Waiting for child ${childSessionID} fallback result (max ${maxWaitMs}ms idle timeout)`);
  while (deps.sessionRetryInFlight.has(childSessionID)) {
    if (Date.now() - startTime >= maxWaitMs) {
      logInfo(`[subagent-sync] Timed out waiting for child ${childSessionID} dispatch after ${maxWaitMs}ms`);
      return null;
    }
    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
  }
  const remainingMs = Math.max(1000, maxWaitMs - (Date.now() - startTime));
  const wentIdle = await waitForSessionIdle(deps, childSessionID, remainingMs, pollIntervalMs);
  if (!wentIdle) {
    logInfo(`[subagent-sync] Timed out waiting for child ${childSessionID} after ${Date.now() - startTime}ms`);
    return null;
  }
  const result = await extractAssistantResponse(deps, childSessionID, options?.notBeforeMs);
  if (result) {
    logInfo(`[subagent-sync] Got fallback result for ${childSessionID} (${Date.now() - startTime}ms)`);
    return result;
  }
  logInfo(`[subagent-sync] Child ${childSessionID} idle but no assistant response found`);
  return null;
}
async function extractAssistantResponse(deps, childSessionID, notBeforeMs) {
  try {
    const msgs = await deps.ctx.client.session.messages({
      path: { id: childSessionID },
      query: { directory: deps.ctx.directory }
    });
    if (!msgs.data || msgs.data.length === 0)
      return null;
    const lastAssistant = [...msgs.data].reverse().find((m) => {
      if (m.info?.role !== "assistant")
        return false;
      if (notBeforeMs === undefined)
        return true;
      const time = m.info.time;
      const createdAt = typeof time?.created === "number" ? time.created : typeof time?.created === "string" ? Date.parse(time.created) : NaN;
      return Number.isFinite(createdAt) && createdAt >= notBeforeMs;
    });
    if (!lastAssistant?.parts)
      return null;
    const textParts = lastAssistant.parts.filter((p) => p.type === "text" && p.text).map((p) => p.text);
    if (textParts.length === 0)
      return null;
    return textParts.join("");
  } catch (err) {
    logInfo(`[subagent-sync] Error reading child messages: ${err}`);
    return null;
  }
}

// foreground-task.ts
import { tool } from "@opencode-ai/plugin";
function createForegroundTask(deps) {
  const owners = new Map;
  const client = deps.ctx.client;
  const query = { directory: deps.ctx.directory };
  const definition = {
    description: "Launch a foreground subagent and wait for its result, transparently switching to configured fallback models on retryable failures. Background execution is not supported by this task replacement.",
    args: {
      description: tool.schema.string().describe("A short (3-5 words) description of the task"),
      prompt: tool.schema.string().describe("The task for the agent to perform"),
      subagent_type: tool.schema.string().describe("The type of specialized agent to use"),
      task_id: tool.schema.string().optional().describe("Existing child session to resume"),
      command: tool.schema.string().optional(),
      background: tool.schema.boolean().optional()
    },
    async execute(args, context) {
      if (args.background)
        throw new Error("foreground_task_fallback supports foreground tasks only");
      context.abort.throwIfAborted();
      await context.ask({
        permission: "task",
        patterns: [args.subagent_type],
        always: ["*"],
        metadata: { description: args.description, subagent_type: args.subagent_type }
      });
      const agents = await client.app.agents({ query, throwOnError: true });
      const agent = agents.data?.find((item) => item.name === args.subagent_type);
      if (!agent || agent.mode === "primary")
        throw new Error(`Unknown subagent: ${args.subagent_type}`);
      if (!Array.isArray(agent.permission))
        throw new Error("foreground_task_fallback requires a modern OpenCode permission ruleset");
      const parent = (await client.session.get({ path: { id: context.sessionID }, query, throwOnError: true })).data;
      let ancestor = parent;
      let depth = 0;
      while (ancestor.parentID) {
        depth++;
        ancestor = (await client.session.get({ path: { id: ancestor.parentID }, query, throwOnError: true })).data;
      }
      if (depth >= maxDepth)
        throw new Error(`Subagent depth limit reached (${maxDepth})`);
      const messages = (await client.session.messages({ path: { id: context.sessionID }, query, throwOnError: true })).data ?? [];
      const parentMessage = messages.find((message) => message.info.id === context.messageID)?.info;
      const inheritedModel = parentMessage?.role === "assistant" ? { providerID: parentMessage.providerID, modelID: parentMessage.modelID } : undefined;
      const initial = agent.model ?? inheritedModel;
      if (!initial)
        throw new Error("Cannot resolve subagent model");
      const permission = [
        ...(parent.permission ?? []).filter((rule) => rule.action === "deny" || rule.permission === "external_directory"),
        ...["task", "todowrite"].filter((name) => !agent.permission.some((rule) => rule.permission === name)).map((name) => ({ permission: name, pattern: "*", action: "deny" })),
        ...primaryTools.map((name) => ({ permission: name, pattern: "*", action: "deny" }))
      ];
      const child = args.task_id ? (await client.session.get({ path: { id: args.task_id }, query, throwOnError: true })).data : (await client.session.create({ query, body: { parentID: context.sessionID, title: `${args.description} (@${agent.name} subagent)`, permission }, throwOnError: true })).data;
      if (child.parentID !== context.sessionID)
        throw new Error("Cannot resume a task belonging to another parent");
      const previousMessageIDs = args.task_id ? new Set(((await client.session.messages({ path: { id: child.id }, query, throwOnError: true })).data ?? []).map((message) => message.info.id)) : new Set;
      if (owners.has(child.id))
        throw new Error("Task is already running");
      context.abort.throwIfAborted();
      const state = createFallbackState(`${initial.providerID}/${initial.modelID}`);
      const models = getFallbackModelsForSession(child.id, agent.name, deps.agentConfigs, deps.globalFallbackModels);
      logInfo("Foreground task fallback candidates captured", {
        childSession: child.id,
        parentSession: context.sessionID,
        agent: agent.name,
        currentModel: state.currentModel,
        fallbackModels: models
      });
      const blockedProviders = new Set;
      let taskPromptSaved = false;
      let lastError;
      try {
        while (true) {
          context.abort.throwIfAborted();
          const [providerID, ...modelParts] = state.currentModel.split("/");
          const model = { providerID, modelID: modelParts.join("/") };
          const metadata = { parentSessionId: context.sessionID, sessionId: child.id, model };
          context.metadata({ title: args.description, metadata });
          let wake;
          const signal = new Promise((resolve) => {
            wake = resolve;
          });
          let timer;
          const activity = () => {
            clearTimeout(timer);
            timer = undefined;
          };
          owners.set(child.id, {
            activity,
            signal(value) {
              if (value.type === "cancel" || nextPlan(value.error).success)
                wake(value);
              else
                activity();
            }
          });
          const onAbort = () => wake({ type: "cancel" });
          context.abort.addEventListener("abort", onAbort, { once: true });
          if (context.abort.aborted)
            onAbort();
          if (deps.config.timeout_seconds > 0) {
            timer = setTimeout(() => wake({ type: "retry", error: new Error("Subagent time-to-first-token timeout") }), deps.config.timeout_seconds * 1000);
          }
          const request = client.session.prompt({
            path: { id: child.id },
            query,
            throwOnError: true,
            body: { agent: agent.name, model, parts: [{ type: "text", text: taskPromptSaved ? FALLBACK_CONTINUATION_PROMPT : args.prompt }] }
          }).then((result) => ({ type: "result", result }), (error) => ({ type: "error", error }));
          try {
            const outcome = await Promise.race([request, signal]);
            if (outcome.type === "retry" || outcome.type === "cancel") {
              await client.session.abort({ path: { id: child.id }, query, throwOnError: true });
              await request;
              context.abort.throwIfAborted();
              if (outcome.type === "cancel")
                throw new Error("Task cancelled");
              lastError = outcome.error;
            } else if (outcome.type === "error") {
              lastError = outcome.error;
              if (!isRetryableError(lastError, deps.config.retry_on_errors, deps.config.retryable_error_patterns))
                throw lastError;
            } else {
              context.abort.throwIfAborted();
              const result = outcome.result.data;
              if (!result)
                throw new Error("Subagent prompt returned no result");
              lastError = result.info.error;
              if (!lastError) {
                const text = result.parts.filter((part) => part.type === "text").map((part) => part.text).join(`
`);
                if (!text.trim())
                  throw new Error("Subagent returned no text result");
                return {
                  title: args.description,
                  output: `task_id: ${child.id}

<task_result>
${text}
</task_result>`,
                  metadata
                };
              }
              if (!isRetryableError(lastError, deps.config.retry_on_errors, deps.config.retryable_error_patterns))
                throw new Error(JSON.stringify(lastError));
            }
          } finally {
            activity();
            context.abort.removeEventListener("abort", onAbort);
          }
          const plan = nextPlan(lastError);
          if (!plan.success)
            throw new Error(`Subagent failed (task_id: ${child.id}): ${plan.error}; ${JSON.stringify(lastError)}`);
          if (!taskPromptSaved) {
            const history = await client.session.messages({ path: { id: child.id }, query, throwOnError: true });
            taskPromptSaved = history.data?.some((message) => !previousMessageIDs.has(message.info.id) && message.info.role === "user" && message.parts?.some((part) => part.type === "text" && part.text === args.prompt)) ?? false;
          }
          commitFallback(state, plan);
          logInfo("Foreground task switching model while retaining parent promise", { childSession: child.id, model: state.currentModel });
          if (deps.config.notify_on_fallback) {
            await deps.ctx.client.tui.showToast({ body: { title: "Subagent Fallback", message: `${plan.failedModel} \u2192 ${plan.newModel}`, variant: "warning", duration: 5000 } }).catch(() => {});
          }
        }
      } finally {
        owners.delete(child.id);
      }
      function nextPlan(error) {
        const providerWide = isProviderWideRateLimitError(error);
        if (providerWide)
          blockedProviders.add(state.currentModel.split("/")[0]);
        logInfo("Foreground task rate-limit scope and candidate filtering", {
          childSession: child.id,
          agent: args.subagent_type,
          currentModel: state.currentModel,
          errorMessage: getErrorMessage(error),
          providerWide,
          blockedProviders: [...blockedProviders],
          fallbackModels: models
        });
        return planFallback(child.id, state, models.map((model) => blockedProviders.has(model.split("/")[0]) ? state.currentModel : model), deps.config);
      }
    }
  };
  let maxDepth = 1;
  let primaryTools = [];
  return {
    tool: definition,
    owns: (id) => owners.has(id),
    configure(config) {
      if (typeof config.subagent_depth === "number")
        maxDepth = config.subagent_depth;
      const experimental = config.experimental;
      primaryTools = experimental?.primary_tools ?? [];
    },
    handleEvent(event) {
      const props = event.properties;
      const id = props?.sessionID ?? props?.info?.sessionID ?? props?.part?.sessionID ?? (event.type.startsWith("session.") ? props?.info?.id : undefined);
      const owner = owners.get(id);
      if (!owner)
        return false;
      if (event.type === "session.status" && props?.status?.type === "retry")
        owner.signal({ type: "retry", error: { message: props.status.message } });
      if (event.type === "session.stop" || event.type === "session.deleted")
        owner.signal({ type: "cancel" });
      if (event.type === "message.part.delta" && props?.delta?.length)
        owner.activity();
      if (event.type === "message.part.updated" && (props?.part?.type === "tool" || props?.part?.text?.length))
        owner.activity();
      return true;
    }
  };
}

// v1.ts
import { randomUUID } from "crypto";
var buildID = "2026-10-26T13:49:17.190Z";
function summarizeAgentFallbackConfig(value) {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    return {};
  return Object.fromEntries(Object.entries(value).flatMap(([name, config]) => {
    if (typeof config !== "object" || config === null || Array.isArray(config))
      return [];
    const agent = config;
    if (agent.model === undefined && agent.fallback_models === undefined)
      return [];
    return [[name, { model: agent.model, fallback_models: agent.fallback_models }]];
  }));
}
async function OpenCodeFallbackPlugin(ctx, configOverrides) {
  const instanceContext = {
    instanceID: randomUUID(),
    buildID,
    moduleURL: import.meta.url,
    directory: ctx.directory,
    processStartedAt: new Date(Date.now() - process.uptime() * 1000).toISOString()
  };
  let configRevision = 0;
  let agentConfigs;
  let fileConfig = loadPluginConfig(ctx.directory);
  let mergedConfig;
  const globalFallbackModels = normalizeFallbackModelsField(configOverrides?.fallback_models ?? fileConfig.fallback_models);
  const getConfig = () => {
    mergedConfig ??= {
      enabled: configOverrides?.enabled ?? fileConfig?.enabled ?? DEFAULT_CONFIG.enabled,
      retry_on_errors: configOverrides?.retry_on_errors ?? fileConfig?.retry_on_errors ?? DEFAULT_CONFIG.retry_on_errors,
      retryable_error_patterns: configOverrides?.retryable_error_patterns ?? fileConfig?.retryable_error_patterns ?? DEFAULT_CONFIG.retryable_error_patterns,
      max_fallback_attempts: configOverrides?.max_fallback_attempts ?? fileConfig?.max_fallback_attempts ?? DEFAULT_CONFIG.max_fallback_attempts,
      cooldown_seconds: configOverrides?.cooldown_seconds ?? fileConfig?.cooldown_seconds ?? DEFAULT_CONFIG.cooldown_seconds,
      timeout_seconds: configOverrides?.timeout_seconds ?? fileConfig?.timeout_seconds ?? DEFAULT_CONFIG.timeout_seconds,
      notify_on_fallback: configOverrides?.notify_on_fallback ?? fileConfig?.notify_on_fallback ?? DEFAULT_CONFIG.notify_on_fallback,
      fallback_models: configOverrides?.fallback_models ?? fileConfig?.fallback_models ?? DEFAULT_CONFIG.fallback_models,
      foreground_task_fallback: configOverrides?.foreground_task_fallback ?? fileConfig?.foreground_task_fallback ?? DEFAULT_CONFIG.foreground_task_fallback
    };
    return mergedConfig;
  };
  const deps = {
    ctx,
    get config() {
      return getConfig();
    },
    get agentConfigs() {
      return agentConfigs;
    },
    globalFallbackModels,
    sessionStates: new Map,
    sessionLastAccess: new Map,
    sessionRetryInFlight: new Set,
    sessionAwaitingFallbackResult: new Set,
    sessionFallbackTimeouts: new Map,
    sessionFirstTokenReceived: new Map,
    sessionSelfAbortTimestamp: new Map,
    sessionParentID: new Map,
    sessionIdleResolvers: new Map,
    sessionLastMessageTime: new Map,
    sessionCompactionInFlight: new Set,
    sessionProviderRetrying: new Set
  };
  const helpers = createAutoRetryHelpers(deps);
  const { handleEvent: baseEventHandler, handleActivity } = createEventHandler(deps, helpers);
  const messageUpdateHandler = createMessageUpdateHandler(deps, helpers);
  const chatMessageHandler = createChatMessageHandler(deps, helpers);
  const foregroundTask = createForegroundTask(deps);
  const cleanupInterval = setInterval(helpers.cleanupStaleSessions, 5 * 60 * 1000);
  cleanupInterval.unref();
  withLogContext(instanceContext, () => logInfo(`Plugin initialized (${globalFallbackModels.length} global fallback model(s) configured)`, {
    foregroundTaskFallback: deps.config.foreground_task_fallback
  }));
  const hooks = {
    name: PLUGIN_NAME,
    ...deps.config.enabled && deps.config.foreground_task_fallback ? { tool: { task: foregroundTask.tool } } : {},
    config: (opencodeConfig) => {
      configRevision++;
      foregroundTask.configure(opencodeConfig);
      const agentsValue = opencodeConfig.agents;
      const agentValue = opencodeConfig.agent;
      agentConfigs = mergeAgentConfigs(agentsValue, agentValue);
      logInfo(`Plugin initialized with ${agentConfigs ? Object.keys(agentConfigs).length : 0} agents`, {
        configRevision,
        pluralFallbackConfig: summarizeAgentFallbackConfig(agentsValue),
        singularFallbackConfig: summarizeAgentFallbackConfig(agentValue),
        mergedFallbackConfig: summarizeAgentFallbackConfig(agentConfigs)
      });
    },
    event: async ({
      event
    }) => {
      const props = event.properties;
      if (event.type === "session.created" || event.type === "session.error" || event.type === "session.status" && props?.status?.type === "retry") {
        logInfo("Fallback event routed to plugin instance", {
          eventType: event.type,
          sessionID: props?.sessionID ?? props?.info?.id,
          configRevision,
          ownedForegroundTask: foregroundTask.owns(props?.sessionID ?? props?.info?.id),
          agentFallbackConfig: summarizeAgentFallbackConfig(agentConfigs)
        });
      }
      if (foregroundTask.handleEvent(event))
        return;
      if (event.type === "message.updated") {
        if (!deps.config.enabled)
          return;
        const props = event.properties;
        await messageUpdateHandler(props);
        return;
      }
      if (hasConcreteAssistantActivity(event.type, event.properties)) {
        const props = event.properties;
        const info = props?.info;
        const sessionID = props?.sessionID ?? info?.sessionID ?? info?.id;
        const activityModel = info?.model ?? (typeof info?.providerID === "string" && typeof info?.modelID === "string" ? `${info.providerID}/${info.modelID}` : undefined) ?? props?.model;
        if (sessionID) {
          await handleActivity(sessionID, activityModel);
        }
      }
      await baseEventHandler({ event });
    },
    "tool.execute.after": async (input, output) => {
      if (!output)
        return;
      const isEmptyResult = isEmptyTaskResult(output.output);
      const isCancelledResult = isCancelledTaskResult(output.output, output.metadata);
      if (input.tool !== "task" || !isEmptyResult && !isCancelledResult) {
        return;
      }
      const childSessionID = extractChildSessionID(output.output) ?? extractChildSessionID(serializeMetadata(output.metadata));
      if (!childSessionID) {
        logInfo("Task result needs fallback sync but no child session ID found", {
          sessionID: input.sessionID,
          outputPreview: output.output?.substring(0, 200)
        });
        return;
      }
      const bridgeStartedAt = helpers.claimChildFallbackBridge(childSessionID, input.sessionID);
      if (isCancelledResult && bridgeStartedAt === undefined && !deps.sessionAwaitingFallbackResult.has(childSessionID) && !deps.sessionRetryInFlight.has(childSessionID) && !deps.sessionStates.get(childSessionID)?.pendingFallbackModel) {
        logInfo("Ignoring cancelled task without active fallback", {
          parentSession: input.sessionID,
          childSession: childSessionID
        });
        return;
      }
      if (isEmptyResult && !deps.sessionRetryInFlight.has(childSessionID)) {
        logInfo("Empty task result has no active fallback; dispatching child fallback", {
          parentSession: input.sessionID,
          childSession: childSessionID
        });
        const taskAgent = typeof input.args?.subagent_type === "string" ? input.args.subagent_type : undefined;
        await helpers.retryEmptyChildTask(childSessionID, taskAgent);
      }
      logInfo("Detected task result requiring fallback sync, waiting for child fallback", {
        parentSession: input.sessionID,
        childSession: childSessionID
      });
      const maxWaitMs = Math.min((deps.config.timeout_seconds || 120) * 1000, 120000);
      try {
        const replacementText = await waitForChildFallbackResult(deps, childSessionID, {
          maxWaitMs,
          pollIntervalMs: 500,
          notBeforeMs: bridgeStartedAt
        });
        if (replacementText) {
          output.output = replacementText;
          logInfo("Replaced task result with fallback response", {
            parentSession: input.sessionID,
            childSession: childSessionID,
            responseLength: replacementText.length
          });
        } else {
          logInfo("No fallback response available, preserving original output", {
            parentSession: input.sessionID,
            childSession: childSessionID
          });
        }
      } finally {
        helpers.clearChildFallbackBridge(childSessionID);
      }
    },
    "chat.message": async (input, output) => {
      if (foregroundTask.owns(input.sessionID))
        return;
      await chatMessageHandler(input, output);
    }
  };
  const tracedHooks = Object.fromEntries(Object.entries(hooks).map(([name, hook]) => [
    name,
    typeof hook === "function" ? (...args) => withLogContext(instanceContext, () => hook(...args)) : hook
  ]));
  if (tracedHooks.tool) {
    const execute = foregroundTask.tool.execute;
    tracedHooks.tool = { task: {
      ...foregroundTask.tool,
      execute: (...args) => withLogContext(instanceContext, () => execute(...args))
    } };
  }
  return tracedHooks;
}
function hasConcreteAssistantActivity(type, properties) {
  const props = properties;
  if (type === "message.part.delta") {
    const field = props?.field;
    return (field === "text" || field === "reasoning") && typeof props?.delta === "string" && props.delta.length > 0;
  }
  if (type !== "message.part.updated")
    return false;
  const part = props?.part;
  if (!part)
    return false;
  if (part.type === "tool")
    return true;
  return (part.type === "text" || part.type === "reasoning") && typeof part.text === "string" && part.text.length > 0;
}
function serializeMetadata(metadata) {
  if (typeof metadata === "string")
    return metadata;
  try {
    return JSON.stringify(metadata) ?? "";
  } catch {
    return "";
  }
}
export {
  OpenCodeFallbackPlugin as default,
  hasConcreteAssistantActivity
};
