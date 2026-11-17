// @bun
import {
  fallbackRpc2,
  isFallbackNotificationData2,
  describeFallback2
} from "./chunk-dg3f0bg1.js";
import {
  logInfo,
  logError
} from "./chunk-0wvy6bk9.js";

// tui.ts
var TUI_DEFAULTS = { notify_toast: true, notify_desktop: false, notify_sound: false };
var STATE_KEY = "opencode-fallback:notifications";
var SEEN_LIMIT = 100;
function readTuiOptions(options) {
  const read = (key) => {
    const value = options[key];
    return typeof value === "boolean" ? value : TUI_DEFAULTS[key];
  };
  return { notify_toast: read("notify_toast"), notify_desktop: read("notify_desktop"), notify_sound: read("notify_sound") };
}
function present(context, options, data) {
  const { title, message } = describeFallback2(data);
  if (options.notify_toast) {
    try {
      context.ui.toast.show({ title, message, variant: "warning", sessionID: data.sessionID });
      logInfo("V2 fallback TUI toast submitted", { sessionID: data.sessionID, title });
    } catch (error) {
      logError("V2 fallback TUI toast failed", { sessionID: data.sessionID, error: String(error) });
    }
  }
  if (options.notify_desktop || options.notify_sound) {
    const alert = async () => {
      await context.attention.notify({
        title,
        message,
        notification: options.notify_desktop ? { when: "blurred" } : false,
        sound: options.notify_sound ? { name: "default", when: "blurred" } : false
      });
    };
    alert().catch(() => {});
  }
}
var plugin = {
  id: "opencode-fallback",
  setup(context) {
    const options = readTuiOptions(context.options);
    logInfo("V2 fallback TUI initialized", { directory: (context.location ?? context.data.location.default()).directory, ...options });
    const [state, mutate] = context.storage.memory(STATE_KEY, { initial: { seen: [] } });
    const recent = new Map;
    const forget = (sessionID) => {
      const item = recent.get(sessionID);
      if (!item)
        return;
      clearTimeout(item.expiry);
      clearTimeout(item.restore);
      recent.delete(sessionID);
    };
    const stopFailures = context.data.on("session.execution.failed", (event) => {
      const item = recent.get(event.data.sessionID);
      if (!item)
        return;
      if (!Number.isFinite(event.created) || event.created > item.created) {
        forget(event.data.sessionID);
        return;
      }
      clearTimeout(item.restore);
      item.restore = setTimeout(() => {
        if (recent.get(event.data.sessionID) !== item)
          return;
        present(context, { ...options, notify_desktop: false, notify_sound: false }, item.data);
        logInfo("V2 fallback TUI toast restored after older failure", { sessionID: event.data.sessionID, failureCreated: event.created, notificationCreated: item.created });
      }, 0);
    });
    const unsubscribe = context.client.rpc(fallbackRpc2).events.on("notification", async (event) => {
      try {
        const data = event.data;
        if (!isFallbackNotificationData2(data))
          return;
        const here = context.location ?? context.data.location.default();
        const directory = event.location?.directory ?? data.directory;
        if (directory !== here.directory) {
          logInfo("V2 fallback TUI notification ignored for another directory", { sessionID: data.sessionID, directory, currentDirectory: here.directory });
          return;
        }
        const workspaceID = event.location?.workspaceID ?? data.workspaceID;
        if (typeof workspaceID === "string" && here.workspaceID && workspaceID !== here.workspaceID) {
          logInfo("V2 fallback TUI notification ignored for another workspace", { sessionID: data.sessionID, workspaceID, currentWorkspaceID: here.workspaceID });
          return;
        }
        if (state.seen.includes(data.id))
          return;
        mutate((draft) => {
          draft.seen.push(data.id);
          if (draft.seen.length > SEEN_LIMIT)
            draft.seen.splice(0, draft.seen.length - SEEN_LIMIT);
        });
        present(context, options, data);
        if (options.notify_toast && Number.isFinite(event.created)) {
          forget(data.sessionID);
          const item = { data, created: event.created, expiry: setTimeout(() => forget(data.sessionID), 5000) };
          recent.set(data.sessionID, item);
        }
        logInfo("V2 fallback TUI notification handled", { sessionID: data.sessionID, from: data.from, to: data.to, ...options });
      } catch (error) {
        logError("V2 fallback TUI notification failed", { error: String(error) });
      }
    });
    return () => {
      unsubscribe();
      stopFailures();
      for (const sessionID of recent.keys())
        forget(sessionID);
    };
  }
};
var tui_default = plugin;
export {
  tui_default as default,
  readTuiOptions
};
