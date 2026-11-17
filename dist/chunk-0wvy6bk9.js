// @bun
// constants.ts
var PLUGIN_NAME = "opencode-fallback";
var DEFAULT_CONFIG = {
  enabled: true,
  retry_on_errors: [401, 402, 429, 500, 502, 503, 504],
  retryable_error_patterns: [],
  max_fallback_attempts: 10,
  cooldown_seconds: 60,
  timeout_seconds: 30,
  notify_on_fallback: true,
  fallback_models: [],
  foreground_task_fallback: true
};
var RETRYABLE_ERROR_PATTERNS = [
  /rate.?limit/i,
  /too.?many.?requests/i,
  /quota.?exceeded/i,
  /quota.?protection/i,
  /key.?limit.?exceeded/i,
  /usage\s+limit\s+has\s+been\s+reached/i,
  /service.?unavailable/i,
  /overloaded/i,
  /temporarily.?unavailable/i,
  /try.?again/i,
  /credit.*balance.*too.*low/i,
  /insufficient.?(?:credits?|funds?|balance)/i,
  /(?:^|\s)429(?:\s|$)/,
  /(?:^|\s)503(?:\s|$)/,
  /(?:^|\s)529(?:\s|$)/
];

// logger.ts
import { appendFileSync, mkdirSync } from "fs";
import { join } from "path";
import { homedir } from "os";
import { AsyncLocalStorage } from "async_hooks";
var logContext = new AsyncLocalStorage;
function withLogContext(context, callback) {
  return logContext.run(context, callback);
}
var LOG_FILE = join(homedir(), ".config", "opencode", "opencode-fallback.log");
try {
  mkdirSync(join(homedir(), ".config", "opencode"), { recursive: true });
} catch {}
function writeToFile(level, message, context) {
  const timestamp = new Date().toISOString();
  const combinedContext = { ...logContext.getStore(), ...context };
  const contextStr = Object.keys(combinedContext).length ? ` ${JSON.stringify(combinedContext)}` : "";
  const logLine = `[${timestamp}] [${level}] [${PLUGIN_NAME}] [pid=${process.pid}] ${message}${contextStr}
`;
  try {
    appendFileSync(LOG_FILE, logLine);
  } catch {}
}
var DEBUG_MODE = false;
function logInfo(message, context) {
  if (DEBUG_MODE) {
    const contextStr = context ? ` ${JSON.stringify(context)}` : "";
    console.log(`[${PLUGIN_NAME}] ${message}${contextStr}`);
  }
  writeToFile("INFO", message, context);
}
function logError(message, context) {
  if (DEBUG_MODE) {
    const contextStr = context ? ` ${JSON.stringify(context)}` : "";
    console.error(`[${PLUGIN_NAME}] ${message}${contextStr}`);
  }
  writeToFile("ERROR", message, context);
}

export { PLUGIN_NAME, DEFAULT_CONFIG, RETRYABLE_ERROR_PATTERNS, withLogContext, logInfo, logError };
