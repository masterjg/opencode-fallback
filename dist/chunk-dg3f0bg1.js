// @bun
// fallback-rpc.ts
var FALLBACK_RPC_ID2 = "opencode-fallback";
var fallbackRpc2 = {
  id: FALLBACK_RPC_ID2,
  methods: {},
  events: {
    notification: {
      schema: {
        type: "object",
        required: ["id", "directory", "sessionID", "from", "to", "kind"],
        properties: {
          id: { type: "string" },
          directory: { type: "string" },
          workspaceID: { type: "string" },
          sessionID: { type: "string" },
          from: { type: "string" },
          to: { type: "string" },
          kind: { type: "string", enum: ["primary", "compaction"] }
        }
      }
    }
  }
};
function isFallbackNotificationData2(value) {
  if (!value || typeof value !== "object")
    return false;
  const data = value;
  const strings = ["id", "directory", "sessionID", "from", "to"];
  return strings.every((key) => typeof data[key] === "string" && data[key] !== "") && (data.workspaceID === undefined || typeof data.workspaceID === "string") && (data.kind === "primary" || data.kind === "compaction");
}
function describeFallback2(data) {
  return {
    title: data.kind === "compaction" ? "Compaction fallback" : "Model fallback",
    message: `${data.from} -> ${data.to}`
  };
}

export { FALLBACK_RPC_ID2, fallbackRpc2, isFallbackNotificationData2, describeFallback2 };
