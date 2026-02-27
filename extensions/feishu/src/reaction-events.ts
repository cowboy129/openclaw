import type { ClawdbotConfig, RuntimeEnv } from "openclaw/plugin-sdk";
import {
  resolveOpenProviderRuntimeGroupPolicy,
  resolveDefaultGroupPolicy,
  warnMissingProviderGroupPolicyFallbackOnce,
} from "openclaw/plugin-sdk";
import { resolveFeishuAccount } from "./accounts.js";
import { tryRecordMessagePersistent } from "./dedup.js";
import {
  isFeishuGroupAllowed,
  resolveFeishuAllowlistMatch,
  resolveFeishuGroupConfig,
} from "./policy.js";
import { createFeishuReplyDispatcher } from "./reply-dispatcher.js";
import { getFeishuRuntime } from "./runtime.js";
import { getMessageFeishu, type FeishuMessageInfo } from "./send.js";
import type { FeishuConfig } from "./types.js";

/**
 * Feishu reaction event payload shape.
 * @see https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/message-reaction/events/created
 */
export type FeishuReactionEvent = {
  message_id: string;
  reaction_type: {
    emoji_type: string;
  };
  operator_type: string;
  user_id: {
    open_id?: string;
    user_id?: string;
    union_id?: string;
  };
  action_time?: string;
};

// In-memory cache for message info lookups (avoids repeated API calls for the same message).
const MESSAGE_CACHE_TTL_MS = 5 * 60 * 1000;
const messageInfoCache = new Map<string, { info: FeishuMessageInfo | null; expireAt: number }>();

async function getCachedMessageInfo(params: {
  cfg: ClawdbotConfig;
  messageId: string;
  accountId?: string;
}): Promise<FeishuMessageInfo | null> {
  const cacheKey = `${params.accountId ?? "default"}:${params.messageId}`;
  const now = Date.now();
  const cached = messageInfoCache.get(cacheKey);
  if (cached && cached.expireAt > now) {
    return cached.info;
  }

  const info = await getMessageFeishu(params);
  messageInfoCache.set(cacheKey, { info, expireAt: now + MESSAGE_CACHE_TTL_MS });
  return info;
}

export async function handleFeishuReactionEvent(params: {
  cfg: ClawdbotConfig;
  event: FeishuReactionEvent;
  action: "added" | "removed";
  botOpenId?: string;
  runtime?: RuntimeEnv;
  accountId?: string;
}): Promise<void> {
  const { cfg, event, action, botOpenId, runtime, accountId } = params;

  const account = resolveFeishuAccount({ cfg, accountId });
  const feishuCfg = account.config;

  const log = runtime?.log ?? console.log;
  const error = runtime?.error ?? console.error;

  const userOpenId = event.user_id.open_id ?? "";
  const emoji = event.reaction_type.emoji_type;
  const messageId = event.message_id;

  // 1. Filter bot's own reactions (typing indicator, etc.)
  if (event.operator_type === "app" || (botOpenId && userOpenId === botOpenId)) {
    return;
  }

  // 2. Dedup — include action_time so add→remove→add cycles are not collapsed.
  const actionTime = event.action_time ?? "";
  const dedupKey = `reaction:${action}:${messageId}:${userOpenId}:${emoji}:${actionTime}`;
  if (!(await tryRecordMessagePersistent(dedupKey, account.accountId, log))) {
    return;
  }

  // 3. Fetch message context to obtain chatId (reaction events don't include it).
  let msgInfo: FeishuMessageInfo | null;
  try {
    msgInfo = await getCachedMessageInfo({ cfg, messageId, accountId: account.accountId });
  } catch (err) {
    error(`feishu[${account.accountId}]: failed to fetch message for reaction: ${String(err)}`);
    return;
  }
  if (!msgInfo) {
    log(`feishu[${account.accountId}]: cannot resolve message ${messageId} for reaction, skipping`);
    return;
  }

  const chatId = msgInfo.chatId;
  if (!chatId) {
    log(`feishu[${account.accountId}]: message ${messageId} has no chatId, skipping reaction`);
    return;
  }

  // 4. Group policy check — only for group chats, DMs skip (same as bot.ts message handling).
  const isGroup = msgInfo.chatType === "group";
  if (isGroup) {
    const defaultGroupPolicy = resolveDefaultGroupPolicy(cfg);
    const { groupPolicy, providerMissingFallbackApplied } = resolveOpenProviderRuntimeGroupPolicy({
      providerConfigPresent: cfg.channels?.feishu !== undefined,
      groupPolicy: feishuCfg?.groupPolicy,
      defaultGroupPolicy,
    });
    warnMissingProviderGroupPolicyFallbackOnce({
      providerMissingFallbackApplied,
      providerKey: "feishu",
      accountId: account.accountId,
      log,
    });

    const groupAllowFrom = feishuCfg?.groupAllowFrom ?? [];
    const groupAllowed = isFeishuGroupAllowed({
      groupPolicy,
      allowFrom: groupAllowFrom,
      senderId: chatId,
      senderName: undefined,
    });
    if (!groupAllowed) {
      log(`feishu[${account.accountId}]: reaction in disallowed group ${chatId}, skipping`);
      return;
    }

    // Additional sender-level allowlist check if group has specific config.
    const groupConfig = resolveFeishuGroupConfig({ cfg: feishuCfg, groupId: chatId });
    const senderAllowFrom = groupConfig?.allowFrom ?? [];
    if (senderAllowFrom.length > 0) {
      const senderAllowed = resolveFeishuAllowlistMatch({
        allowFrom: senderAllowFrom,
        senderId: userOpenId,
        senderIds: [event.user_id.user_id],
      }).allowed;
      if (!senderAllowed) {
        log(
          `feishu[${account.accountId}]: reaction sender ${userOpenId} not in group sender allowlist`,
        );
        return;
      }
    }
  } else {
    // DM policy check — block unauthorized senders (mirrors bot.ts DM handling).
    const dmPolicy = feishuCfg?.dmPolicy ?? "pairing";
    if (dmPolicy !== "open") {
      const configAllowFrom = feishuCfg?.allowFrom ?? [];
      const dmAllowed = resolveFeishuAllowlistMatch({
        allowFrom: configAllowFrom,
        senderId: userOpenId,
        senderIds: [event.user_id.user_id],
      }).allowed;
      if (!dmAllowed) {
        log(
          `feishu[${account.accountId}]: reaction from unauthorized DM sender ${userOpenId}, skipping`,
        );
        return;
      }
    }
  }

  // 5. Resolve agent route.
  const core = getFeishuRuntime();
  const peerId = isGroup ? chatId : userOpenId;
  const route = core.channel.routing.resolveAgentRoute({
    cfg,
    channel: "feishu",
    accountId: account.accountId,
    peer: { kind: isGroup ? "group" : "direct", id: peerId },
  });

  // 6. Enqueue system event and dispatch to agent so it can decide whether to reply.
  const preview = (msgInfo.content ?? "").replace(/\s+/g, " ").slice(0, 80);
  const eventText = `Feishu[${account.accountId}] reaction ${action}: :${emoji}: by ${userOpenId} on msg ${messageId} ("${preview}")`;

  core.system.enqueueSystemEvent(eventText, {
    sessionKey: route.sessionKey,
    contextKey: `feishu:reaction:${action}:${messageId}:${userOpenId}:${emoji}`,
  });

  // Build a lightweight inbound body for the agent.
  const body = `[System: reaction ${action} :${emoji}: by ${userOpenId} on message ${messageId} ("${preview}")]`;
  const feishuTo = `feishu:${account.accountId}:${chatId}`;

  const ctxPayload = core.channel.reply.finalizeInboundContext({
    Body: body,
    BodyForAgent: body,
    RawBody: body,
    CommandBody: body,
    From: `feishu:${userOpenId}`,
    To: feishuTo,
    SessionKey: route.sessionKey,
    AccountId: route.accountId,
    ChatType: isGroup ? "group" : "direct",
    GroupSubject: isGroup ? chatId : undefined,
    SenderName: userOpenId,
    SenderId: userOpenId,
    Provider: "feishu" as const,
    Surface: "feishu" as const,
    MessageSid: `reaction:${messageId}:${actionTime}`,
    Timestamp: Date.now(),
    WasMentioned: false,
    CommandAuthorized: false,
    OriginatingChannel: "feishu" as const,
    OriginatingTo: feishuTo,
  });

  const { dispatcher, replyOptions, markDispatchIdle } = createFeishuReplyDispatcher({
    cfg,
    agentId: route.agentId,
    runtime: runtime as RuntimeEnv,
    chatId,
    replyToMessageId: messageId,
    accountId: account.accountId,
  });

  log(
    `feishu[${account.accountId}]: reaction ${action} :${emoji}: by ${userOpenId} on ${messageId}, dispatching to agent`,
  );

  await core.channel.reply.withReplyDispatcher({
    dispatcher,
    onSettled: () => {
      markDispatchIdle();
    },
    run: () =>
      core.channel.reply.dispatchReplyFromConfig({
        ctx: ctxPayload,
        cfg,
        dispatcher,
        replyOptions,
      }),
  });
}

/** Exported for testing only. */
export function _resetMessageInfoCacheForTest() {
  messageInfoCache.clear();
}
