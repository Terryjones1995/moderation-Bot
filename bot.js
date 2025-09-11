#!/usr/bin/env node
/**
 * Consolidated & Updated bot.js
 * - Interaction dedupe (processedInteractions)
 * - Safer modal/button locking (ticketModalLocks)
 * - Promise-lock + DB-unique-catch for ticket creation (ticketCreationLocks)
 * - Replace deprecated ephemeral: true with flags: 64
 * - Defensive try/catch around showModal / deferReply / reply/editReply to avoid "Unknown interaction" / "already acknowledged"
 * - Improved betting detection and moderation prompt to reduce false positives (e.g. "alright bet")
 * - NEW: secondary OpenAI verification step for flagged messages to decide whether to record a strike
 * - Updates per owner requests:
 *   * default account age -> 30 days
 *   * admin user whitelist (won't be flagged/deleted)
 *   * allow "nigga" usage (do not block); keep blocking "nigger"
 *   * quarantined users may type but all messages go through OpenAI; do not auto-perm-mute them unless verification indicates severe offense
 *   * lenient verification fallback: if verification unavailable, do NOT create a strike
 *   * improved NBA/picks detection
 *   * matchmaking channel enforcement for specific channel (only certain prefixes allowed)
 *   * advertising channel bypass (allow adverts in specific channel)
 *   * strike system made less harsh via explicit hate allowlist and reduced immediate strikes in allowed channels
 */

import 'dotenv/config';
import fs from 'fs';
import os from 'os';
import path from 'path';
import {
  Client,
  GatewayIntentBits,
  ChannelType,
  PermissionsBitField,
  EmbedBuilder,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  AttachmentBuilder,
} from 'discord.js';
import OpenAI from 'openai';
import { PrismaClient } from '@prisma/client';

//
// ---------- CONFIG ----------
//

// Default account age limit is now 30 days (was 7)
const ACCOUNT_AGE_LIMIT_DAYS = parseInt(process.env.ACCOUNT_AGE_LIMIT_DAYS || '30', 10);

// Admin user ID (exempt from moderation). You can override with env ADMIN_USER_ID.
const ADMIN_USER_ID = process.env.ADMIN_USER_ID || '637758147330572349';

const MOD_CATEGORY_NAME = process.env.MOD_CATEGORY_NAME || 'Moderation';
const LOG_CHANNEL_NAME = process.env.LOG_CHANNEL_NAME || 'moderation-log';
const QUARANTINED_ROLE_NAME = process.env.QUARANTINED_ROLE_NAME || 'Quarantined';
const MODERATOR_ROLE_NAME = process.env.MODERATOR_ROLE_NAME || 'Moderators';
const MUTED_ROLE_NAME = process.env.MUTED_ROLE_NAME || 'Muted';

const SAMPLE_RATE = parseFloat(process.env.SAMPLE_RATE || '0.02'); // default 2%
const MAX_OPENAI_PER_MIN = parseInt(process.env.MAX_OPENAI_PER_MIN || '600', 10);
const CONCURRENCY = parseInt(process.env.CONCURRENCY || '3', 10);
const CACHE_TTL_MS = parseInt(process.env.CACHE_TTL_MS || String(10 * 60 * 1000), 10);
const MODEL = process.env.MODEL || 'gpt-3.5-turbo';

const DEFAULT_PANEL_CHANNEL_NAME = process.env.DEFAULT_PANEL_CHANNEL_NAME || 'ticket-panel';
const DEFAULT_TICKET_CATEGORY_NAME = process.env.DEFAULT_TICKET_CATEGORY_NAME || 'Tickets';

const GREETING_MESSAGE = `Hello {member}, welcome to the server!

Please note that new Discord accounts (less than ${ACCOUNT_AGE_LIMIT_DAYS} days old) cannot participate in the public channels yet. Please browse around and come back once your account is at least ${ACCOUNT_AGE_LIMIT_DAYS} days old.`;

const ACCOUNT_TOO_NEW_MESSAGE = `Hi {member}, your Discord account is under ${ACCOUNT_AGE_LIMIT_DAYS} days old and cannot send messages here yet. Your message was removed.`;
const CONTENT_DELETED_MESSAGE = `Hi {member}, your recent message was removed because it violated our community guidelines (hate, gambling, NSFW, or spam).`;

/**
 * Improved moderation system prompt:
 * - precise, conservative instructions
 * - examples to help avoid ambiguous "FLAG" outputs and classify short slang like "bet" as OK
 */
const MODERATION_SYSTEM_PROMPT = `
You are a content moderation assistant. Your job is to examine a single user message and return exactly one token: one of:
  - FLAG_HATE
  - FLAG_NSFW
  - FLAG_BET
  - FLAG_SPAM
  - OK

Rules:
1. Be conservative and precise: only return a FLAG_* token if the message clearly contains activity in that category.
2. For betting: return FLAG_BET only when the user is clearly offering, requesting, or instructing on placing bets, exchanging betting picks for money, advertising paid picks, discussing odds/money, or requesting/arranging wagering. Single-word affirmations like "bet", "alright bet", "bet!" used as slang for "okay" should be classified as OK.
3. For hate or sexual content, look for targeted slurs or explicit sexual content respectively.
4. For spam, look for solicitations, mass-posted links, or clear promotional language.
5. If the message is ambiguous, return OK (do not return generic "FLAG" without a category).
6. Output must be exactly one token from the list above, nothing else (no explanation).

Examples (input -> output):
- "alright bet" -> OK
- "bet" -> OK
- "bet $50 on the Lakers -4" -> FLAG_BET
- "dm me for picks, $20 each" -> FLAG_BET
- "free picks for sale, dm" -> FLAG_BET
- "visit https://spam.example to get free coins" -> FLAG_SPAM
- "you suck, you f***" -> FLAG_HATE
- "porn link: http://..." -> FLAG_NSFW
`;

// ----- improved betting detection / affirmation handling -----
// short single-word or short-phrase affirmations we should treat as "OK"
const AFFIRMATION_REGEX = /^(?:bet|alright bet|bet!|bet\.*|bett+|bet rn|bet bro|bet fam|facts|ok|okie|yep|yup|ya|yah|betty)$/i;

// high-signal betting keywords/phrases that strongly indicate betting activity (dm for picks, selling, parlay, bookie, etc.)
const BETTING_SIGNAL_REGEX = /\b(?:dm\s+(?:for\s+)?picks|pm\s+(?:for\s+)?picks|dmme\s+for\s+picks|dm\s+for\s+pick|sell(?:ing)?\s+picks|picks\s+for\s+sale|parlay(?:s)?|sportsbook|bookie(?:s)?|parlaytip|parlay\s+tips|tipster|selling\s+picks|dm\s+for\s+picks|nba\s+picks|nba\s+pick|nbapicks|nba\s+parlay|mlb\s+picks|nba\s+parlays)\b/i;

// betting numeric/odds patterns that indicate a real bet (amounts, money signs, odds, spreads)
const BETTING_LIKELY_REGEX = /\b(?:\$\s?\d{1,6}(?:\.\d{1,2})?|\d{1,6}\s?(?:USD|usd|\$|€|£)|moneyline|ml|odds|spread|over|under|o\/u|parlay|vig|juice|\+\d{1,3}|\-\d{1,3})\b/;

const NSFW_KEYWORDS = [
  'porn','pornography','xxx','nsfw','camgirl','cams','nude','nudes','sex','pornhub','xvideos','xhamster','adult','explicit','breast','cock','pussy'
];
const NSFW_REGEX = new RegExp(`\\b(${NSFW_KEYWORDS.map(k => k.replace(/[.*+?^${}()|[\\]\\\\]/g,'\\$&')).join('|')})\\b`, 'i');

const SPAM_LINKS_REGEX = /\bhttps?:\/\/\S+\b/i;

// HATE_KEYWORDS: keep explicit slurs that we must block (the most severe forms). Per prior request, "nigga" is intentionally NOT included.
// This list is intentionally conservative to avoid overblocking. Add only clearly offensive slurs to this list.
const HATE_KEYWORDS = [
  'nigger','chink','kike','spic','wetback','gook','raghead','honky','faggot','fag','coon','paki'
];
const HATE_REGEX = new RegExp(`\\b(${HATE_KEYWORDS.map(k => k.replace(/[.*+?^${}()|[\\]\\\\]/g,'\\$&')).join('|')})\\b`, 'i');

// words we explicitly allow even if the classifier returns FLAG_HATE (to make the strike system less harsh)
// e.g., "nigga" usage is allowed per your request; "sweats" or similar gamer slang should not generate strikes.
// These are lowercased and used to reduce false positives for FLAG_HATE responses.
const HATE_ALLOWLIST = ['nigga', 'sweats', 'sweat', 'sweaties', 'sweaty'];

//
// ---------- MATCHMAKING CHANNEL CONFIG ----------
//
// Channel to enforce strict matchmaking message format (user requested)
const MATCH_CHANNEL_ID = process.env.MATCH_CHANNEL_ID || '1031429142538895380';

// Allowed prefix regex: starts with "code", "codes", "code?", "5." or "5 " or a single-letter "s" optionally followed by punctuation/space
const MATCH_ALLOWED_PREFIX_REGEX = /^\s*(?:code(?:s)?\b|code\?|5(?:\.|\b)|s(?:\.|\b))/i;

// Lightweight heuristic to detect messages that are clearly matchmaking-related even if they don't start with the allowed prefixes
// e.g., "LF5", "Looking for 5v5", "need 4 more", "5v5", "need 4"
const MATCH_HEURISTIC_REGEX = /\b(?:lf5|lfg|looking for 5v5|looking for 5|looking for players|looking for a team|need\s+\d+\s+(?:more\s+)?(?:players|people|people)|need\s+\d+\b|need\s+\d+\s+more|5v5|5v5s|team\s+of\s+5|need\s+4|need\s+3|need\s+2|need\s+1|need\s+4\s+more|want\s+5v5)\b/i;

//
// ---------- ADVERTISING CHANNEL CONFIG ----------
//
// Channel where advertising/league promotions are explicitly allowed (bypass moderation)
const AD_CHANNEL_ID = process.env.AD_CHANNEL_ID || '1120271059262906368';

//
// ---------- ENV & CLIENT ----------
//
const DISCORD_BOT_TOKEN = process.env.DISCORD_BOT_TOKEN;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const PERM_MUTE_THRESHOLD = parseInt(process.env.PERM_MUTE_THRESHOLD || '5', 10);
const STRIKE_DECAY_DAYS = parseInt(process.env.STRIKE_DECAY_DAYS || '30', 10);

if (!DISCORD_BOT_TOKEN) throw new Error('DISCORD_BOT_TOKEN env var is required');

const openai = OPENAI_API_KEY ? new OpenAI({ apiKey: OPENAI_API_KEY }) : null;
const prisma = new PrismaClient();

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
});

//
// ---------- RATE / QUEUE / CACHE ----------
//
let windowStart = Date.now();
let callsThisWindow = 0;
let activeCalls = 0;
const pendingQueue = [];
const messageCache = new Map();

function cacheGet(content) {
  const e = messageCache.get(content);
  if (!e) return null;
  if (Date.now() - e.ts > CACHE_TTL_MS) {
    messageCache.delete(content);
    return null;
  }
  return e;
}
function cacheSet(content, flagged, category) {
  messageCache.set(content, { flagged, category, ts: Date.now() });
}

function allowOpenAICall() {
  const now = Date.now();
  if (now - windowStart >= 60_000) {
    windowStart = now;
    callsThisWindow = 0;
  }
  if (callsThisWindow >= MAX_OPENAI_PER_MIN) return false;
  callsThisWindow += 1;
  return true;
}

function enqueueOpenAICall(content) {
  return new Promise((resolve) => {
    pendingQueue.push({ content, resolve });
    processQueue();
  });
}

async function processQueue() {
  while (activeCalls < CONCURRENCY && pendingQueue.length > 0) {
    const item = pendingQueue.shift();
    activeCalls += 1;
    try {
      const { flagged, category } = await performOpenAICall(item.content);
      item.resolve({ flagged, category });
    } catch (err) {
      item.resolve({ flagged: false, category: 'OK' });
    } finally {
      activeCalls -= 1;
    }
  }
}

async function performOpenAICall(content) {
  const cached = cacheGet(content);
  if (cached !== null) return { flagged: cached.flagged, category: cached.category };

  if (!allowOpenAICall()) {
    console.warn('OpenAI rate cap reached; skipping check for content.');
    cacheSet(content, false, 'OK');
    return { flagged: false, category: 'OK' };
  }
  if (!openai) {
    cacheSet(content, false, 'OK');
    return { flagged: false, category: 'OK' };
  }

  try {
    // Try Chat Completions; fall back gracefully if SDK differs
    let raw = '';
    try {
      const res = await openai.chat.completions.create({
        model: MODEL,
        messages: [
          { role: 'system', content: MODERATION_SYSTEM_PROMPT },
          { role: 'user', content },
        ],
        max_tokens: 16,
        temperature: 0,
      });
      raw = res.choices?.[0]?.message?.content?.trim?.() || '';
    } catch (e) {
      // some SDKs use openai.responses.create vs openai.chat.completions
      try {
        const r2 = await openai.responses.create({
          model: MODEL,
          input: `${MODERATION_SYSTEM_PROMPT}\n\n${content}`,
          max_tokens: 16,
        });
        raw = (r2.output?.[0]?.content?.[0]?.text || r2.output?.[0]?.content || '').trim();
      } catch (err2) {
        console.error('OpenAI both API attempts failed:', err2?.message || err2);
        cacheSet(content, false, 'OK');
        return { flagged: false, category: 'OK' };
      }
    }

    const out = (raw || '').toUpperCase();
    let category = 'OK';
    if (out.includes('FLAG_HATE')) category = 'FLAG_HATE';
    else if (out.includes('FLAG_NSFW')) category = 'FLAG_NSFW';
    else if (out.includes('FLAG_BET')) category = 'FLAG_BET';
    else if (out.includes('FLAG_SPAM')) category = 'FLAG_SPAM';
    else if (out === 'OK' || out.includes(' OK')) category = 'OK';
    else if (out.includes('FLAG')) {
      // Unexpected but still suspicious (OpenAI returned 'FLAG' or similar)
      console.warn('OpenAI returned ambiguous FLAG token:', raw);
      category = 'FLAG_SPAM';
    } else {
      console.warn('Unexpected moderation output (no token recognized):', raw);
      category = 'OK';
    }

    const flagged = category !== 'OK';
    cacheSet(content, flagged, category);
    return { flagged, category };
  } catch (err) {
    console.error('OpenAI call error:', err?.message || err);
    cacheSet(content, false, 'OK');
    return { flagged: false, category: 'OK' };
  }
}

/**
 * verifyStrikeDecision:
 * After a message has been flagged by the classifier, ask OpenAI to decide if a formal strike should be recorded.
 * Returns { strike: boolean, reason: string }.
 *
 * Leniency change: if OpenAI is unavailable or rate-limited, we now fall back to NO_STRIKE (lenient).
 */
async function verifyStrikeDecision(content, category) {
  if (!openai) return { strike: false, reason: 'verification_unavailable_no_openai_lenient' };
  if (!allowOpenAICall()) return { strike: false, reason: 'verification_unavailable_rate_limited_lenient' };

  const systemPrompt = `You are a moderation adjudicator. A classifier labeled a user message as "${category}". Your job: decide whether this message should generate a formal strike under typical community moderation rules. Consider severity, targeted insults, sexual explicitness, solicitations for paid picks, exchange of money, and repeated spam. Be conservative but protective of community safety.

Return EXACTLY two lines:
First line: STRIKE or NO_STRIKE
Second line: a single short reason (max 30 words) explaining the decision.

Message:
<<<
${content}
>>>`;

  try {
    let raw = '';
    try {
      const res = await openai.chat.completions.create({
        model: MODEL,
        messages: [
          { role: 'system', content: systemPrompt },
        ],
        max_tokens: 60,
        temperature: 0,
      });
      raw = res.choices?.[0]?.message?.content?.trim?.() || '';
    } catch (e) {
      try {
        const r2 = await openai.responses.create({
          model: MODEL,
          input: systemPrompt,
          max_tokens: 60,
        });
        raw = (r2.output?.[0]?.content?.[0]?.text || r2.output?.[0]?.content || '').trim();
      } catch (err2) {
        console.error('OpenAI verification both API attempts failed:', err2?.message || err2);
        // Lenient fallback: do not create strike if verification cannot run
        return { strike: false, reason: 'verification_failed_both_calls_lenient' };
      }
    }

    // parse response
    const lines = raw.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
    const first = (lines[0] || '').toUpperCase();
    const second = lines.slice(1).join(' ').slice(0, 200) || 'No reason provided';

    if (first.includes('STRIKE')) return { strike: true, reason: second };
    if (first.includes('NO_STRIKE')) return { strike: false, reason: second };
    // if ambiguous, be lenient and do NOT strike
    return { strike: false, reason: `ambiguous_verification_response_lenient: ${raw.slice(0,200)}` };
  } catch (err) {
    console.error('verifyStrikeDecision error', err?.message || err);
    // Lenient fallback
    return { strike: false, reason: 'verification_exception_lenient' };
  }
}

/**
 * decideAndApplyStrike:
 * Given a flagged message, run verification and conditionally create an entry/penalty.
 * This centralizes the behavior so we consistently apply verification for all OpenAI-flagged messages.
 *
 * IMPORTANT: We are lenient: verification must explicitly return STRIKE to create a strike.
 */
async function decideAndApplyStrike(guild, message, category, baseLog = {}) {
  try {
    // attempt verification
    const verification = await verifyStrikeDecision(message.content || '', category);
    await logDetailed(guild, { ...baseLog, event: 'openai.verification', reason: verification.reason, category });

    if (verification.strike) {
      // create strike
      try {
        await createStrike({
          guildId: guild.id,
          userId: message.author.id,
          category,
          reason: `auto-moderation - message flagged (verified)`,
          messageId: message.id,
          actorId: 'bot',
          meta: { verificationReason: verification.reason },
        });
        const count = await countActiveStrikes(guild.id, message.author.id);
        await logDetailed(guild, { event: 'strike.recorded', authorId: message.author.id, note: `Strikes now ${count}`, category, reason: verification.reason });
        if (count >= PERM_MUTE_THRESHOLD) {
          await muteMemberForever(message.member, `Reached ${count} strikes (permanent mute)`);
        } else if (category === 'FLAG_HATE' || category === 'FLAG_NSFW') {
          await muteMember(message.member, AUTO_MUTE_MINUTES * 3, `Auto-mute: ${category}`);
        }
      } catch (e) {
        await logDetailed(guild, { event: 'strike.create_failed', note: e?.message || e, category });
      }
    } else {
      // verification decided NO_STRIKE -> log and skip strike creation (lenient)
      await logDetailed(guild, {
        event: 'strike.skipped_by_verification',
        authorId: message.author.id,
        category,
        reason: verification.reason,
        note: 'Message was flagged by classifier but verification step declined to escalate to a strike (lenient).',
      });
    }
  } catch (e) {
    // On unexpected error: be lenient (do not auto-strike), but log the failure.
    await logDetailed(guild, { event: 'verification_flow_failed_lenient', note: e?.message || e, category });
  }
}

/**
 * shouldCheckByPolicy
 * - Uses deterministic heuristics first (affirmation guard, high-signal phrases, numeric checks)
 * - Always check for explicit NSFW/HATE
 * - Sampling fallback for everything else (reduced in forum channels)
 */
function shouldCheckByPolicy(content, channelType = null) {
  if (!content || !content.trim()) return false;
  const c = content.trim();

  // 1) VERY fast short-affirmation guard: short one-word "bet" replies are common slang for "ok"
  if (c.length <= 20 && AFFIRMATION_REGEX.test(c)) return false;

  // 2) High-confidence betting signals -> definitely check
  if (BETTING_SIGNAL_REGEX.test(c)) return true;

  // 3) Numeric/odds money patterns -> likely betting -> check
  if (BETTING_LIKELY_REGEX.test(c)) return true;

  // 4) Always check if NSFW/hate obvious keywords match
  if (NSFW_REGEX.test(c) || HATE_REGEX.test(c)) return true;

  // 5) If message is long enough and contains suspicious link / "dm for" etc.
  if (c.length > 120 && SPAM_LINKS_REGEX.test(c)) return true;

  // 6) Reduce sampling in forum channels
  if (channelType === ChannelType.GuildForum) {
    const forumSampleRate = Math.max(0.001, SAMPLE_RATE / 4);
    return Math.random() < forumSampleRate;
  }

  // 7) fallback sampling for general traffic
  return Math.random() < SAMPLE_RATE;
}

//
// ---------- LOGGING / EMBEDS ----------
//
const logChannels = new Map();
const setupLoggedGuilds = new Set();

function safeTruncate(s, n = 1000) {
  if (!s) return '';
  if (s.length <= n) return s;
  return s.slice(0, n) + '...';
}

function computeSeverity(obj) {
  const category = String(obj.category || '').toUpperCase();
  const action = String(obj.action || '').toLowerCase();
  const event = String(obj.event || '').toLowerCase();

  if (category === 'FLAG_HATE' || action === 'deleted' || event === 'role.assign.quarantine') return 'high';
  if (event.startsWith('openai.')) return 'medium';
  const spamMedium = (category === 'FLAG_SPAM' && action === 'deleted') || event.includes('spam_detection') || event.includes('spam_flagged');
  if (category === 'FLAG_NSFW' || category === 'FLAG_BET' || event.startsWith('ticket.') || spamMedium) return 'medium';
  return 'low';
}

async function logDetailed(guild, obj) {
  try {
    const severity = computeSeverity(obj);
    const ev = String(obj.event || '').toLowerCase();

    const allowDespiteLow =
      ev.includes('error') ||
      ev.includes('failed') ||
      ev.startsWith('ticket.') ||
      ev.startsWith('setup.') ||
      ev.startsWith('role.') ||
      ev.startsWith('channel.') ||
      ev.startsWith('member.') ||
      ev.startsWith('strike.') ||
      ev.startsWith('transcript.') ||
      ev.startsWith('dm.');

    if (severity === 'low' && !allowDespiteLow) {
      return;
    }

    const color = severity === 'high' ? 0xE02B2B : (severity === 'medium' ? 0xF59E0B : 0x22C55E);
    const prefix = severity === 'high' ? '⚠️ HIGH PRIORITY — ' : '';

    const embed = new EmbedBuilder()
      .setTimestamp()
      .setTitle(prefix + (obj.event ? String(obj.event) : (obj.action ? String(obj.action) : 'Log')))
      .setColor(color);

    if (guild) embed.addFields([{ name: 'Guild', value: `${guild.name}`, inline: true }]);
    if (obj.channelName) embed.addFields([{ name: 'Channel', value: `#${String(obj.channelName)}`, inline: true }]);

    let authorDisplay = obj.userDisplayName || obj.authorTag || 'Unknown';
    let authorMention = obj.authorId ? `<@${obj.authorId}>` : null;
    if (guild && obj.authorId) {
      try {
        const member = await guild.members.fetch(obj.authorId).catch(() => null);
        if (member) {
          authorDisplay = member.displayName;
          authorMention = `<@${member.id}>`;
        }
      } catch {}
    }
    const authorFieldValue = authorMention ? `${authorMention} — ${authorDisplay}` : `${authorDisplay}`;
    embed.addFields([{ name: 'Author', value: authorFieldValue, inline: true }]);
    embed.addFields([{ name: 'Severity', value: severity.toUpperCase(), inline: true }]);

    const meta = [];
    if (obj.reason) meta.push(`Reason: ${obj.reason}`);
    if (obj.category) meta.push(`Category: ${obj.category}`);
    if (obj.note) meta.push(`Note: ${obj.note}`);
    if (obj.check_reason) meta.push(`Precheck: ${obj.check_reason}`);
    if (meta.length > 0) embed.addFields([{ name: 'Meta', value: meta.join('\n'), inline: false }]);

    const content = obj.content ? String(obj.content) : '';
    if (content) embed.addFields([{ name: 'Message', value: safeTruncate(content, 1024), inline: false }]);

    if (obj.messageLink) {
      const linkField = `[Jump to message](${String(obj.messageLink)})`;
      embed.addFields([{ name: 'Message Link', value: safeTruncate(linkField, 1024), inline: false }]);
    } else if (obj.channelId && obj.messageId) {
      try {
        const guildId = guild ? guild.id : obj.guildId;
        if (guildId) {
          const link = `https://discord.com/channels/${guildId}/${obj.channelId}/${obj.messageId}`;
          embed.addFields([{ name: 'Message Link', value: safeTruncate(`[Jump to message](${link})`, 1024), inline: false }]);
        }
      } catch (e) { /* ignore */ }
    }

    if (obj.authorTag) embed.setFooter({ text: `Tag: ${obj.authorTag}` });

    const ch = guild ? logChannels.get(guild.id) : null;
    if (ch) {
      try { await ch.send({ embeds: [embed] }); } catch (e) { /* avoid recursive logs */ }
    }

    if (severity !== 'low' || allowDespiteLow) {
      try {
        const consoleObj = {
          ts: new Date().toISOString(),
          event: obj.event,
          messageId: obj.messageId,
          authorId: obj.authorId,
          authorTag: obj.authorTag,
          channelId: obj.channelId,
          channelName: obj.channelName,
          severity,
          reason: obj.reason,
          category: obj.category,
          messageLink: obj.messageLink || null,
          content: content ? safeTruncate(content, 400) : undefined
        };
        console.log(JSON.stringify(consoleObj, null, 2));
      } catch (e) {}
    }
  } catch (e) {
    console.error('logDetailed error:', e?.message || e);
  }
}

//
// ---------- HELPERS (account age, muted role) ----------
//
function isNewAccount(userOrMember) {
  if (!userOrMember) return false;
  const user = userOrMember.user ? userOrMember.user : userOrMember;
  if (!user || !user.createdAt) return false;
  const ageMs = Date.now() - user.createdAt.getTime();
  return ageMs < ACCOUNT_AGE_LIMIT_DAYS * 24 * 60 * 60 * 1000;
}

async function ensureMutedRole(guild) {
  await guild.roles.fetch();
  let role = guild.roles.cache.find(r => r.name === MUTED_ROLE_NAME);
  if (!role) {
    role = await guild.roles.create({ name: MUTED_ROLE_NAME, color: 'DarkGrey', reason: 'Muted role for spammers/offenders' });
    for (const ch of guild.channels.cache.values()) {
      try {
        if (ch.type === ChannelType.GuildText && ch.permissionsFor(guild.members.me).has(PermissionsBitField.Flags.ManageChannels)) {
          await ch.permissionOverwrites.edit(role.id, { SendMessages: false, AddReactions: false }).catch(() => {});
        }
      } catch {}
    }
  }
  return role;
}

async function muteMember(member, minutes, reason = 'Auto-mute (spam)') {
  const guild = member.guild;
  const role = await ensureMutedRole(guild);
  try {
    await member.roles.add(role, reason);
    await logDetailed(guild, { event: 'member.muted', reason, authorId: member.id, userDisplayName: member.displayName, note: `Muted for ${minutes} minutes` });
    setTimeout(async () => {
      try {
        await member.roles.remove(role, 'Auto-unmute after timeout').catch(() => {});
        await logDetailed(guild, { event: 'member.unmuted', authorId: member.id, userDisplayName: member.displayName });
      } catch (e) {
        await logDetailed(guild, { event: 'member.unmute_failed', note: e?.message || e });
      }
    }, minutes * 60 * 1000);
  } catch (e) {
    await logDetailed(guild, { event: 'member.mute_failed', note: e?.message || e });
  }
}

async function muteMemberForever(member, reason = 'Permanent mute (strike threshold)') {
  try {
    const guild = member.guild;
    const role = await ensureMutedRole(guild);
    if (!member.roles.cache.has(role.id)) {
      await member.roles.add(role, reason);
      await logDetailed(guild, { event: 'member.muted_forever', authorId: member.id, userDisplayName: member.displayName || member.user.username, note: reason });
    } else {
      await logDetailed(guild, { event: 'member.already_muted_forever', authorId: member.id, note: 'Member already has muted role' });
    }
  } catch (e) {
    await logDetailed(member.guild, { event: 'member.mute_forever_failed', authorId: member.id, note: e?.message || e });
  }
}

//
// ---------- DATABASE HELPERS (Prisma) ----------
//
async function upsertGuildConfig(guildId, data) {
  return prisma.guildConfig.upsert({
    where: { guildId },
    update: { ...data, updatedAt: new Date() },
    create: { guildId, ...data },
  });
}
async function getGuildConfig(guildId) {
  return prisma.guildConfig.findUnique({ where: { guildId } });
}

async function createTicketRow(obj) {
  return prisma.ticket.create({ data: obj });
}
async function getTicketRow(channelId) {
  return prisma.ticket.findUnique({ where: { channelId } });
}
async function getOpenTicketByOwner(guildId, ownerId) {
  return prisma.ticket.findFirst({ where: { guildId, ownerId, closedAt: null } });
}
async function claimTicketRow(channelId, claimerId) {
  return prisma.ticket.update({ where: { channelId }, data: { claimerId } });
}
async function closeTicketRow(channelId) {
  return prisma.ticket.update({ where: { channelId }, data: { closedAt: new Date() } });
}

async function createStrike({ guildId, userId, category, reason, messageId, actorId, meta }) {
  return prisma.strike.create({
    data: {
      guildId,
      userId,
      category,
      reason,
      messageId,
      actorId,
      meta: meta || {},
    }
  });
}

async function countActiveStrikes(guildId, userId, decayDays = STRIKE_DECAY_DAYS) {
  const cutoff = new Date(Date.now() - decayDays * 24 * 60 * 60 * 1000);
  const q = await prisma.strike.count({
    where: {
      guildId,
      userId,
      pardoned: false,
      createdAt: { gt: cutoff },
    }
  });
  return q;
}

//
// ---------- TICKET HELPERS & LOCKS ----------
//
const TICKET_PANEL_CUSTOM_ID = 'create_ticket_button_v1';

// Interaction dedupe: avoid processing same interaction id twice.
const processedInteractions = new Set();
const PROCESSED_INTERACTION_TTL_MS = 60 * 1000; // 60s TTL

// Locking for modal/button display (pre-modal) to prevent multiple modals from same user quickly.
const ticketModalLocks = new Set();
const ticketModalTimers = new Map();
const TICKET_MODAL_LOCK_TTL_MS = 30 * 1000; // 30s

// Promise-based lock map to serialize ticket creation per (guildId:userId)
const ticketCreationLocks = new Map();
const TICKET_CREATION_LOCK_TTL_MS = 30 * 1000; // safety TTL

function ticketChannelName(member, subject) {
  const base = (member.displayName || member.user.username || 'ticket').toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').slice(0, 20);
  const subj = (subject || 'ticket').toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').slice(0, 20);
  const suffix = `${Date.now() % 10000}`;
  return `ticket-${base}-${subj}-${suffix}`;
}

async function ensureTicketPanel(guild) {
  try {
    let cfg = await getGuildConfig(guild.id);
    if (!cfg) {
      const ticketCategory = await guild.channels.create({ name: DEFAULT_TICKET_CATEGORY_NAME, type: ChannelType.GuildCategory });
      const panelCreated = await guild.channels.create({ name: DEFAULT_PANEL_CHANNEL_NAME, type: ChannelType.GuildText, parent: ticketCategory.id });
      await upsertGuildConfig(guild.id, { panelChannelId: panelCreated.id, ticketCategoryId: ticketCategory.id });
      cfg = await getGuildConfig(guild.id);
      await logDetailed(guild, { event: 'setup.guild_config', note: 'Default ticket panel & category created and saved' });
    }

    const panelId = cfg.panelChannelId;
    const panel = panelId ? await guild.channels.fetch(panelId).catch(() => null) : null;

    const panelIsText = !!panel && (
      (typeof panel.isText === 'function' ? panel.isText() :
        (panel.type === ChannelType.GuildText || typeof panel.send === 'function'))
    );

    if (panelIsText) {
      const msgs = await (panel.messages ? panel.messages.fetch({ limit: 50 }).catch(() => null) : null);
      if (msgs && msgs.some(m => m.author?.id === client.user.id && m.components?.length)) return;

      const embed = new EmbedBuilder()
        .setTitle('Support / Ticket Center')
        .setDescription('Need help? Click **Open Ticket** to create a private ticket channel where staff can assist you.\nWhen creating a ticket you will be asked for a **subject** and **message**. Mods will be notified.')
        .setColor(0x3b82f6)
        .setTimestamp();
      const button = new ButtonBuilder().setCustomId(TICKET_PANEL_CUSTOM_ID).setLabel('Open Ticket').setStyle(ButtonStyle.Primary);
      const row = new ActionRowBuilder().addComponents(button);
      await panel.send({ embeds: [embed], components: [row] });
      await logDetailed(guild, { event: 'ticket.panel_sent', channelName: panel.name });
    }
  } catch (e) {
    await logDetailed(guild, { event: 'ticket.panel_failed', note: e?.message || e });
  }
}

/**
 * createTicketChannel:
 * - Promise lock per (guildId:userId) to serialize creation inside process
 * - Re-check DB before insert
 * - Create channel first, attempt DB insert; on DB unique violation, cleanup and return existing
 */
async function createTicketChannel(guild, member, subject, messageBody) {
  const lockKey = `${guild.id}:${member.id}`;

  // If creation in progress, wait for it to finish and return existing if present
  if (ticketCreationLocks.has(lockKey)) {
    const existingLock = ticketCreationLocks.get(lockKey);
    try { await existingLock.promise; } catch (_) {}
    const existingAfter = await getOpenTicketByOwner(guild.id, member.id).catch(() => null);
    if (existingAfter && existingAfter.channelId) {
      const existingCh = guild.channels.cache.get(existingAfter.channelId) || await guild.channels.fetch(existingAfter.channelId).catch(()=>null);
      if (existingCh) return existingCh;
    }
    // else continue to try creating (rare)
  }

  // create lock
  let resolveLock, rejectLock;
  const lockObj = {};
  lockObj.promise = new Promise((res, rej) => { resolveLock = res; rejectLock = rej; });
  lockObj.resolve = resolveLock;
  lockObj.reject = rejectLock;
  // TTL to avoid stuck lock
  lockObj.timerId = setTimeout(() => {
    try { lockObj.resolve(); } catch (_) {}
    ticketCreationLocks.delete(lockKey);
  }, TICKET_CREATION_LOCK_TTL_MS);
  ticketCreationLocks.set(lockKey, lockObj);

  try {
    const open = await getOpenTicketByOwner(guild.id, member.id);
    if (open && open.channelId) {
      const channel = guild.channels.cache.get(open.channelId) || await guild.channels.fetch(open.channelId).catch(()=>null);
      if (channel) {
        await logDetailed(guild, { event: 'ticket.open_exists', reason: 'User already has an open ticket (pre-check)', authorId: member.id, channelName: channel.name });
        return channel;
      } else {
        try { await closeTicketRow(open.channelId); } catch (e) {}
      }
    }

    // ensure category
    let cfg = await getGuildConfig(guild.id);
    let category = cfg?.ticketCategoryId ? await guild.channels.fetch(cfg.ticketCategoryId).catch(()=>null) : null;
    if (!category) {
      category = await guild.channels.create({ name: DEFAULT_TICKET_CATEGORY_NAME, type: ChannelType.GuildCategory });
      await upsertGuildConfig(guild.id, { ticketCategoryId: category.id });
    }

    const modRole = guild.roles.cache.find(r => r.name === MODERATOR_ROLE_NAME);

    const channel = await guild.channels.create({
      name: ticketChannelName(member, subject),
      type: ChannelType.GuildText,
      parent: category.id,
      permissionOverwrites: [
        { id: guild.roles.everyone.id, deny: [PermissionsBitField.Flags.ViewChannel] },
        { id: member.id, allow: [PermissionsBitField.Flags.ViewChannel, PermissionsBitField.Flags.SendMessages, PermissionsBitField.Flags.ReadMessageHistory] },
        ...(modRole ? [{ id: modRole.id, allow: [PermissionsBitField.Flags.ViewChannel, PermissionsBitField.Flags.SendMessages, PermissionsBitField.Flags.ReadMessageHistory] }] : []),
        { id: client.user.id, allow: [PermissionsBitField.Flags.ViewChannel, PermissionsBitField.Flags.SendMessages, PermissionsBitField.Flags.ManageChannels] },
      ],
    });

    // Try DB insert
    try {
      await createTicketRow({ channelId: channel.id, guildId: guild.id, ownerId: member.id, subject, meta: { createdBy: member.user.tag }, createdAt: new Date() });
    } catch (dbErr) {
      const isUniqueErr = (dbErr && (dbErr.code === 'P2002' || dbErr.code === '23505' || (dbErr?.meta && Array.isArray(dbErr.meta.target))));
      if (isUniqueErr) {
        await logDetailed(guild, { event: 'ticket.db_unique_violation', note: `Unique constraint prevented duplicate ticket for ${member.id}`, authorId: member.id });
        const existing = await getOpenTicketByOwner(guild.id, member.id).catch(() => null);
        if (existing && existing.channelId && existing.channelId !== channel.id) {
          try {
            await channel.delete('Duplicate ticket created; DB already has open ticket');
          } catch (delErr) {
            await logDetailed(guild, { event: 'ticket.channel_delete_failed', note: delErr?.message || delErr });
          }
          const existingChannel = guild.channels.cache.get(existing.channelId) || await guild.channels.fetch(existing.channelId).catch(()=>null);
          if (existingChannel) return existingChannel;
        }
        // if weirdness, try a second insert (best-effort), otherwise keep created channel
        try {
          await createTicketRow({ channelId: channel.id, guildId: guild.id, ownerId: member.id, subject, meta: { createdBy: member.user.tag }, createdAt: new Date() });
        } catch (err2) {
          await logDetailed(guild, { event: 'ticket.create_after_unique_failed', note: err2?.message || err2 });
        }
      } else {
        await logDetailed(guild, { event: 'ticket.create_db_error', note: dbErr?.message || dbErr });
        throw dbErr;
      }
    }

    // send header & buttons
    const headerEmbed = new EmbedBuilder()
      .setTitle(`Ticket: ${subject}`)
      .setDescription(`Owner: ${member} — ${member.displayName}\nSubject: ${subject}\n\n${safeTruncate(messageBody, 1000)}`)
      .setColor(0x60A5FA)
      .setTimestamp();

    const claimBtn = new ButtonBuilder().setCustomId(`ticket_claim_${channel.id}`).setLabel('Claim').setStyle(ButtonStyle.Success);
    const closeBtn = new ButtonBuilder().setCustomId(`ticket_close_${channel.id}`).setLabel('Close').setStyle(ButtonStyle.Danger);
    const row = new ActionRowBuilder().addComponents(claimBtn, closeBtn);

    await channel.send({ content: modRole ? `${modRole}` : 'Moderators, new ticket opened', embeds: [headerEmbed], components: [row] });

    await logDetailed(guild, {
      event: 'ticket.opened',
      reason: 'Ticket created',
      authorId: member.id,
      authorTag: member.user.tag,
      userDisplayName: member.displayName,
      channelName: channel.name,
      content: `Subject: ${subject}\nMessage: ${safeTruncate(messageBody, 1200)}`,
    });

    const modLogCh = logChannels.get(guild.id);
    if (modLogCh && modRole) {
      await modLogCh.send({ content: `${modRole} — new ticket opened by <@${member.id}> (${member.displayName}) in <#${channel.id}>` });
    }

    return channel;
  } catch (e) {
    await logDetailed(guild, { event: 'ticket.create_failed', note: e?.message || e });
    throw e;
  } finally {
    const lock = ticketCreationLocks.get(lockKey);
    if (lock) {
      try { lock.resolve(); } catch (_) {}
      if (lock.timerId) clearTimeout(lock.timerId);
      ticketCreationLocks.delete(lockKey);
    }
  }
}

async function fetchAllMessages(channel, limitAll = 2000) {
  const messages = [];
  let lastId = null;
  while (true) {
    const options = { limit: 100 };
    if (lastId) options.before = lastId;
    const fetched = await channel.messages.fetch(options);
    if (!fetched || fetched.size === 0) break;
    messages.push(...Array.from(fetched.values()));
    lastId = fetched.last().id;
    if (messages.length >= limitAll) break;
    if (fetched.size < 100) break;
  }
  return messages.reverse();
}

async function saveTranscriptAndLog(guild, ticketChannel, ticketMeta) {
  try {
    const msgs = await fetchAllMessages(ticketChannel);
    const lines = msgs.map(m => {
      const time = m.createdAt.toISOString();
      const author = `${m.author.tag} (${m.member ? m.member.displayName : m.author.username})`;
      const content = m.content || '';
      const attach = m.attachments.size ? ` [attachments: ${Array.from(m.attachments.values()).map(a => a.url).join(', ')}]` : '';
      return `[${time}] ${author}: ${content}${attach}`;
    });

    const transcriptText = `Ticket transcript\nSubject: ${ticketMeta.subject}\nOwner: <@${ticketMeta.ownerId}>\nCreated: ${new Date(ticketMeta.createdAt).toISOString()}\nClaimer: ${ticketMeta.claimerId ? `<@${ticketMeta.claimerId}>` : 'none'}\n\nMessages:\n\n${lines.join(os.EOL)}`;

    const fileName = `transcript-${ticketChannel.name}-${Date.now()}.txt`;
    const filePath = path.join(os.tmpdir(), fileName);
    fs.writeFileSync(filePath, transcriptText, 'utf8');

    const attachment = new AttachmentBuilder(fs.createReadStream(filePath), { name: fileName });

    const modLogCh = logChannels.get(guild.id);
    if (modLogCh) {
      const embed = new EmbedBuilder()
        .setTitle('Ticket transcript saved')
        .setDescription(`Subject: ${ticketMeta.subject}\nOwner: <@${ticketMeta.ownerId}>\nClaimer: ${ticketMeta.claimerId ? `<@${ticketMeta.claimerId}>` : 'none'}`)
        .setTimestamp()
        .setColor(0x6B7280);

      await modLogCh.send({ embeds: [embed], files: [attachment] });
    }

    try { fs.unlinkSync(filePath); } catch {}
  } catch (e) {
    await logDetailed(guild, { event: 'transcript.save_failed', note: e?.message || e });
  }
}

async function claimTicket(interaction, channelId) {
  const guild = interaction.guild;
  const member = interaction.member;
  const modRole = guild.roles.cache.find(r => r.name === MODERATOR_ROLE_NAME);
  if (!modRole || !member.roles.cache.has(modRole.id)) {
    try {
      if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: 'You must be a moderator to claim tickets.', flags: 64 });
      else await interaction.followUp({ content: 'You must be a moderator to claim tickets.', flags: 64 });
    } catch {}
    return;
  }
  try {
    await claimTicketRow(channelId, member.id);
    const channel = guild.channels.cache.get(channelId);
    if (channel) await channel.send({ content: `Ticket claimed by <@${member.id}> (${member.displayName})` }).catch(()=>{});
    await logDetailed(guild, { event: 'ticket.claimed', reason: `Claimed by ${member.displayName}`, authorId: member.id, channelName: channel ? channel.name : channelId });
    if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: 'You claimed the ticket.', flags: 64 });
    else await interaction.followUp({ content: 'You claimed the ticket.', flags: 64 });
  } catch (e) {
    await logDetailed(guild, { event: 'ticket.claim_failed', note: e?.message || e });
    try { if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: 'Failed to claim ticket.', flags: 64 }); else await interaction.followUp({ content: 'Failed to claim ticket.', flags: 64 }); } catch {}
  }
}

async function closeTicket(interaction, channelId, closedByMember) {
  const guild = interaction.guild;
  const member = closedByMember;
  const meta = await getTicketRow(channelId);
  if (!meta) {
    try { if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: 'Ticket metadata not found.', flags: 64 }); else await interaction.followUp({ content: 'Ticket metadata not found.', flags: 64 }); } catch {}
    return;
  }

  const isOwner = member.id === meta.ownerId;
  const modRole = guild.roles.cache.find(r => r.name === MODERATOR_ROLE_NAME);
  const isMod = modRole ? member.roles.cache.has(modRole.id) : false;

  if (!isOwner && !isMod) {
    try { if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: 'You do not have permission to close this ticket.', flags: 64 }); else await interaction.followUp({ content: 'You do not have permission to close this ticket.', flags: 64 }); } catch {}
    return;
  }

  const channel = guild.channels.cache.get(channelId);
  if (!channel) {
    try { if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: 'Ticket channel not found (already deleted?)', flags: 64 }); else await interaction.followUp({ content: 'Ticket channel not found (already deleted?)', flags: 64 }); } catch {}
    return;
  }

  // Save transcript first
  await saveTranscriptAndLog(guild, channel, meta);

  const replyText = 'Ticket closed and transcript saved to moderation log.';
  try {
    if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: replyText, flags: 64 });
    else await interaction.followUp({ content: replyText, flags: 64 });
  } catch (e) { console.warn('Failed reply pre-delete', e?.message || e); }

  try { await closeTicketRow(channelId); } catch (e) { await logDetailed(guild, { event: 'ticket.close_db_failed', note: e?.message || e }); }

  await logDetailed(guild, {
    event: 'ticket.closed',
    reason: `Closed by ${member.displayName} (${isMod ? 'mod' : 'owner'})`,
    authorId: member.id,
    channelName: channel.name,
    content: `Subject: ${meta.subject}`,
  });

  try {
    await channel.delete('Ticket closed and archived by bot');
  } catch (e) {
    await logDetailed(guild, { event: 'ticket.delete_failed', note: e?.message || e });
  }
}

//
// ---------- GUILD SETUP (roles, categories, log channel) ----------
//
async function setupGuild(guild) {
  try {
    await guild.roles.fetch();
    let modRole = guild.roles.cache.find(r => r.name === MODERATOR_ROLE_NAME);
    if (!modRole) {
      modRole = await guild.roles.create({ name: MODERATOR_ROLE_NAME, color: 'Blue', reason: 'Moderation role' });
      await logDetailed(guild, { event: 'role.create', reason: `Created ${MODERATOR_ROLE_NAME}` });
    }
    let quarantinedRole = guild.roles.cache.find(r => r.name === QUARANTINED_ROLE_NAME);
    if (!quarantinedRole) {
      quarantinedRole = await guild.roles.create({ name: QUARANTINED_ROLE_NAME, color: 'Grey', reason: 'Quarantine for new accounts' });
      await logDetailed(guild, { event: 'role.create', reason: `Created ${QUARANTINED_ROLE_NAME}` });
    }
    await ensureMutedRole(guild);

    let category = guild.channels.cache.find(c => c.type === ChannelType.GuildCategory && c.name === MOD_CATEGORY_NAME);
    if (!category) {
      category = await guild.channels.create({
        name: MOD_CATEGORY_NAME,
        type: ChannelType.GuildCategory,
        permissionOverwrites: [
          { id: guild.roles.everyone.id, deny: [PermissionsBitField.Flags.ViewChannel] },
          { id: modRole.id, allow: [PermissionsBitField.Flags.ViewChannel, PermissionsBitField.Flags.SendMessages] },
          { id: client.user.id, allow: [PermissionsBitField.Flags.ViewChannel, PermissionsBitField.Flags.SendMessages, PermissionsBitField.Flags.ManageChannels] },
        ],
      });
      await logDetailed(guild, { event: 'category.create', reason: `Created private ${MOD_CATEGORY_NAME}` });
    } else {
      try {
        await category.permissionOverwrites.edit(guild.roles.everyone.id, { ViewChannel: false }).catch(() => {});
        await category.permissionOverwrites.edit(modRole.id, { ViewChannel: true, SendMessages: true }).catch(() => {});
        await category.permissionOverwrites.edit(client.user.id, { ViewChannel: true, SendMessages: true, ManageChannels: true }).catch(() => {});
      } catch (e) { await logDetailed(guild, { event: 'category.permission_warn', note: e?.message || e }); }
    }

    let logChannel = guild.channels.cache.find(ch => ch.type === ChannelType.GuildText && ch.name === LOG_CHANNEL_NAME && ch.parentId === category.id);
    if (!logChannel) {
      logChannel = await guild.channels.create({
        name: LOG_CHANNEL_NAME,
        type: ChannelType.GuildText,
        parent: category.id,
        permissionOverwrites: [
          { id: guild.roles.everyone.id, deny: [PermissionsBitField.Flags.ViewChannel] },
          { id: modRole.id, allow: [PermissionsBitField.Flags.ViewChannel, PermissionsBitField.Flags.SendMessages] },
          { id: client.user.id, allow: [PermissionsBitField.Flags.ViewChannel, PermissionsBitField.Flags.SendMessages] },
        ],
      });
      await logDetailed(guild, { event: 'channel.create', reason: `Created ${LOG_CHANNEL_NAME}` });
    }
    logChannels.set(guild.id, logChannel);

    // Ticket defaults in DB
    let cfg = await getGuildConfig(guild.id);
    if (!cfg) {
      const ticketCategory = await guild.channels.create({ name: DEFAULT_TICKET_CATEGORY_NAME, type: ChannelType.GuildCategory });
      const panel = await guild.channels.create({ name: DEFAULT_PANEL_CHANNEL_NAME, type: ChannelType.GuildText, parent: ticketCategory.id });
      await upsertGuildConfig(guild.id, { panelChannelId: panel.id, ticketCategoryId: ticketCategory.id });
      cfg = await getGuildConfig(guild.id);
      await logDetailed(guild, { event: 'setup.guild_config', note: 'Default ticket panel & category created and saved' });
    }

    // ensure panel has embed & button
    await ensureTicketPanel(guild);

    if (!setupLoggedGuilds.has(guild.id)) {
      await logDetailed(guild, { event: 'setup.complete', note: `Setup complete for ${guild.name}` });
      setupLoggedGuilds.add(guild.id);
    }
  } catch (err) {
    await logDetailed(guild, { event: 'setup.failed', note: err?.message || err });
  }
}

//
// ---------- DM QUEUE (rate-limit safe) ----------
//
const dmQueue = [];
let dmInProgress = false;
const lastDmToUser = new Map(); // per-user last DM time (ms)
const DM_BACKOFF_BASE_MS = 800; // base backoff

async function sendDMWithRetries(userOrMember, payload, maxRetries = 5) {
  const id = userOrMember.id || (userOrMember.user && userOrMember.user.id);
  if (!id) return;
  return new Promise((resolve) => {
    dmQueue.push({ id, userOrMember, payload, resolve, tries: 0, maxRetries });
    processDmQueue();
  });
}

async function processDmQueue() {
  if (dmInProgress) return;
  dmInProgress = true;
  while (dmQueue.length > 0) {
    const item = dmQueue.shift();
    const { userOrMember, payload } = item;
    const target = userOrMember.user ? userOrMember.user : userOrMember;
    let tries = 0;
    while (tries <= item.maxRetries) {
      try {
        const now = Date.now();
        const last = lastDmToUser.get(target.id) || 0;
        if (now - last < 700) await new Promise((r) => setTimeout(r, 700 - (now - last)));
        await target.send(payload);
        lastDmToUser.set(target.id, Date.now());
        await logDetailed(null, { event: 'dm.sent', reason: 'DM via queue', authorId: target.id });
        item.resolve(true);
        break;
      } catch (e) {
        const msg = e?.message || String(e);
        const isRate = msg.includes('You are opening direct messages too fast') || (e?.code === 50007) || (e?.status === 429);
        tries += 1;
        if (!isRate || tries > item.maxRetries) {
          await logDetailed(null, { event: 'dm.failed', reason: 'DM attempt failed', authorId: target.id, note: msg });
          item.resolve(false);
          break;
        }
        const backoff = DM_BACKOFF_BASE_MS * Math.pow(2, tries);
        await new Promise((r) => setTimeout(r, backoff));
      }
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  dmInProgress = false;
}

//
// ---------- QUARANTINE PERSISTENCE & BACKGROUND JOB ----------
//
const QUARANTINE_FILE = path.join(process.cwd(), 'quarantine_map.json');
let quarantineMap = {}; // { "<guildId>:<userId>": timestampMs }

function loadQuarantineMap() {
  try {
    if (fs.existsSync(QUARANTINE_FILE)) {
      const raw = fs.readFileSync(QUARANTINE_FILE, 'utf8');
      quarantineMap = JSON.parse(raw || '{}');
    } else quarantineMap = {};
  } catch (e) { quarantineMap = {}; }
}
function saveQuarantineMap() {
  try { fs.writeFileSync(QUARANTINE_FILE, JSON.stringify(quarantineMap), 'utf8'); } catch (e) { console.warn('Failed to save quarantine map', e?.message || e); }
}
function setQuarantineTimestamp(guildId, userId, ts = Date.now()) { quarantineMap[`${guildId}:${userId}`] = ts; saveQuarantineMap(); }
function getQuarantineTimestamp(guildId, userId) { return quarantineMap[`${guildId}:${userId}`] || null; }
function clearQuarantineTimestamp(guildId, userId) { delete quarantineMap[`${guildId}:${userId}`]; saveQuarantineMap(); }

async function scanGuildsForQuarantine() {
  try {
    for (const guild of client.guilds.cache.values()) {
      await guild.members.fetch().catch(()=>{});
      const quarantinedRole = guild.roles.cache.find(r => r.name === QUARANTINED_ROLE_NAME);
      if (!quarantinedRole) continue;
      for (const member of guild.members.cache.values()) {
        if (member.user?.bot) continue;
        const isQuarantine = member.roles.cache.has(quarantinedRole.id);
        const isNew = isNewAccount(member.user);
        if (isNew && !isQuarantine) {
          try {
            await member.roles.add(quarantinedRole, 'Periodic quarantine check: account too new');
            setQuarantineTimestamp(guild.id, member.id, Date.now());
            await logDetailed(guild, { event: 'role.assign.quarantine', reason: 'Hourly re-check assigned quarantine', authorId: member.id });
          } catch (e) { await logDetailed(guild, { event: 'role.assign_failed', note: e?.message || e }); }
        }
        if (isQuarantine && !isNew) {
          try {
            await member.roles.remove(quarantinedRole, 'Periodic re-check: account aged past limit');
            clearQuarantineTimestamp(guild.id, member.id);
            await logDetailed(guild, { event: 'role.remove', roleName: quarantinedRole.name, authorId: member.id });
          } catch (e) { await logDetailed(guild, { event: 'role.remove_failed', note: e?.message || e }); }
        }
      }

      for (const key of Object.keys(quarantineMap)) {
        const [gId, uId] = key.split(':');
        if (gId !== guild.id) continue;
        const ts = quarantineMap[key];
        if (!ts) continue;
        const age = Date.now() - ts;
        const SEVENTY_TWO_HOURS = 72 * 60 * 60 * 1000;
        if (age >= SEVENTY_TWO_HOURS) {
          try {
            const member = await guild.members.fetch(uId).catch(()=>null);
            if (member && member.roles.cache.has(quarantinedRole.id)) {
              await member.roles.remove(quarantinedRole, 'Quarantine period expired (72h) - hourly job');
              clearQuarantineTimestamp(guild.id, member.id);
              await logDetailed(guild, { event: 'role.remove', roleName: quarantinedRole.name, authorId: member.id, reason: 'Quarantine expired (72h)' });
            } else {
              clearQuarantineTimestamp(guild.id, uId);
            }
          } catch (e) { await logDetailed(guild, { event: 'role.remove_failed', note: e?.message || e }); }
        }
      }
    }
  } catch (e) { console.error('scanGuildsForQuarantine error', e?.message || e); }
}

//
// ---------- GUILD EVENTS ----------
//
async function handleClientReady() {
  if (handleClientReady._handled) return;
  handleClientReady._handled = true;

  console.log(`Logged in as ${client.user.tag}`);
  loadQuarantineMap();
  for (const g of client.guilds.cache.values()) await setupGuild(g);
  scanGuildsForQuarantine().catch(()=>{});
  setInterval(scanGuildsForQuarantine, 60 * 60 * 1000);
}
client.once('ready', handleClientReady);
client.once('clientReady', handleClientReady);

client.on('guildCreate', async (guild) => { await setupGuild(guild); });

client.on('guildMemberAdd', async (member) => {
  const guild = member.guild;
  await setupGuild(guild).catch(() => {});
  const quarantinedRole = guild.roles.cache.find(r => r.name === QUARANTINED_ROLE_NAME);

  try {
    await sendDMWithRetries(member, { content: GREETING_MESSAGE.replace('{member}', `<@${member.id}>`) });
    await logDetailed(guild, { event: 'dm.sent', reason: 'Greeting DM sent', authorId: member.id, authorTag: member.user.tag, userDisplayName: member.displayName });
  } catch (e) { await logDetailed(guild, { event: 'dm.failed', reason: 'Greeting DM failed', authorId: member.id, authorTag: member.user.tag, userDisplayName: member.displayName, note: e?.message || e }); }

  if (isNewAccount(member.user) && quarantinedRole) {
    try {
      await member.roles.add(quarantinedRole, 'Account under age limit');
      setQuarantineTimestamp(guild.id, member.id, Date.now());
      await logDetailed(guild, { event: 'role.assign.quarantine', roleName: quarantinedRole.name, reason: `${QUARANTINED_ROLE_NAME} assigned on join`, authorId: member.id, authorTag: member.user.tag, userDisplayName: member.displayName });
    } catch (e) { await logDetailed(guild, { event: 'role.assign_failed', roleName: quarantinedRole ? quarantinedRole.name : QUARANTINED_ROLE_NAME, note: e?.message || e }); }
  }
});

//
// ---------- INTERACTIONS ----------
//
client.on('interactionCreate', async (interaction) => {
  try {
    // Dedupe identical interaction deliveries
    if (processedInteractions.has(interaction.id)) return;
    processedInteractions.add(interaction.id);
    setTimeout(() => processedInteractions.delete(interaction.id), PROCESSED_INTERACTION_TTL_MS);

    if (interaction.isButton()) {
      const customId = interaction.customId;

      if (customId === TICKET_PANEL_CUSTOM_ID) {
        // modal lock key per guild:user
        const modalLockKey = `${interaction.guildId || interaction.guild.id}:${interaction.user.id}`;
        if (ticketModalLocks.has(modalLockKey)) {
          try {
            if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: 'Ticket creation already in progress — please finish the open modal or wait a moment.', flags: 64 });
            else await interaction.followUp({ content: 'Ticket creation already in progress — please finish the open modal or wait a moment.', flags: 64 });
          } catch (e) {}
          return;
        }

        // set modal lock & TTL
        ticketModalLocks.add(modalLockKey);
        if (ticketModalTimers.has(modalLockKey)) clearTimeout(ticketModalTimers.get(modalLockKey));
        ticketModalTimers.set(modalLockKey, setTimeout(() => {
          ticketModalLocks.delete(modalLockKey);
          ticketModalTimers.delete(modalLockKey);
        }, TICKET_MODAL_LOCK_TTL_MS));

        // build modal
        const modal = new ModalBuilder()
          .setCustomId(`ticket_modal_${interaction.user.id}_${Date.now()}`)
          .setTitle('Open a support ticket');

        const subjectInput = new TextInputBuilder().setCustomId('ticket_subject').setLabel('Subject').setStyle(TextInputStyle.Short).setRequired(true).setMaxLength(100);
        const messageInput = new TextInputBuilder().setCustomId('ticket_message').setLabel('Message').setStyle(TextInputStyle.Paragraph).setRequired(true).setMaxLength(4000);

        modal.addComponents(new ActionRowBuilder().addComponents(subjectInput), new ActionRowBuilder().addComponents(messageInput));

        try {
          await interaction.showModal(modal);
        } catch (err) {
          // release lock
          ticketModalLocks.delete(modalLockKey);
          if (ticketModalTimers.has(modalLockKey)) { clearTimeout(ticketModalTimers.get(modalLockKey)); ticketModalTimers.delete(modalLockKey); }
          await logDetailed(interaction.guild, { event: 'interaction.modal_failed', reason: 'showModal failed', note: err?.message || err });
          try {
            if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: 'Could not open the ticket modal (interaction expired). Please try the "Open Ticket" button again or DM staff.', flags: 64 });
            else await interaction.followUp({ content: 'Could not open ticket modal. Please try again later.', flags: 64 });
          } catch (e) {}
        }
        return;
      }

      if (customId.startsWith('ticket_claim_')) {
        const channelId = customId.split('ticket_claim_')[1];
        await claimTicket(interaction, channelId);
        return;
      }
      if (customId.startsWith('ticket_close_')) {
        const channelId = customId.split('ticket_close_')[1];
        await closeTicket(interaction, channelId, interaction.member);
        return;
      }
    }

    if (interaction.isModalSubmit()) {
      const id = interaction.customId || '';
      if (id.startsWith('ticket_modal_')) {
        // Defer reply if we can: use flags instead of ephemeral option
        try {
          if (!interaction.deferred && !interaction.replied) await interaction.deferReply({ flags: 64 });
        } catch (e) {
          await logDetailed(interaction.guild, { event: 'deferReply.failed', note: e?.message || e });
        }

        // release the earlier modal lock
        try {
          const parts = id.split('_'); // ticket_modal_<userId>_<ts>
          const lockUserId = parts[2];
          const lockKey = `${interaction.guildId || interaction.guild.id}:${lockUserId}`;
          ticketModalLocks.delete(lockKey);
          if (ticketModalTimers.has(lockKey)) { clearTimeout(ticketModalTimers.get(lockKey)); ticketModalTimers.delete(lockKey); }
        } catch (e) {}

        const subject = interaction.fields.getTextInputValue('ticket_subject').trim();
        const messageBody = interaction.fields.getTextInputValue('ticket_message').trim();

        if (!subject || !messageBody) {
          try {
            if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: 'Subject and message are required.', flags: 64 });
            else await interaction.editReply({ content: 'Subject and message are required.' });
          } catch {}
          return;
        }

        let ticketChannel;
        try {
          ticketChannel = await createTicketChannel(interaction.guild, interaction.member, subject, messageBody);
        } catch (e) {
          try {
            if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: 'Failed to create ticket channel. Contact staff.', flags: 64 });
            else await interaction.editReply({ content: 'Failed to create ticket channel. Contact staff.' });
          } catch {}
          return;
        }

        try {
          if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: `Ticket created: <#${ticketChannel.id}>`, flags: 64 });
          else {
            try { await interaction.editReply({ content: `Ticket created: <#${ticketChannel.id}>` }); } catch (e) {
              try { await interaction.followUp({ content: `Ticket created: <#${ticketChannel.id}>`, flags: 64 }); } catch (_) {}
            }
          }
        } catch (e) {
          console.warn('Failed to confirm ticket creation reply:', e?.message || e);
        }
        return;
      }
    }
  } catch (err) {
    console.error('interaction error', err);
    try { if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: 'An error occurred handling your interaction.', flags: 64 }); else await interaction.followUp({ content: 'An error occurred handling your interaction.', flags: 64 }); } catch {}
  }
});

//
// ---------- MESSAGE MODERATION (spam, NSFW, OpenAI) ----------
//
const recentMessages = new Map();
const SPAM_WINDOW_MS = 10 * 1000;
const SPAM_THRESHOLD = 5;
const REPEAT_THRESHOLD = 3;
const AUTO_MUTE_MINUTES = 10;

function addRecentMessage(guildId, userId, content) {
  const key = `${guildId}:${userId}`;
  const now = Date.now();
  const arr = recentMessages.get(key) || [];
  arr.push({ content, ts: now });
  while (arr.length && now - arr[0].ts > SPAM_WINDOW_MS) arr.shift();
  recentMessages.set(key, arr);
  return arr;
}
function checkSpam(arr) {
  if (arr.length >= SPAM_THRESHOLD) return true;
  const freq = {};
  for (const m of arr) {
    freq[m.content] = (freq[m.content] || 0) + 1;
    if (freq[m.content] >= REPEAT_THRESHOLD) return true;
  }
  return false;
}

client.on('messageCreate', async (message) => {
  if (!message.guild || message.author.bot) return;
  const guild = message.guild;

  // Admin whitelist: skip moderation entirely for admin user (allowed to post anything)
  if (String(message.author.id) === String(ADMIN_USER_ID)) {
    await logDetailed(guild, { event: 'message.bypass_admin', messageId: message.id, authorId: message.author.id, authorTag: message.author.tag, channelId: message.channel.id, channelName: message.channel.name || '(unknown)', content: safeTruncate(message.content || '', 400) });
    return;
  }

  if (!logChannels.has(guild.id)) await setupGuild(guild).catch(()=>{});

  const quarantinedRole = guild.roles.cache.find((r) => r.name === QUARANTINED_ROLE_NAME);
  const isQuarantined = quarantinedRole && message.member && message.member.roles.cache.has(quarantinedRole.id);

  const baseLog = {
    event: 'message.process',
    messageId: message.id,
    authorId: message.author.id,
    authorTag: message.author.tag,
    userDisplayName: message.member ? message.member.displayName : message.author.username,
    channelId: message.channel.id,
    channelName: message.channel.name || '(unknown)',
    createdAt: message.createdAt?.toISOString?.() || null,
    content: message.content || '',
    quarantined: !!isQuarantined,
  };

  await logDetailed(guild, { ...baseLog, event: 'message.received', note: 'message received' });

  // AD CHANNEL BYPASS: allow advertising/league posts in configured ad channel
  try {
    if (String(message.channel.id) === String(AD_CHANNEL_ID)) {
      await logDetailed(guild, { ...baseLog, event: 'ad_channel.allowed', note: `Advertising allowed in channel ${AD_CHANNEL_ID}; skipping moderation.` });
      return; // full bypass: allow adverts and promotions in this channel
    }
  } catch (e) {
    await logDetailed(guild, { ...baseLog, event: 'ad_channel_handler_failed', note: e?.message || e });
  }

  //
  // Special-case: matchmaking channel strict formatting & deletion
  //
  try {
    if (String(message.channel.id) === String(MATCH_CHANNEL_ID)) {
      // Allow moderators / server managers to post freely
      const modRole = guild.roles.cache.find(r => r.name === MODERATOR_ROLE_NAME);
      const isMod = modRole ? (message.member && message.member.roles.cache.has(modRole.id)) : false;
      const isServerManager = message.member && message.member.permissions && message.member.permissions.has(PermissionsBitField.Flags.ManageGuild);
      if (isMod || isServerManager || String(message.author.id) === String(ADMIN_USER_ID)) {
        await logDetailed(guild, { ...baseLog, event: 'match_channel.bypass_mod', note: 'Moderator/manager/admin posted in matchmaking channel' });
        // allow
      } else {
        const txt = (message.content || '').trim();
        // 1) allowed prefix: starts with code/codes/code? or 5. or s.
        if (MATCH_ALLOWED_PREFIX_REGEX.test(txt)) {
          await logDetailed(guild, { ...baseLog, event: 'match_channel.allowed', note: 'Starts with allowed matchmaking prefix (code / 5 / s)' });
          // allow
        } else {
          // 2) try heuristic: LF5 / LFG / 5v5 / need X more etc.
          if (MATCH_HEURISTIC_REGEX.test(txt)) {
            await logDetailed(guild, { ...baseLog, event: 'match_channel.allowed_heuristic', note: 'Heuristic considered this matchmaking content' });
            // allow
          } else {
            // Not clearly a matchmaking post -> delete the message and DM the user
            try {
              await message.delete();
              await logDetailed(guild, { ...baseLog, action: 'deleted', reason: 'match_channel.non_matchmaking', note: 'Deleted non-matchmaking post in matchmaking channel' });
            } catch (e) {
              await logDetailed(guild, { ...baseLog, action: 'delete_failed', note: e?.message || e });
            }
            try {
              await sendDMWithRetries(message.author, {
                content:
`Hi <@${message.author.id}>, your message in the 5v5 matchmaking channel was removed.
This channel is reserved for 5v5 matchup posts. Please start messages with one of these formats:
• \`Code\`, \`codes\`, or \`code?\` followed by the lobby code
• \`5.\` or starting with \`5\`
• \`s\` (single-letter prefix like \`s.\`)
Or clearly state you're looking for a 5v5 (examples: "LF5", "Looking for 5v5", "Need 4 more"). If you believe this was removed in error, contact a moderator.`
              });
            } catch (e) {
              await logDetailed(guild, { ...baseLog, event: 'dm_failed', note: e?.message || e });
            }
            return; // don't continue other moderation for this deleted message
          }
        }
      }
    }
  } catch (e) {
    await logDetailed(guild, { ...baseLog, event: 'match_channel_handler_failed', note: e?.message || e });
    // fallthrough to regular moderation if match channel handler errors
  }

  // If user is quarantined: messages are allowed to be posted but we MUST run OpenAI check on them.
  // If flagged -> delete message. For strikes: use verification (lenient default). We DO NOT auto-perm-mute in quarantine by default.
  if (isQuarantined) {
    const ts = getQuarantineTimestamp(guild.id, message.author.id) || Date.now();
    const age = Date.now() - ts;
    const SEVENTY_TWO_HOURS = 72 * 60 * 60 * 1000;

    if (age < SEVENTY_TWO_HOURS) {
      await logDetailed(guild, { ...baseLog, event: 'quarantine.monitor', reason: 'Monitoring quarantined user message (72h window)' });
      const cached = cacheGet(message.content || '');
      if (cached !== null) {
        // If cache says hate but only allowlisted words present -> treat OK
        if (cached.flagged && cached.category === 'FLAG_HATE') {
          const low = (message.content || '').toLowerCase();
          const hasAllow = HATE_ALLOWLIST.some(w => low.includes(w));
          const hasSevere = HATE_REGEX.test(low);
          if (hasAllow && !hasSevere) {
            await logDetailed(guild, { ...baseLog, event: 'hate_allowlist_quarantine', note: 'Message contained allowlisted words; not enforcing hate flag' });
            // do nothing
            return;
          }
        }

        if (cached.flagged) {
          try { await message.delete(); } catch (e) { await logDetailed(guild, { ...baseLog, action: 'delete_failed', note: e?.message || e }); }
          // run verification before creating a strike (decideAndApplyStrike uses lenient fallback)
          await decideAndApplyStrike(guild, message, cached.category, { ...baseLog, event: 'quarantine.auto_flag_cache' });
          await logDetailed(guild, { ...baseLog, action: 'deleted', reason: `cache_${cached.category}`, category: cached.category });
          try { await message.author.send({ content: `We suspect your account is a bot or violating rules and have taken action: ${cached.category}` }); } catch {}
          // Do NOT permanently mute by default for quarantine auto-flagging; strikes are applied only if verification decides so.
        }
      } else {
        if (allowOpenAICall()) {
          try {
            const res = await performOpenAICall(message.content || '');
            await logDetailed(guild, { ...baseLog, event: 'openai.call.end', reason: 'Quarantine monitoring', resultCategory: res.category });

            // Hate allowlist check for quarantine
            if (res.flagged && res.category === 'FLAG_HATE') {
              const low = (message.content || '').toLowerCase();
              const hasAllow = HATE_ALLOWLIST.some(w => low.includes(w));
              const hasSevere = HATE_REGEX.test(low);
              if (hasAllow && !hasSevere) {
                await logDetailed(guild, { ...baseLog, event: 'hate_allowlist_quarantine', note: 'OpenAI flagged hate but message contains allowlisted words only; skipping enforcement' });
                // treat as OK (do nothing further)
                return;
              }
            }

            if (res.flagged) {
              try { await message.delete(); } catch (e) { await logDetailed(guild, { ...baseLog, action: 'delete_failed', note: e?.message || e }); }
              await decideAndApplyStrike(guild, message, res.category, { ...baseLog, event: 'quarantine.auto_flag_openai' });
              await logDetailed(guild, { ...baseLog, action: 'deleted', reason: res.category, category: res.category, note: 'Deleted flagged message by AI during quarantine' });
              try { await message.author.send({ content: `We suspect your account is a bot or violating rules and have taken action: ${res.category}` }); } catch {}
            }
          } catch (e) {
            await logDetailed(guild, { ...baseLog, event: 'quarantine.openai_error', note: e?.message || e });
          }
        } else {
          await logDetailed(guild, { ...baseLog, event: 'openai.rate_limited', reason: 'OpenAI rate limit - quarantine monitor skipped' });
        }
      }
    } else {
      try {
        if (message.member.roles.cache.has(quarantinedRole.id)) {
          await message.member.roles.remove(quarantinedRole, 'Quarantine expired (real-time check)');
          clearQuarantineTimestamp(guild.id, message.author.id);
          await logDetailed(guild, { event: 'role.remove', roleName: quarantinedRole.name, authorId: message.author.id, reason: 'Quarantine expired (message triggered)' });
        }
      } catch (e) { await logDetailed(guild, { event: 'role.remove_failed', note: e?.message || e }); }
    }
    return;
  }

  const ch = message.channel;
  const isForumChannel = ch.type === ChannelType.GuildForum;
  const isThread = typeof ch.isThread === 'function' ? ch.isThread() : (ch.type === ChannelType.PublicThread || ch.type === ChannelType.PrivateThread || ch.type === ChannelType.AnnouncementThread);

  // Spam/flood detection: delete & immediate mute; create strike only if NOT in ad channel (we already bypass AD_CHANNEL at top),
  // and still keep behavior strict for real spam (multiple repeats). This avoids striking promotional posts in AD channel.
  if (!isForumChannel && !isThread) {
    const arr = addRecentMessage(guild.id, message.author.id, message.content || '<embed/attachment>');
    if (checkSpam(arr)) {
      try { await message.delete(); } catch (e) { await logDetailed(guild, { ...baseLog, action: 'delete_failed', note: e?.message || e }); }
      await logDetailed(guild, { ...baseLog, action: 'deleted', reason: 'spam_detection' });
      await muteMember(message.member, AUTO_MUTE_MINUTES, 'Auto-mute: spam/flooding');

      try {
        // spam detection uses deterministic heuristics -> apply strike immediately (no verification)
        // However: be cautious and only create strike in normal channels (ad channel bypass handled earlier)
        await createStrike({ guildId: guild.id, userId: message.author.id, category: 'FLAG_SPAM', reason: 'spam_detection', messageId: message.id, actorId: 'bot' });
        const count = await countActiveStrikes(guild.id, message.author.id);
        await logDetailed(guild, { event: 'strike.recorded', authorId: message.author.id, note: `Strikes now ${count}` });
        if (count >= PERM_MUTE_THRESHOLD) await muteMemberForever(message.member, `Reached ${count} strikes (permanent mute)`);
      } catch (e) { await logDetailed(guild, { event: 'strike.error', note: e?.message || e }); }
      return;
    }
  } else {
    await logDetailed(guild, { ...baseLog, event: 'spam_check.skipped', reason: 'forum_or_thread' });
  }

  const hasAttachment = message.attachments && message.attachments.size > 0;
  const nsfwPrefilter = NSFW_REGEX.test(message.content || '') || (hasAttachment && SPAM_LINKS_REGEX.test(Array.from(message.attachments.values()).map(a => a.url).join(' ')));
  const hatePrefilter = HATE_REGEX.test(message.content || '');
  const bettingPrefilter = BETTING_SIGNAL_REGEX.test(message.content || '') || BETTING_LIKELY_REGEX.test(message.content || '');

  const cached = cacheGet(message.content || '');
  if (cached !== null) {
    await logDetailed(guild, { ...baseLog, event: 'cache.hit', reason: 'cache hit', category: cached.category, flagged: cached.flagged });

    // If cache reasons indicate hate but content is in allowlist only -> do not delete
    if (cached.flagged && cached.category === 'FLAG_HATE') {
      const low = (message.content || '').toLowerCase();
      const hasAllow = HATE_ALLOWLIST.some(w => low.includes(w));
      const hasSevere = HATE_REGEX.test(low);
      if (hasAllow && !hasSevere) {
        await logDetailed(guild, { ...baseLog, event: 'hate_allowlist_cache', note: 'Cached hate flag but message contains allowlisted words only; skipping delete' });
        return;
      }
    }

    if (cached.flagged) {
      try { await message.delete(); await logDetailed(guild, { ...baseLog, action: 'deleted', reason: `cache_${cached.category}`, category: cached.category }); } catch (e) { await logDetailed(guild, { ...baseLog, action: 'delete_failed', note: e?.message || e }); }
      try { await sendDMWithRetries(message.author, { content: CONTENT_DELETED_MESSAGE.replace('{member}', `<@${message.author.id}>`) }); } catch (e) { await logDetailed(guild, { ...baseLog, action: 'dm_failed', note: e?.message || e }); }
      // decide whether to apply strike based on verification (lenient fallback)
      await decideAndApplyStrike(guild, message, cached.category, { ...baseLog, event: 'cache.flagged' });
    }
    return;
  }

  // Use the improved shouldCheckByPolicy function that incorporates affirmation guard & numeric checks
  const shouldCheck = shouldCheckByPolicy(message.content || '', message.channel.type);
  let reason;
  if (!shouldCheck) {
    reason = isForumChannel ? 'skipped_by_sampling_forum' : 'skipped_by_sampling';
  } else {
    reason = (bettingPrefilter || nsfwPrefilter || hatePrefilter || SPAM_LINKS_REGEX.test(message.content || '')) ? 'keyword_matched' : 'sampled';
  }
  await logDetailed(guild, { ...baseLog, event: 'precheck', reason });

  if (reason === 'skipped_by_sampling' || reason === 'skipped_by_sampling_forum') return;
  if (!openai) { await logDetailed(guild, { ...baseLog, event: 'openai.missing', reason: 'OpenAI missing - skip moderation' }); return; }

  if (!allowOpenAICall()) { await logDetailed(guild, { ...baseLog, event: 'rate_limit', reason: 'Rate limit reached - skip moderation' }); return; }

  try {
    let result = { flagged: false, category: 'OK' };

    if (activeCalls < CONCURRENCY) {
      activeCalls += 1;
      await logDetailed(guild, { ...baseLog, event: 'openai.call.start', reason: 'Starting OpenAI call', model: MODEL });
      try {
        result = await performOpenAICall(message.content || '');
      } finally {
        activeCalls -= 1;
        processQueue();
      }
      await logDetailed(guild, { ...baseLog, event: 'openai.call.end', reason: 'OpenAI call finished', resultCategory: result.category });
    } else {
      await logDetailed(guild, { ...baseLog, event: 'openai.enqueue', reason: 'Enqueuing OpenAI call', queueLength: pendingQueue.length });
      result = await enqueueOpenAICall(message.content || '');
      await logDetailed(guild, { ...baseLog, event: 'openai.enqueue_result', reason: 'Enqueue result', resultCategory: result.category });
    }

    // Apply hate allowlist safeguard: if classifier says FLAG_HATE but message contains only allowlisted tokens and no severe slur -> treat as OK
    if (result.flagged && result.category === 'FLAG_HATE') {
      const low = (message.content || '').toLowerCase();
      const hasAllow = HATE_ALLOWLIST.some(w => low.includes(w));
      const hasSevere = HATE_REGEX.test(low);
      if (hasAllow && !hasSevere) {
        await logDetailed(guild, { ...baseLog, event: 'hate_allowlist', note: 'OpenAI flagged hate but message contains allowlisted words only; downgrading to OK' });
        // treat as OK
        result.flagged = false;
        result.category = 'OK';
      }
    }

    if (result.flagged) {
      try {
        await message.delete();
        await logDetailed(guild, { ...baseLog, action: 'deleted', reason: result.category, category: result.category, note: 'Deleted flagged message by AI' });
      } catch (e) { await logDetailed(guild, { ...baseLog, action: 'delete_failed', note: e?.message || e }); }

      try { await sendDMWithRetries(message.author, { content: CONTENT_DELETED_MESSAGE.replace('{member}', `<@${message.author.id}>`) }); } catch (e) { await logDetailed(guild, { ...baseLog, action: 'dm_failed', note: e?.message || e }); }

      // NEW: verify with OpenAI whether to record a strike for this flagged message (lenient default)
      await decideAndApplyStrike(guild, message, result.category, { ...baseLog, event: 'auto_flag_openai' });

      return;
    } else {
      await logDetailed(guild, { ...baseLog, event: 'message.allowed', reason: 'Message allowed by moderation' });
    }
  } catch (err) {
    await logDetailed(guild, { ...baseLog, event: 'moderation_error', note: err?.message || err });
  }
});

//
// ---------- START ----------
//
client.login(DISCORD_BOT_TOKEN).catch((e) => {
  console.error('Login failed:', e?.message || e);
});
