#!/usr/bin/env node
/**
 * Full bot.js — Node.js (ESM)
 * - Uses Discord.js v14
 * - Uses OpenAI for moderation (optional)
 * - Persists guild config, tickets, and strikes to Postgres via Prisma
 * - Permanently mutes users when strike threshold met
 *
 * Requirements:
 *   npm install discord.js openai dotenv @prisma/client prisma
 *   npx prisma generate
 *   npx prisma db push
 *
 * Env required:
 *   DATABASE_URL, DISCORD_BOT_TOKEN
 * Optional:
 *   OPENAI_API_KEY
 *   PERM_MUTE_THRESHOLD (default 5)
 *   STRIKE_DECAY_DAYS (default 30)
 *   SAMPLE_RATE, MAX_OPENAI_PER_MIN, CONCURRENCY, CACHE_TTL_MS, MODEL
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
const ACCOUNT_AGE_LIMIT_DAYS = parseInt(process.env.ACCOUNT_AGE_LIMIT_DAYS || '7', 10);

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

const MODERATION_SYSTEM_PROMPT = `You are a content moderation assistant. Decide whether the user's message contains:
- hateful content (racism, slurs, targeted harassment) -> return "FLAG_HATE"
- sports betting/picks/gambling content -> return "FLAG_BET"
- explicit sexual/NSFW content (pornographic) -> return "FLAG_NSFW"
- spam/flooding (repetitive messages, solicitations, mass-links) -> return "FLAG_SPAM"
- none of the above -> return "OK"

Return exactly one token (FLAG_HATE / FLAG_NSFW / FLAG_BET / FLAG_SPAM / OK). If multiple categories apply, prioritize FLAG_HATE, then FLAG_NSFW, then FLAG_BET, then FLAG_SPAM.`;

// betting/picks keywords
const BETTING_KEYWORDS = [
  'bet','bets','wager','wagers','gamble','gambles','sportsbook','parlay','parlays','odds','bookie','bookies','betting',
  'pick','picks','pickem','pick-em','pick em','tip','tips','tipster','sharp','sharpie','juice','edge','prop bet','propbet',
  'banker','treble','lay','back','spread','moneyline','money line','ml','ou','o/u','over under','ats','pk','multibet','bookmaker'
];
const BETTING_REGEX = new RegExp(`\\b(${BETTING_KEYWORDS.map(k => k.replace(/[.*+?^${}()|[\\]\\\\]/g,'\\$&')).join('|')})\\b`, 'i');

const NSFW_KEYWORDS = [
  'porn','pornography','xxx','nsfw','camgirl','cams','nude','nudes','sex','pornhub','xvideos','xhamster','adult','explicit','breast','cock','pussy'
];
const NSFW_REGEX = new RegExp(`\\b(${NSFW_KEYWORDS.map(k => k.replace(/[.*+?^${}()|[\\]\\\\]/g,'\\$&')).join('|')})\\b`, 'i');

const SPAM_LINKS_REGEX = /\bhttps?:\/\/\S+\b/i;

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
    const res = await openai.chat.completions.create({
      model: MODEL,
      messages: [
        { role: 'system', content: MODERATION_SYSTEM_PROMPT },
        { role: 'user', content },
      ],
      max_tokens: 1,
      temperature: 0,
    });
    const raw = res.choices?.[0]?.message?.content?.trim?.() || '';
    const out = raw.toUpperCase();

    let category = 'OK';
    if (out.includes('FLAG_HATE')) category = 'FLAG_HATE';
    else if (out.includes('FLAG_NSFW')) category = 'FLAG_NSFW';
    else if (out.includes('FLAG_BET')) category = 'FLAG_BET';
    else if (out.includes('FLAG_SPAM')) category = 'FLAG_SPAM';
    else if (out.includes('OK')) category = 'OK';
    else {
      console.warn('Unexpected moderation output:', raw);
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

function shouldCheckByPolicy(content) {
  if (BETTING_REGEX.test(content)) return true;
  if (NSFW_REGEX.test(content)) return true;
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
  if (category === 'FLAG_NSFW' || category === 'FLAG_BET' || event.startsWith('ticket.') || event.includes('spam')) return 'medium';
  return 'low';
}

async function logDetailed(guild, obj) {
  try {
    const severity = computeSeverity(obj);
    // Only log medium+ severity for routine message events.
    // Still allow logging for important non-message events like errors, setup, ticket flows, role/channel creation, strikes, etc.
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
      // skip noisy low-severity logs (this prevents logging every message.received / precheck etc)
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

    if (obj.content) embed.setDescription(safeTruncate(String(obj.content), 1024));
    if (obj.authorTag) embed.setFooter({ text: `Tag: ${obj.authorTag}` });

    const ch = guild ? logChannels.get(guild.id) : null;
    if (ch) {
      // send embed, but guard against send errors
      try { await ch.send({ embeds: [embed] }); } catch (e) { /* ignore send errors to avoid recursive logs */ }
    }

    // Keep a concise console JSON for medium+ and also for allowed low-severity important events.
    if (severity !== 'low' || allowDespiteLow) {
      try {
        console.log(JSON.stringify({ ts: new Date().toISOString(), ...obj }, null, 2));
      } catch (e) {
        // no-op
      }
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
    // Best-effort: deny SendMessages in channels where bot can manage perms
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
// ---------- GUILD SETUP & TICKETS (panel) ----------
//
const TICKET_PANEL_CUSTOM_ID = 'create_ticket_button_v1';

function ticketChannelName(member, subject) {
  const base = (member.displayName || member.user.username || 'ticket').toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').slice(0, 20);
  const subj = (subject || 'ticket').toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').slice(0, 20);
  const suffix = `${Date.now() % 10000}`;
  return `ticket-${base}-${subj}-${suffix}`;
}

async function ensureTicketPanel(guild) {
  try {
    let cfg = await getGuildConfig(guild.id);
    // ensure a ticket category exists and the panel exists
    if (!cfg) {
      // create category & panel
      const ticketCategory = await guild.channels.create({ name: DEFAULT_TICKET_CATEGORY_NAME, type: ChannelType.GuildCategory });
      const panel = await guild.channels.create({ name: DEFAULT_PANEL_CHANNEL_NAME, type: ChannelType.GuildText, parent: ticketCategory.id });
      await upsertGuildConfig(guild.id, { panelChannelId: panel.id, ticketCategoryId: ticketCategory.id });
      cfg = await getGuildConfig(guild.id);
      await logDetailed(guild, { event: 'setup.guild_config', note: 'Default ticket panel & category created and saved' });
    }
    // send panel message if missing
    const panelId = cfg.panelChannelId;
    const panel = panelId ? await guild.channels.fetch(panelId).catch(() => null) : null;
    if (panel && panel.isText()) {
      const msgs = await panel.messages.fetch({ limit: 50 }).catch(() => null);
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
    await logDetailed(guild, { event: 'ticket.panel_error', note: e?.message || e });
  }
}

async function createTicketChannel(guild, member, subject, messageBody) {
  try {
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

    await createTicketRow({ channelId: channel.id, guildId: guild.id, ownerId: member.id, subject, meta: { createdBy: member.user.tag } });

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

  // Reply to interaction BEFORE deleting the channel
  const replyText = 'Ticket closed and transcript saved to moderation log.';
  try {
    if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: replyText, flags: 64 });
    else await interaction.followUp({ content: replyText, flags: 64 });
  } catch (e) { console.warn('Failed reply pre-delete', e?.message || e); }

  // Mark ticket closed in DB
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
// ---------- EVENTS ----------
//
client.once('ready', async () => {
  console.log(`Logged in as ${client.user.tag}`);
  for (const g of client.guilds.cache.values()) {
    await setupGuild(g);
  }
});

client.on('guildCreate', async (guild) => {
  await setupGuild(guild);
});

client.on('guildMemberAdd', async (member) => {
  const guild = member.guild;
  await setupGuild(guild).catch(() => {});
  const quarantinedRole = guild.roles.cache.find(r => r.name === QUARANTINED_ROLE_NAME);

  try {
    await member.send({ content: GREETING_MESSAGE.replace('{member}', `<@${member.id}>`) });
    await logDetailed(guild, { event: 'dm.sent', reason: 'Greeting DM sent', authorId: member.id, authorTag: member.user.tag, userDisplayName: member.displayName });
  } catch (e) {
    await logDetailed(guild, { event: 'dm.failed', reason: 'Greeting DM failed', authorId: member.id, authorTag: member.user.tag, userDisplayName: member.displayName, note: e?.message || e });
  }

  if (isNewAccount(member.user) && quarantinedRole) {
    try {
      await member.roles.add(quarantinedRole, 'Account under age limit');
      await logDetailed(guild, { event: 'role.assign.quarantine', roleName: quarantinedRole.name, reason: `${QUARANTINED_ROLE_NAME} assigned on join`, authorId: member.id, authorTag: member.user.tag, userDisplayName: member.displayName });
    } catch (e) {
      await logDetailed(guild, { event: 'role.assign_failed', roleName: quarantinedRole ? quarantinedRole.name : QUARANTINED_ROLE_NAME, note: e?.message || e });
    }
  }
});

client.on('interactionCreate', async (interaction) => {
  try {
    if (interaction.isButton()) {
      const customId = interaction.customId;

      if (customId === TICKET_PANEL_CUSTOM_ID) {
        const modal = new ModalBuilder()
          .setCustomId(`ticket_modal_${interaction.user.id}_${Date.now()}`)
          .setTitle('Open a support ticket');

        const subjectInput = new TextInputBuilder().setCustomId('ticket_subject').setLabel('Subject').setStyle(TextInputStyle.Short).setRequired(true).setMaxLength(100);
        const messageInput = new TextInputBuilder().setCustomId('ticket_message').setLabel('Message').setStyle(TextInputStyle.Paragraph).setRequired(true).setMaxLength(4000);

        modal.addComponents(new ActionRowBuilder().addComponents(subjectInput), new ActionRowBuilder().addComponents(messageInput));
        await interaction.showModal(modal);
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
        // Defer reply immediately to avoid modal UI error
        try { await interaction.deferReply({ flags: 64 }); } catch (e) { console.warn('deferReply failed:', e?.message || e); }

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
          else await interaction.editReply({ content: `Ticket created: <#${ticketChannel.id}>` });
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

  if (isQuarantined) {
    try { await message.delete(); await logDetailed(guild, { ...baseLog, action: 'deleted', reason: 'quarantined_user' }); } catch (e) { await logDetailed(guild, { ...baseLog, action: 'delete_failed', note: e?.message || e }); }
    try { await message.author.send({ content: ACCOUNT_TOO_NEW_MESSAGE.replace('{member}', `<@${message.author.id}>`) }); } catch (e) { await logDetailed(guild, { ...baseLog, action: 'dm_failed', note: e?.message || e }); }
    if (!isNewAccount(message.author) && message.member.roles.cache.has(quarantinedRole.id)) {
      try { await message.member.roles.remove(quarantinedRole, 'Account aged past limit (runtime check)'); await logDetailed(guild, { event: 'role.remove', roleName: quarantinedRole.name, authorId: message.author.id }); } catch (e) { await logDetailed(guild, { event: 'role.remove_failed', note: e?.message || e }); }
    }
    return;
  }

  // spam checks
  const arr = addRecentMessage(guild.id, message.author.id, message.content || '<embed/attachment>');
  if (checkSpam(arr)) {
    try { await message.delete(); } catch (e) { await logDetailed(guild, { ...baseLog, action: 'delete_failed', note: e?.message || e }); }
    await logDetailed(guild, { ...baseLog, action: 'deleted', reason: 'spam_detection' });
    await muteMember(message.member, AUTO_MUTE_MINUTES, 'Auto-mute: spam/flooding');
    // create strike for spam
    try {
      await createStrike({ guildId: guild.id, userId: message.author.id, category: 'FLAG_SPAM', reason: 'spam_detection', messageId: message.id, actorId: 'bot' });
      const count = await countActiveStrikes(guild.id, message.author.id);
      await logDetailed(guild, { event: 'strike.recorded', authorId: message.author.id, note: `Strikes now ${count}` });
      if (count >= PERM_MUTE_THRESHOLD) {
        await muteMemberForever(message.member, `Reached ${count} strikes (permanent mute)`);
      }
    } catch (e) { await logDetailed(guild, { event: 'strike.error', note: e?.message || e }); }
    return;
  }

  // quick NSFW prefilter
  const hasAttachment = message.attachments && message.attachments.size > 0;
  const nsfwPrefilter = NSFW_REGEX.test(message.content || '') || (hasAttachment && SPAM_LINKS_REGEX.test(Array.from(message.attachments.values()).map(a => a.url).join(' ')));

  const cached = cacheGet(message.content || '');
  if (cached !== null) {
    await logDetailed(guild, { ...baseLog, event: 'cache.hit', reason: 'cache hit', category: cached.category, flagged: cached.flagged });
    if (cached.flagged) {
      try { await message.delete(); await logDetailed(guild, { ...baseLog, action: 'deleted', reason: `cache_${cached.category}` }); } catch (e) { await logDetailed(guild, { ...baseLog, action: 'delete_failed', note: e?.message || e }); }
      try { await message.author.send({ content: CONTENT_DELETED_MESSAGE.replace('{member}', `<@${message.author.id}>`) }); } catch (e) { await logDetailed(guild, { ...baseLog, action: 'dm_failed', note: e?.message || e }); }
    }
    return;
  }

  const reason = (BETTING_REGEX.test(message.content) || nsfwPrefilter || SPAM_LINKS_REGEX.test(message.content)) ? 'keyword_matched' : (Math.random() < SAMPLE_RATE ? 'sampled' : 'skipped_by_sampling');
  await logDetailed(guild, { ...baseLog, event: 'precheck', reason });

  if (reason === 'skipped_by_sampling') return;
  if (!openai) { await logDetailed(guild, { ...baseLog, event: 'openai.missing', reason: 'OpenAI missing - skip moderation' }); return; }

  if (!allowOpenAICall()) { await logDetailed(guild, { ...baseLog, event: 'rate_limit', reason: 'Rate limit reached - skip moderation' }); return; }

  try {
    let result = { flagged: false, category: 'OK' };
    if (activeCalls < CONCURRENCY) {
      activeCalls += 1;
      await logDetailed(guild, { ...baseLog, event: 'openai.call.start', reason: 'Starting OpenAI call', model: MODEL });
      result = await performOpenAICall(message.content || '');
      activeCalls -= 1;
      processQueue();
      await logDetailed(guild, { ...baseLog, event: 'openai.call.end', reason: 'OpenAI call finished', resultCategory: result.category });
    } else {
      await logDetailed(guild, { ...baseLog, event: 'openai.enqueue', reason: 'Enqueuing OpenAI call', queueLength: pendingQueue.length });
      result = await enqueueOpenAICall(message.content || '');
      await logDetailed(guild, { ...baseLog, event: 'openai.enqueue_result', reason: 'Enqueue result', resultCategory: result.category });
    }

    if (result.flagged) {
      try {
        await message.delete();
        await logDetailed(guild, { ...baseLog, action: 'deleted', reason: result.category, category: result.category, note: 'Deleted flagged message by AI' });
      } catch (e) { await logDetailed(guild, { ...baseLog, action: 'delete_failed', note: e?.message || e }); }

      try { await message.author.send({ content: CONTENT_DELETED_MESSAGE.replace('{member}', `<@${message.author.id}>`) }); } catch (e) { await logDetailed(guild, { ...baseLog, action: 'dm_failed', note: e?.message || e }); }

      // create strike
      try {
        await createStrike({ guildId: guild.id, userId: message.author.id, category: result.category, reason: 'auto-moderation - message flagged', messageId: message.id, actorId: 'bot' });
        const count = await countActiveStrikes(guild.id, message.author.id);
        await logDetailed(guild, { event: 'strike.recorded', authorId: message.author.id, note: `Strikes now ${count}` });
        if (count >= PERM_MUTE_THRESHOLD) {
          await muteMemberForever(message.member, `Reached ${count} strikes (permanent mute)`);
        } else {
          // progressive action for serious categories
          if (result.category === 'FLAG_HATE' || result.category === 'FLAG_NSFW') {
            await muteMember(message.member, AUTO_MUTE_MINUTES * 3, `Auto-mute: ${result.category}`);
          }
        }
      } catch (e) {
        await logDetailed(guild, { event: 'strike.error', note: e?.message || e });
      }
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
