// db.js - Prisma wrapper for Supabase/Postgres
// Requires: npm install @prisma/client prisma
import { PrismaClient } from '@prisma/client';

let prisma = null;
export function initDb() {
  if (!prisma) prisma = new PrismaClient();
  return prisma;
}

export async function getGuildConfig(guildId) {
  const db = initDb();
  return await db.guildConfig.findUnique({ where: { guildId } });
}

export async function upsertGuildConfig(guildId, data = {}) {
  const db = initDb();
  return await db.guildConfig.upsert({
    where: { guildId },
    create: { guildId, panelChannelId: data.panelChannelId ?? null, ticketCategoryId: data.ticketCategoryId ?? null },
    update: { panelChannelId: data.panelChannelId ?? undefined, ticketCategoryId: data.ticketCategoryId ?? undefined },
  });
}

export async function createTicket({ channelId, guildId, ownerId, subject, meta = null }) {
  const db = initDb();
  return await db.ticket.create({
    data: { channelId, guildId, ownerId, subject, meta },
  });
}

export async function getTicket(channelId) {
  const db = initDb();
  return await db.ticket.findUnique({ where: { channelId } });
}

export async function claimTicket(channelId, claimerId) {
  const db = initDb();
  return await db.ticket.update({
    where: { channelId },
    data: { claimerId },
  });
}

export async function closeTicket(channelId) {
  const db = initDb();
  const t = await db.ticket.findUnique({ where: { channelId } });
  if (!t) return null;
  const closed = await db.ticket.update({
    where: { channelId },
    data: { closedAt: new Date() },
  });
  // Optionally keep closed row or move to archive; for now we keep the row and set closedAt.
  return closed;
}

export async function listOpenTickets(guildId) {
  const db = initDb();
  return await db.ticket.findMany({ where: { guildId, closedAt: null }, orderBy: { createdAt: 'asc' } });
}
