const {initializeApp} = require('firebase-admin/app');
const {getFirestore, FieldValue} = require('firebase-admin/firestore');
const {onDocumentCreated} = require('firebase-functions/v2/firestore');
const {onCall, HttpsError} = require('firebase-functions/v2/https');
const {onSchedule} = require('firebase-functions/v2/scheduler');

initializeApp();
const db = getFirestore();
// Rate limiting: track message counts per user per minute
const RATE_LIMIT_WINDOW_MS = 60000; // 1 minute
const RATE_LIMIT_MAX_MESSAGES = 30; // max messages per minute
const ORPHAN_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
const CLEANUP_BATCH_SIZE = 450;

function serverIdFromChannel(channel) {
  const match = typeof channel === 'string' && channel.match(/^srv_([^_]+)_/);
  return match ? match[1] : null;
}

function isOlderThanCutoff(value, cutoff) {
  if (!value) return false;
  const date = typeof value.toDate === 'function' ? value.toDate() : new Date(value);
  return !Number.isNaN(date.getTime()) && date < cutoff;
}

async function deleteRefs(refs) {
  let deleted = 0;
  for (let i = 0; i < refs.length; i += CLEANUP_BATCH_SIZE) {
    const batch = db.batch();
    refs.slice(i, i + CLEANUP_BATCH_SIZE).forEach(ref => batch.delete(ref));
    await batch.commit();
    deleted += Math.min(CLEANUP_BATCH_SIZE, refs.length - i);
  }
  return deleted;
}

async function findOldOrphanedDocs(collectionName, timestampField, serverIds, cutoff) {
  const snapshot = await db.collection(collectionName)
    .where(timestampField, '<', cutoff)
    .get();
  return snapshot.docs.filter(doc => {
    const data = doc.data();
    const serverId = data.serverId || serverIdFromChannel(data.channel);
    return serverId && !serverIds.has(serverId) && isOlderThanCutoff(data[timestampField], cutoff);
  });
}

// Remove stale records left behind when a server is deleted. This intentionally
// waits 30 days so deleted-server data remains recoverable for a short period.
exports.cleanupOrphanedData = onSchedule({
  schedule: 'every 24 hours',
  timeZone: 'UTC'
}, async () => {
  const cutoff = new Date(Date.now() - ORPHAN_RETENTION_MS);
  const serversSnapshot = await db.collection('servers').select().get();
  const serverIds = new Set(serversSnapshot.docs.map(doc => doc.id));
  const orphanedMessages = await findOldOrphanedDocs('messages', 'timestamp', serverIds, cutoff);
  const orphanedMessageIds = orphanedMessages.map(doc => doc.id);
  const refsToDelete = orphanedMessages.map(doc => doc.ref);

  const relatedCollections = [
    ['pins', 'timestamp'],
    ['events', 'timestamp'],
    ['auditLog', 'timestamp'],
    ['invites', 'createdAt'],
    ['webhooks', 'createdAt']
  ];
  for (const [collectionName, timestampField] of relatedCollections) {
    const docs = await findOldOrphanedDocs(collectionName, timestampField, serverIds, cutoff);
    refsToDelete.push(...docs.map(doc => doc.ref));
  }

  for (let i = 0; i < orphanedMessageIds.length; i += 30) {
    const ids = orphanedMessageIds.slice(i, i + 30);
    const chunks = await db.collection('messageChunks').where('messageId', 'in', ids).get();
    refsToDelete.push(...chunks.docs.map(doc => doc.ref));
    const history = await db.collection('messageHistory').where('messageId', 'in', ids).get();
    refsToDelete.push(...history.docs
      .filter(doc => isOlderThanCutoff(doc.data().timestamp, cutoff))
      .map(doc => doc.ref));
  }

  const deleted = await deleteRefs(refsToDelete);
  console.log(`Orphan cleanup removed ${deleted} records older than ${cutoff.toISOString()}`);
});

exports.validateMessage = onDocumentCreated('messages/{messageId}', async (event) => {
  const snap = event.data;
  if (!snap) return;
  const data = snap.data();
  const uid = data.uid;
  const user = data.user;
  const text = data.text || '';

  // Validate text length
  if (text.length > 4000) {
    await snap.ref.delete();
    console.log(`Message deleted: text too long (${text.length} chars) by ${user}`);
    return;
  }

  // Validate required fields
  if (!data.channel || !data.user) {
    await snap.ref.delete();
    console.log(`Message deleted: missing required fields by ${user}`);
    return;
  }
  const attachmentCount = (Array.isArray(data.images) ? data.images.length : 0)
    + (Array.isArray(data.files) ? data.files.length : 0)
    + (Array.isArray(data.attachments) ? data.attachments.length : 0);
  if (attachmentCount > 3 || (data.attachments && !Array.isArray(data.attachments))) {
    await snap.ref.delete();
    console.log(`Message deleted: too many attachments by ${user}`);
    return;
  }

  // Rate limiting
  if (uid) {
    const now = Date.now();
    const windowStart = now - RATE_LIMIT_WINDOW_MS;
    const rateLimitRef = db.collection('rateLimits').doc(uid);

    try {
      const rateDoc = await rateLimitRef.get();
      let timestamps = [];

      if (rateDoc.exists) {
        timestamps = rateDoc.data().timestamps || [];
        // Filter to only recent timestamps
        timestamps = timestamps.filter(t => t > windowStart);
      }

      timestamps.push(now);

      if (timestamps.length > RATE_LIMIT_MAX_MESSAGES) {
        await snap.ref.delete();
        console.log(`Message deleted: rate limit exceeded by ${user} (${timestamps.length} messages in window)`);
        return;
      }

      await rateLimitRef.set({ timestamps, lastUpdated: FieldValue.serverTimestamp() }, { merge: true });
    } catch (e) {
      console.error('Rate limit check failed', e);
    }
  }

  // Award badges based on activity
  if (user) {
    try {
      const userRef = db.collection('users').doc(user);
      const userDoc = await userRef.get();
      const badges = userDoc.exists ? (userDoc.data().badges || []) : [];

      // First message badge
      if (!badges.includes('first-message')) {
        badges.push('first-message');
        await userRef.set({ badges }, { merge: true });
        console.log(`Badge awarded: first-message to ${user}`);
      }

      // Count messages for "active chatter" badge
      const msgCount = await db.collection('messages').where('user', '==', user).count().get();
      if (msgCount.data().count >= 100 && !badges.includes('active-chatter')) {
        badges.push('active-chatter');
        await userRef.set({ badges }, { merge: true });
        console.log(`Badge awarded: active-chatter to ${user}`);
      }
    } catch (e) {
      console.error('Badge award failed', e);
    }
  }
});

// Server creation badge award
exports.onServerCreated = onDocumentCreated('servers/{serverId}', async (event) => {
  const snap = event.data;
  if (!snap) return;
  const data = snap.data();
  const owner = data.owner;

  if (owner) {
    try {
      const userRef = db.collection('users').doc(owner);
      const userDoc = await userRef.get();
      const badges = userDoc.exists ? (userDoc.data().badges || []) : [];

      if (!badges.includes('server-creator')) {
        badges.push('server-creator');
        await userRef.set({ badges }, { merge: true });
        console.log(`Badge awarded: server-creator to ${owner}`);
      }
    } catch (e) {
      console.error('Server creator badge failed', e);
    }
  }
});

// Friend request badge award
exports.onFriendAccepted = onDocumentCreated('friends/{friendId}', async (event) => {
  const snap = event.data;
  if (!snap) return;
  const data = snap.data();

  if (data.status === 'accepted') {
    const from = data.from;
    const to = data.to;

    for (const username of [from, to]) {
      try {
        const userRef = db.collection('users').doc(username);
        const userDoc = await userRef.get();
        const badges = userDoc.exists ? (userDoc.data().badges || []) : [];

        // Count friends
        const friendsSnap = await db.collection('friends')
          .where('status', '==', 'accepted')
          .get();
        const friendCount = friendsSnap.docs.filter(d => d.data().from === username || d.data().to === username).length;

        if (friendCount >= 10 && !badges.includes('popular')) {
          badges.push('popular');
          await userRef.set({ badges }, { merge: true });
          console.log(`Badge awarded: popular to ${username}`);
        }
      } catch (e) {
        console.error('Friend badge award failed', e);
      }
    }
  }
});

// Callable function to check rate limit status
exports.checkRateLimit = onCall(async (request) => {
  if (!request.auth) {
    throw new HttpsError('unauthenticated', 'Must be authenticated');
  }
  const uid = request.auth.uid;
  const rateLimitRef = db.collection('rateLimits').doc(uid);
  const rateDoc = await rateLimitRef.get();

  if (!rateDoc.exists) {
    return { remaining: RATE_LIMIT_MAX_MESSAGES, resetIn: 0 };
  }

  const timestamps = rateDoc.data().timestamps || [];
  const now = Date.now();
  const windowStart = now - RATE_LIMIT_WINDOW_MS;
  const recent = timestamps.filter(t => t > windowStart);

  return {
    remaining: Math.max(0, RATE_LIMIT_MAX_MESSAGES - recent.length),
    resetIn: recent.length > 0 ? RATE_LIMIT_WINDOW_MS - (now - recent[0]) : 0
  };
});
