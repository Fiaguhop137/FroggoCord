// Voice Chat - WebRTC audio using Firestore as signaling server
(function () {
  const { doc, getDoc, setDoc, deleteDoc, collection, query, where, getDocs, onSnapshot, serverTimestamp, updateDoc, addDoc } = window.froggocordFirebase;

  const escHtml = (s) => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');

  // State
  let currentVoiceChannel = null; // { serverId, channelName }
  let localStream = null;
  let peerConnections = {}; // username -> RTCPeerConnection
  let remoteAudioElements = {}; // username -> HTMLAudioElement
  let pendingIceCandidates = {}; // username -> RTCIceCandidateInit[]
  let peerSignalUnsubs = {}; // username -> unsubscribe function
  let isMuted = false;
  let participantUnsub = null;
  let signalUnsubs = [];
  let myUsername = null;

  // STUN servers for NAT traversal
  const ICE_SERVERS = {
    iceServers: [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' },
      { urls: 'stun:stun2.l.google.com:19302' },
    ]
  };

  // ---- Public API ----

  window.joinVoiceChannel = async (sid, cname) => {
    if (currentVoiceChannel) {
      await leaveVoiceChannel();
    }

    const s = (typeof getServerById === 'function') ? getServerById(sid) : null;
    if (!s) { appAlert('Server not found.'); return; }

    myUsername = (typeof user !== 'undefined') ? user : null;
    if (!myUsername) { appAlert('You must be signed in.'); return; }

    // Request microphone access
    try {
      localStream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
    } catch (e) {
      appAlert('Microphone access denied or unavailable: ' + (e.message || e));
      return;
    }

    currentVoiceChannel = { serverId: sid, channelName: cname };
    const channelId = sid + '_' + cname;

    // Register participant in Firestore
    try {
      await setDoc(doc(db, 'voiceParticipants', channelId + '__' + myUsername), {
        channelId,
        serverId: sid,
        channelName: cname,
        username: myUsername,
        uid: (auth.currentUser && auth.currentUser.uid) || null,
        muted: false,
        joinedAt: serverTimestamp()
      });
    } catch (e) {
      console.error('Failed to register voice participant:', e);
      appAlert('Failed to join channel: ' + (e.message || e));
      await leaveVoiceChannel();
      return;
    }

    // Listen for other participants
    startParticipantListener(channelId);

    // Show voice UI
    showVoiceUI(sid, cname);

    // Update channel rendering to show "connected" state
    if (typeof renderServerList === 'function') renderServerList();
  };

  window.leaveVoiceChannel = async () => {
    if (!currentVoiceChannel) return;

    const channelId = currentVoiceChannel.serverId + '_' + currentVoiceChannel.channelName;

    // Close all peer connections
    Object.keys(peerConnections).forEach(remoteUser => {
      try { peerConnections[remoteUser].close(); } catch (e) {}
      delete peerConnections[remoteUser];
    });
    Object.keys(peerSignalUnsubs).forEach(remoteUser => {
      try { peerSignalUnsubs[remoteUser](); } catch (e) {}
      delete peerSignalUnsubs[remoteUser];
    });
    pendingIceCandidates = {};

    // Remove remote audio elements
    Object.keys(remoteAudioElements).forEach(remoteUser => {
      try {
        remoteAudioElements[remoteUser].srcObject = null;
        remoteAudioElements[remoteUser].remove();
      } catch (e) {}
      delete remoteAudioElements[remoteUser];
    });

    // Stop local stream
    if (localStream) {
      localStream.getTracks().forEach(track => track.stop());
      localStream = null;
    }

    // Unsubscribe listeners
    if (participantUnsub) { participantUnsub(); participantUnsub = null; }
    signalUnsubs.forEach(unsub => { try { unsub(); } catch (e) {} });
    signalUnsubs = [];

    // Remove from Firestore
    try {
      await deleteDoc(doc(db, 'voiceParticipants', channelId + '__' + myUsername));
    } catch (e) { console.warn('Failed to remove voice participant:', e); }

    // Clean up signals
    try {
      const signalsSnap = await getDocs(query(collection(db, 'voiceSignals'), where('channelId', '==', channelId)));
      const ownSignals = signalsSnap.docs.filter(d => d.data().to === myUsername);
      await Promise.all(ownSignals.map(d => deleteDoc(d.ref)));
    } catch (e) {
      console.warn('Failed to remove voice signals:', e);
    }

    currentVoiceChannel = null;
    isMuted = false;

    // Hide voice UI
    hideVoiceUI();

    if (typeof renderServerList === 'function') renderServerList();
  };

  window.toggleVoiceMute = () => {
    isMuted = !isMuted;
    if (localStream) {
      localStream.getAudioTracks().forEach(track => { track.enabled = !isMuted; });
    }
    // Update Firestore
    if (currentVoiceChannel) {
      const channelId = currentVoiceChannel.serverId + '_' + currentVoiceChannel.channelName;
      updateDoc(doc(db, 'voiceParticipants', channelId + '__' + myUsername), { muted: isMuted }).catch(e => {});
    }
    // Update UI
    const muteBtn = document.getElementById('voice-mute-btn');
    if (muteBtn) {
      muteBtn.innerHTML = isMuted ? '<img src="/icons/mute.svg" alt="" aria-hidden="true"> Unmute' : '<img src="/icons/voice.svg" alt="" aria-hidden="true"> Mute';
      muteBtn.style.background = isMuted ? 'var(--danger)' : 'var(--bl)';
    }
  };

  // ---- Participant Listener ----

  function startParticipantListener(channelId) {
    const q = query(collection(db, 'voiceParticipants'), where('channelId', '==', channelId));
    participantUnsub = onSnapshot(q, (snap) => {
      const participants = snap.docs.map(d => d.data());

      // Find new participants to connect to
      participants.forEach(p => {
        if (p.username === myUsername) return;
        if (!peerConnections[p.username]) {
          // Use a stable initiator so both peers do not create competing offers.
          const isInitiator = myUsername < p.username;
          createPeerConnection(p.username, channelId, isInitiator);
        }
      });

      // Remove participants who left
      Object.keys(peerConnections).forEach(remoteUser => {
        if (!participants.find(p => p.username === remoteUser)) {
          closePeerConnection(remoteUser);
        }
      });

      // Update UI
      renderVoiceParticipants(participants);
    }, (err) => {
      console.error('Voice participant listener failed:', err);
    });
  }

  // ---- Peer Connection Management ----

  async function createPeerConnection(remoteUser, channelId, isInitiator) {
    if (peerConnections[remoteUser]) return;

    const pc = new RTCPeerConnection(ICE_SERVERS);
    peerConnections[remoteUser] = pc;

    // Add local audio tracks
    if (localStream) {
      localStream.getTracks().forEach(track => {
        pc.addTrack(track, localStream);
      });
    }

    // Handle incoming tracks
    pc.ontrack = (event) => {
      let audio = remoteAudioElements[remoteUser];
      if (!audio) {
        audio = document.createElement('audio');
        audio.autoplay = true;
        audio.playsInline = true;
        document.body.appendChild(audio);
        remoteAudioElements[remoteUser] = audio;
      }
      audio.srcObject = event.streams[0];
    };

    // Handle ICE candidates
    pc.onicecandidate = (event) => {
      if (event.candidate) {
        // Write ICE candidate to Firestore
        const candidateDoc = {
          channelId: channelId,
          from: myUsername,
          to: remoteUser,
          type: 'ice',
          candidate: JSON.stringify(event.candidate.toJSON()),
          timestamp: serverTimestamp()
        };
        addDoc(collection(db, 'voiceSignals'), candidateDoc).catch(e => console.warn('Failed to send ICE candidate:', e));
      }
    };

    // Handle connection state changes
    pc.onconnectionstatechange = () => {
      console.log('Peer connection state with', remoteUser, ':', pc.connectionState);
      if (pc.connectionState === 'disconnected' || pc.connectionState === 'failed') {
        closePeerConnection(remoteUser);
      }
    };

    // Listen for signals from this remote user
    // Filter the recipient in the client to avoid requiring a composite index.
    const signalQuery = query(collection(db, 'voiceSignals'), where('channelId', '==', channelId));
    const unsub = onSnapshot(signalQuery, async (snap) => {
      snap.docChanges().forEach(async (change) => {
        if (change.type !== 'added') return;
        const data = change.doc.data();
        if (data.to !== myUsername || data.from !== remoteUser) return;

        try {
          if (data.type === 'offer') {
            await pc.setRemoteDescription(new RTCSessionDescription({ type: 'offer', sdp: data.sdp }));
            await applyPendingIceCandidates(remoteUser, pc);
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            await addDoc(collection(db, 'voiceSignals'), {
              channelId: channelId,
              from: myUsername,
              to: remoteUser,
              type: 'answer',
              sdp: pc.localDescription.sdp,
              timestamp: serverTimestamp()
            });
          } else if (data.type === 'answer') {
            await pc.setRemoteDescription(new RTCSessionDescription({ type: 'answer', sdp: data.sdp }));
            await applyPendingIceCandidates(remoteUser, pc);
          } else if (data.type === 'ice') {
            const candidate = JSON.parse(data.candidate);
            if (pc.remoteDescription) {
              await pc.addIceCandidate(new RTCIceCandidate(candidate));
            } else {
              (pendingIceCandidates[remoteUser] ||= []).push(candidate);
            }
          }
        } catch (e) {
          console.error('Signal handling error:', e);
        }
      });
    });
    signalUnsubs.push(unsub);
    peerSignalUnsubs[remoteUser] = unsub;

    if (isInitiator) {
      // Create the offer after the signal listener is active.
      try {
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        await addDoc(collection(db, 'voiceSignals'), {
          channelId,
          from: myUsername,
          to: remoteUser,
          type: 'offer',
          sdp: pc.localDescription.sdp,
          timestamp: serverTimestamp()
        });
      } catch (e) {
        console.error('Failed to create offer:', e);
        closePeerConnection(remoteUser);
      }
    }
  }

  async function applyPendingIceCandidates(remoteUser, pc) {
    const candidates = pendingIceCandidates[remoteUser] || [];
    delete pendingIceCandidates[remoteUser];
    for (const candidate of candidates) {
      await pc.addIceCandidate(new RTCIceCandidate(candidate));
    }
  }

  function closePeerConnection(remoteUser) {
    if (peerSignalUnsubs[remoteUser]) {
      try { peerSignalUnsubs[remoteUser](); } catch (e) {}
      delete peerSignalUnsubs[remoteUser];
    }
    if (peerConnections[remoteUser]) {
      try { peerConnections[remoteUser].close(); } catch (e) {}
      delete peerConnections[remoteUser];
    }
    delete pendingIceCandidates[remoteUser];
    if (remoteAudioElements[remoteUser]) {
      try {
        remoteAudioElements[remoteUser].srcObject = null;
        remoteAudioElements[remoteUser].remove();
      } catch (e) {}
      delete remoteAudioElements[remoteUser];
    }
  }

  // ---- UI Management ----

  function showVoiceUI(sid, cname) {
    // Remove existing voice panel if any
    hideVoiceUI();

    const panel = document.createElement('div');
    panel.id = 'voice-channel-panel';
    panel.style.cssText = 'position:fixed;bottom:60px;right:10px;background:var(--bg-s);border:1px solid var(--border);border-radius:8px;padding:12px;min-width:200px;max-width:300px;z-index:1000;box-shadow:0 4px 16px rgba(0,0,0,.4);';

    panel.innerHTML = `
      <div class="voice-channel-title"><img src="/icons/voice.svg" alt="" aria-hidden="true"><span># ${escHtml(cname)}</span></div>
      <div id="voice-participants" style="margin-bottom:8px;font-size:12px;color:var(--txt);"></div>
      <div style="display:flex;gap:6px;">
        <button id="voice-mute-btn" onclick="window.toggleVoiceMute()" style="flex:1;padding:6px;border:none;border-radius:4px;background:var(--bl);color:var(--accent-contrast);cursor:pointer;font-size:12px;"><img src="/icons/voice.svg" alt="" aria-hidden="true"> Mute</button>
        <button onclick="window.leaveVoiceChannel()" style="flex:1;padding:6px;border:none;border-radius:4px;background:var(--danger);color:var(--white);cursor:pointer;font-size:12px;">Leave</button>
      </div>
    `;

    document.body.appendChild(panel);
  }

  function hideVoiceUI() {
    const panel = document.getElementById('voice-channel-panel');
    if (panel) panel.remove();
  }

  function renderVoiceParticipants(participants) {
    const el = document.getElementById('voice-participants');
    if (!el) return;
    el.innerHTML = participants.map(p => {
      const isMe = p.username === myUsername;
      const muteIcon = p.muted ? '<img src="/icons/mute.svg" alt="" aria-hidden="true">' : '<img src="/icons/voice.svg" alt="" aria-hidden="true">';
      return `<div style="padding:2px 0;display:flex;align-items:center;gap:6px;">
        <span style="font-size:14px;">${muteIcon}</span>
        <span style="color:${isMe ? 'var(--bl)' : 'var(--txt)'};">${escHtml(p.username)}${isMe ? ' (you)' : ''}</span>
      </div>`;
    }).join('');
  }

  // ---- Cleanup on page unload ----
  window.addEventListener('beforeunload', () => {
    if (currentVoiceChannel) {
      leaveVoiceChannel();
    }
  });

  // Export
  window.joinVoiceChannel = window.joinVoiceChannel;
  window.leaveVoiceChannel = window.leaveVoiceChannel;
  window.toggleVoiceMute = window.toggleVoiceMute;
})();
