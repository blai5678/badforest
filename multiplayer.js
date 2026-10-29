// Adapta la API de salas que usa el juego a las salas WebSocket de server.js.
// Cada código se convierte en una sala independiente y compartida.
window.claude = window.claude || {};
window.claude.use = async function () {
  return {
    join: async function (name) {
      const roomName = String(name || 'bosque-maldito').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 32) || 'bosque-maldito';
      const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const ws = new WebSocket(scheme + '//' + location.host + '/ws?room=' + encodeURIComponent(roomName));
      let self = '', localPresence = null, known = [];
      const listeners = new Map(), peerListeners = [], buffered = [], pending = [];
      const room = {
        on(event, fn) {
          if (!listeners.has(event)) listeners.set(event, []);
          listeners.get(event).push(fn);
          for (let i = buffered.length - 1; i >= 0; i--) {
            if (buffered[i].name === event) { fn(buffered[i].value); buffered.splice(i, 1); }
          }
        },
        onPeers(fn) { peerListeners.push(fn); if (known.length) fn({ peers: known }); },
        peers() { return known; },
        presence(data) {
          localPresence = data;
          const msg = { type: 'presence', data };
          if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg)); else pending.push(msg);
          return Promise.resolve();
        },
        emit(event, data) {
          const msg = { type: 'event', name: event, data };
          if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg)); else pending.push(msg);
          return Promise.resolve();
        }
      };
      const notifyPeers = peers => {
        known = (peers || []).map(p => ({ peer: p.peer, presence: p.presence, isMe: p.peer === self, sameTab: p.peer === self }));
        if (self && !known.some(p => p.isMe)) known.push({ peer: self, presence: localPresence, isMe: true, sameTab: true });
        for (const fn of peerListeners) fn({ peers: known });
      };
      ws.addEventListener('message', event => {
        let msg; try { msg = JSON.parse(event.data); } catch { return; }
        if (msg.type === 'welcome') {
          self = msg.peer;
          notifyPeers(known.map(p => ({ peer: p.peer, presence: p.presence })));
          while (pending.length && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(pending.shift()));
          if (localPresence) room.presence(localPresence);
        } else if (msg.type === 'peers') notifyPeers(msg.peers);
        else if (msg.type === 'event') {
          const value = { peer: msg.peer, data: msg.data, isMe: msg.peer === self };
          const callbacks = listeners.get(msg.name) || [];
          if (callbacks.length) callbacks.forEach(fn => fn(value)); else buffered.push({ name: msg.name, value });
        }
      });
      await new Promise((resolve, reject) => {
        if (ws.readyState === WebSocket.OPEN) return resolve();
        ws.addEventListener('open', resolve, { once: true });
        ws.addEventListener('error', () => reject(new Error('No se pudo conectar a la sala')), { once: true });
      });
      return room;
    }
  };
};
