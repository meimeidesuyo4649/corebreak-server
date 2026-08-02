// COREBREAK 中継サーバー
// 役割：4桁コードで2人をマッチングし、以後の全メッセージをそのまま中継する
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('COREBREAK relay OK\n');
});

const wss = new WebSocketServer({ server });
const rooms = new Map(); // code -> { host, guest, t }

function send(ws, o){ try{ ws.send(JSON.stringify(o)); }catch(_){} }

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', data => {
    let m; try{ m = JSON.parse(data); }catch(_){ return; }

    if(m.t === 'create'){
      let code;
      do { code = String(1000 + Math.floor(Math.random()*9000)); } while(rooms.has(code));
      rooms.set(code, { host: ws, guest: null, t: Date.now() });
      ws.room = code; ws.role = 'host';
      send(ws, { t:'room', code });

    } else if(m.t === 'join'){
      const code = String(m.code || '');
      const r = rooms.get(code);
      if(!r){ send(ws, { t:'err', m:'部屋が見つかりません' }); return; }
      if(r.guest){ send(ws, { t:'err', m:'その部屋は満員です' }); return; }
      r.guest = ws; ws.room = code; ws.role = 'guest';
      send(r.host, { t:'paired', you:'host' });
      send(ws,     { t:'paired', you:'guest' });

    } else if(m.t === 'm'){
      const r = rooms.get(ws.room); if(!r) return;
      const peer = ws.role === 'host' ? r.guest : r.host;
      if(peer && peer.readyState === 1) send(peer, { t:'m', d:m.d });
    }
  });

  ws.on('close', () => {
    const r = rooms.get(ws.room);
    if(r){
      const peer = ws.role === 'host' ? r.guest : r.host;
      if(peer && peer.readyState === 1) send(peer, { t:'peer_left' });
      rooms.delete(ws.room);
    }
  });
});

// 死活監視＆未使用部屋の掃除
setInterval(() => {
  wss.clients.forEach(ws => {
    if(!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    try{ ws.ping(); }catch(_){}
  });
  const now = Date.now();
  for(const [code, r] of rooms){
    if(!r.guest && now - r.t > 10*60*1000){
      rooms.delete(code);
      try{ r.host.close(); }catch(_){}
    }
  }
}, 30000);

server.listen(PORT, () => console.log('COREBREAK relay listening on ' + PORT));
