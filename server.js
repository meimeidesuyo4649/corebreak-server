// COREBREAK 中継サーバー（ロビー対応版）
// 役割：
//  1. ロビー：募集（シーク）を一覧配信し、押した人同士をマッチング
//  2. 中継：ペア成立後、以後のメッセージをそのまま相手へ流す
// データの永続保存は一切しない（全てメモリ上、切断で消える）
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('COREBREAK relay OK\n');
});

const wss = new WebSocketServer({ server });

const rooms = new Map();  // code -> { host, guest, t }   （コード方式・従来互換）
const seeks = new Map();  // id   -> { ws, name, t }      （ロビーの募集）
let seekSeq = 0;

function send(ws, o){ try{ if(ws && ws.readyState === 1) ws.send(JSON.stringify(o)); }catch(_){} }

function lobbyList(){
  const now = Date.now();
  return [...seeks.entries()].map(([id, s]) => ({
    id, name: s.name, w: Math.round((now - s.t)/1000)
  }));
}
function broadcastLobby(){
  const list = lobbyList();
  wss.clients.forEach(ws => { if(ws.inLobby) send(ws, { t:'lobby', list, online: countOnline() }); });
}
function countOnline(){
  let n = 0;
  wss.clients.forEach(ws => { if(ws.readyState === 1) n++; });
  return n;
}
function dropSeeks(ws){
  let changed = false;
  for(const [id, s] of seeks){ if(s.ws === ws){ seeks.delete(id); changed = true; } }
  return changed;
}
function pair(hostWs, guestWs){
  const code = 'p' + (++seekSeq);
  rooms.set(code, { host: hostWs, guest: guestWs, t: Date.now() });
  hostWs.room = code;  hostWs.role = 'host';  hostWs.inLobby = false;
  guestWs.room = code; guestWs.role = 'guest'; guestWs.inLobby = false;
  send(hostWs,  { t:'paired', you:'host'  });
  send(guestWs, { t:'paired', you:'guest' });
}

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', data => {
    let m; try{ m = JSON.parse(data); }catch(_){ return; }

    // ---- ロビー ----
    if(m.t === 'lobby_in'){
      ws.inLobby = true;
      ws.name = String(m.name || '').slice(0, 12) || 'ゲスト';
      send(ws, { t:'lobby', list: lobbyList(), online: countOnline() });

    } else if(m.t === 'lobby_out'){
      ws.inLobby = false;
      if(dropSeeks(ws)) broadcastLobby();

    } else if(m.t === 'seek'){
      dropSeeks(ws); // 1人1募集
      const id = 's' + (++seekSeq);
      seeks.set(id, { ws, name: ws.name || 'ゲスト', t: Date.now() });
      ws.seekId = id;
      send(ws, { t:'seeking', id });
      broadcastLobby();

    } else if(m.t === 'unseek'){
      if(dropSeeks(ws)){ send(ws, { t:'unseeked' }); broadcastLobby(); }

    } else if(m.t === 'accept'){
      const s = seeks.get(String(m.id));
      if(!s){ send(ws, { t:'err', m:'その募集はもう埋まりました' }); send(ws, { t:'lobby', list: lobbyList(), online: countOnline() }); return; }
      if(s.ws === ws){ send(ws, { t:'err', m:'自分の募集には参加できません' }); return; }
      if(s.ws.readyState !== 1){ seeks.delete(String(m.id)); broadcastLobby(); return; }
      seeks.delete(String(m.id));
      dropSeeks(ws);
      pair(s.ws, ws);   // 募集した側がホスト
      broadcastLobby();

    // ---- コード方式（従来互換）----
    } else if(m.t === 'create'){
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

    // ---- 中継 ----
    } else if(m.t === 'm'){
      const r = rooms.get(ws.room); if(!r) return;
      const peer = ws.role === 'host' ? r.guest : r.host;
      send(peer, { t:'m', d:m.d });
    }
  });

  ws.on('close', () => {
    const changed = dropSeeks(ws);
    const r = rooms.get(ws.room);
    if(r){
      const peer = ws.role === 'host' ? r.guest : r.host;
      send(peer, { t:'peer_left' });
      rooms.delete(ws.room);
    }
    if(changed) broadcastLobby();
    else broadcastLobby(); // 接続人数の更新
  });
});

// 死活監視・古い募集と部屋の掃除
setInterval(() => {
  wss.clients.forEach(ws => {
    if(!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    try{ ws.ping(); }catch(_){}
  });
  const now = Date.now();
  let changed = false;
  for(const [id, s] of seeks){
    if(s.ws.readyState !== 1 || now - s.t > 10*60*1000){ seeks.delete(id); changed = true; }
  }
  for(const [code, r] of rooms){
    if(!r.guest && now - r.t > 10*60*1000){ rooms.delete(code); try{ r.host.close(); }catch(_){} }
  }
  if(changed) broadcastLobby();
}, 20000);

server.listen(PORT, () => console.log('COREBREAK relay listening on ' + PORT));
