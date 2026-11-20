const http=require('http');
const fs=require('fs');
const path=require('path');
const crypto=require('crypto');
const {WebSocketServer}=require('ws');
const PORT=Number(process.env.PORT||3000);
const KEY=process.env.PRESENTER_KEY;
if(!KEY){console.error('PRESENTER_KEY environment variable required');process.exit(1)}
const root=__dirname;
const mime={'.html':'text/html; charset=utf-8','.js':'application/javascript','.css':'text/css','.png':'image/png','.jpg':'image/jpeg','.jpeg':'image/jpeg','.wav':'audio/wav','.ico':'image/x-icon'};
const server=http.createServer((req,res)=>{
  const u=new URL(req.url,'http://localhost');
  let pathname;
  try{pathname=decodeURIComponent(u.pathname)}catch{res.writeHead(400).end();return}
  const name=pathname==='/'?'/index.html':pathname;
  const file=path.resolve(root,'.'+name);
  if(!file.startsWith(root+path.sep)||!mime[path.extname(file)]){
    res.writeHead(404).end('Not found');return;
  }
  fs.stat(file,(err,stat)=>{
    if(err||!stat.isFile()){res.writeHead(404).end('Not found');return}
    res.writeHead(200,{'Content-Type':mime[path.extname(file)],'Cache-Control':'public, max-age=3600','X-Content-Type-Options':'nosniff'});
    fs.createReadStream(file).pipe(res);
  });
});
const wss=new WebSocketServer({server,path:'/live'});
let currentScene=0;
const clients=new Set();
const send=(ws,msg)=>{if(ws.readyState===1)ws.send(JSON.stringify(msg))};
const broadcast=(msg)=>{for(const ws of clients)if(ws.role==='audience')send(ws,msg)};
wss.on('connection',ws=>{
  ws.role='pending';
  ws.on('message',raw=>{
    if(raw.length>2048)return;
    let m;try{m=JSON.parse(raw.toString())}catch{return}
    if(ws.role==='pending'){
      if(m.type!=='join')return;
      if(m.role==='presenter'){
        const a=Buffer.from(String(m.key||'')),b=Buffer.from(KEY);
        if(a.length!==b.length||!crypto.timingSafeEqual(a,b)){send(ws,{type:'error',message:'진행자 비밀번호가 올바르지 않습니다.'});ws.close();return}
        ws.role='presenter';
      }else if(m.role==='audience'){ws.role='audience'}else{return}
      clients.add(ws);send(ws,{type:'joined'});send(ws,{type:'state',scene:currentScene,immediate:true});return;
    }
    if(ws.role!=='presenter')return;
    if(m.type==='scene'&&Number.isInteger(m.scene)&&m.scene>=0&&m.scene<=12){
      currentScene=m.scene;
      broadcast({type:'state',scene:currentScene,immediate:!!m.immediate});
    }
  });
  ws.on('close',()=>clients.delete(ws));
});
server.listen(PORT,()=>console.log('MY CASE FILE live server on port '+PORT));
