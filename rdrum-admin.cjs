'use strict';
const crypto=require('node:crypto'),fs=require('node:fs'),path=require('node:path');
const TEST_MACHINE='RD1-'+crypto.createHash('sha256').update('R-DRUM deployment integration test v1').digest('hex').toUpperCase();
const TEST_ID=crypto.createHash('sha256').update('R-DRUM/trial/v1|'+TEST_MACHINE).digest('hex');
const page=()=>fs.readFileSync(path.join(__dirname,'rdrum-admin.html'));
const script=()=>fs.readFileSync(path.join(__dirname,'rdrum-admin.js'));
const SESSION_SECONDS=8*60*60;
function createAdminRoutes({store,adminKey,clock=()=>Math.floor(Date.now()/1000),secure=true}){
 const sessions=new Map(),attempts=new Map();
 const digest=v=>crypto.createHash('sha256').update(v).digest();
 const keyHash=typeof adminKey==='string'&&adminKey.length>=32?digest(adminKey):null;
 const cookie=(value,age)=>'rdrum_admin='+value+'; Path=/api/rdrum/admin; HttpOnly; SameSite=Strict; Max-Age='+age+(secure?'; Secure':'');
 return async(req,res)=>{
  const url=new URL(req.url,'http://localhost'),route=url.pathname;
  if(!['/rdrum-admin','/rdrum-admin.js','/api/rdrum/admin/session','/api/rdrum/admin/trials'].includes(route))return false;
  const now=clock();
  for(const [id,until]of sessions)if(until<=now)sessions.delete(id);
  for(const [id,a]of attempts)if(a.until<=now)attempts.delete(id);
  const headers={'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Referrer-Policy':'no-referrer'};
  const send=(status,data,extra={})=>{res.writeHead(status,{...headers,'Content-Type':'application/json; charset=utf-8',...extra});res.end(JSON.stringify(data));};
  const sessionId=(req.headers.cookie||'').split(';').map(s=>s.trim()).find(s=>s.startsWith('rdrum_admin='))?.slice(12);
  const loggedIn=sessionId&&sessions.has(sessionId);
  if(route==='/rdrum-admin'||route==='/rdrum-admin.js'){
   if(req.method!=='GET'){send(405,{error:'Método no permitido.'});return true;}
   res.writeHead(200,{...headers,'Content-Type':route.endsWith('.js')?'text/javascript; charset=utf-8':'text/html; charset=utf-8','Content-Security-Policy':"default-src 'none'; script-src 'self'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"});res.end(route.endsWith('.js')?script():page());return true;
  }
  if(!keyHash){send(503,{error:'Panel privado pendiente de configuración.'});return true;}
  if(req.headers.origin){let origin;try{origin=new URL(req.headers.origin);}catch{send(403,{error:'Origen no permitido.'});return true;}if(origin.host!==req.headers.host||!['http:','https:'].includes(origin.protocol)){send(403,{error:'Origen no permitido.'});return true;}}
  if(route==='/api/rdrum/admin/session'){
   if(req.method==='GET'){send(loggedIn?200:401,{authenticated:!!loggedIn});return true;}
   if(req.method==='DELETE'){if(sessionId)sessions.delete(sessionId);send(200,{authenticated:false},{'Set-Cookie':cookie('',0)});return true;}
   if(req.method!=='POST'){send(405,{error:'Método no permitido.'});return true;}
   const ip=req.socket?.remoteAddress||'unknown',prior=attempts.get(ip);
   if(prior&&prior.count>=5){send(429,{error:'Demasiados intentos. Espera 15 minutos.'},{'Retry-After':String(prior.until-now)});return true;}
   try{
    let length=0,chunks=[];for await(const part of req){length+=part.length;if(length>1024){send(413,{error:'Solicitud demasiado grande.'});return true;}chunks.push(part);}
    let data;try{data=JSON.parse(Buffer.concat(chunks).toString());}catch{send(400,{error:'Solicitud inválida.'});return true;}
    if(typeof data?.key!=='string'||!crypto.timingSafeEqual(digest(data.key),keyHash)){
     if(attempts.size>=1000&&!attempts.has(ip)){send(429,{error:'Inténtalo más tarde.'});return true;}
     attempts.set(ip,{count:(prior?.count||0)+1,until:prior?.until||now+900});send(401,{error:'Clave incorrecta.'});return true;
    }
    attempts.delete(ip);if(sessions.size>=1000){send(503,{error:'Inténtalo más tarde.'});return true;}
    const id=crypto.randomBytes(32).toString('hex');sessions.set(id,now+SESSION_SECONDS);
    send(200,{authenticated:true},{'Set-Cookie':cookie(id,SESSION_SECONDS)});
   }catch{send(503,{error:'No se pudo iniciar sesión.'});}
   return true;
  }
  if(req.method!=='GET'){send(405,{error:'Método no permitido.'});return true;}
  if(!loggedIn){send(401,{error:'Inicia sesión para consultar las pruebas.'});return true;}
  const status=url.searchParams.get('status')||'all',search=(url.searchParams.get('search')||'').toLowerCase(),offset=Number(url.searchParams.get('offset')||0);
  if(!['all','active','expired'].includes(status)||! /^[a-f0-9]{0,64}$/.test(search)||!Number.isSafeInteger(offset)||offset<0||offset>10000000){send(400,{error:'Filtro inválido.'});return true;}
  try{send(200,await store.listTrials({now,status,search,offset,limit:50,exclude:TEST_ID}));}catch{send(503,{error:'No se pudo consultar el registro. Inténtalo de nuevo.'});}
  return true;
 };
}
module.exports={createAdminRoutes,TEST_ID,SESSION_SECONDS};
