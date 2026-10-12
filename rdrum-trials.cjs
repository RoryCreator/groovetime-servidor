'use strict';
const crypto=require('node:crypto'),fs=require('node:fs'),path=require('node:path');
const WEEK=7*24*60*60;
class FileStore {
 constructor(directory){this.directory=directory;fs.mkdirSync(directory,{recursive:true,mode:0o700});}
 async getOrStart(id,now){
  const file=path.join(this.directory,id+'.json');
  const record={started_at:now,expires_at:now+WEEK};
  try{fs.writeFileSync(file,JSON.stringify(record),{flag:'wx',mode:0o600});}catch(e){if(e.code!=='EEXIST')throw e;}
  const saved=JSON.parse(fs.readFileSync(file,'utf8'));
  if(!Number.isSafeInteger(saved.started_at)||saved.expires_at-saved.started_at!==WEEK)throw new Error('Invalid trial registry');
  return saved;
 }
}
class PostgresStore {
 constructor(connectionString){const {Pool}=require('pg');this.pool=new Pool({connectionString,max:3});this.ready=null;}
 async getOrStart(id,now){
  if(!this.ready)this.ready=this.pool.query('CREATE TABLE IF NOT EXISTS rdrum_trials(machine_hash text PRIMARY KEY, started_at bigint NOT NULL, expires_at bigint NOT NULL)').catch(e=>{this.ready=null;throw e;});
  await this.ready;
  await this.pool.query('INSERT INTO rdrum_trials(machine_hash,started_at,expires_at) VALUES($1,$2,$3) ON CONFLICT(machine_hash) DO NOTHING',[id,now,now+WEEK]);
  const result=await this.pool.query('SELECT started_at,expires_at FROM rdrum_trials WHERE machine_hash=$1',[id]);
  const record={started_at:Number(result.rows[0].started_at),expires_at:Number(result.rows[0].expires_at)};
  if(record.expires_at-record.started_at!==WEEK)throw new Error('Invalid trial registry');return record;
 }
}
function issuer({store,privateKey,clock=()=>Math.floor(Date.now()/1000)}){
 const key=crypto.createPrivateKey(privateKey);
 if(key.asymmetricKeyType!=='ec'||key.asymmetricKeyDetails.namedCurve!=='prime256v1')throw new Error('Expected dedicated ECDSA P-256 trial key');
 return async(machine,nonce)=>{
  if(typeof machine!=='string'||!/^RD1-[A-F0-9]{64}$/.test(machine))throw Object.assign(new Error('Código de equipo inválido'),{status:400});
  if(typeof nonce!=='string'||! /^[a-f0-9]{32}$/.test(nonce))throw Object.assign(new Error('Solicitud inválida'),{status:400});
  const now=clock(),id=crypto.createHash('sha256').update('R-DRUM/trial/v1|'+machine).digest('hex');
  const record=await store.getOrStart(id,now);
  if(now<record.started_at)throw new Error('Server clock moved backwards');
  const payload=Buffer.from(JSON.stringify({version:1,product:'R-DRUM',kind:'trial',machine,nonce,started_at:record.started_at,expires_at:record.expires_at,checked_at:now}));
  return {payload:payload.toString('base64'),signature:crypto.sign('sha256',payload,{key,dsaEncoding:'der'}).toString('base64')};
 };
}
function createRoutes(options){
 const issue=issuer(options);
 return async(req,res)=>{
  const pathname=new URL(req.url,'http://localhost').pathname;
  if(pathname!=='/api/rdrum/trial')return false;
  const send=(status,data)=>{res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(data));};
  if(req.method!=='POST'){send(405,{error:'Usa POST'});return true;}
  try{
   let bytes=0,chunks=[];
   for await(const chunk of req){bytes+=chunk.length;if(bytes>2048)throw Object.assign(new Error('Solicitud demasiado grande'),{status:413});chunks.push(chunk);}
   let data;try{data=JSON.parse(Buffer.concat(chunks).toString());}catch{throw Object.assign(new Error('JSON inválido'),{status:400});}
   if(!data||typeof data!=='object'||Array.isArray(data))throw Object.assign(new Error('Solicitud inválida'),{status:400});
   send(200,await issue(data.machine,data.nonce));
  }catch(e){send(e.status||503,{error:e.status?e.message:'No se pudo comprobar la prueba. Inténtalo de nuevo.'});}
  return true;
 };
}
function fromEnvironment(env=process.env){
 let store;
 if(env.RDRUM_DATABASE_URL)store=new PostgresStore(env.RDRUM_DATABASE_URL);
 else if(env.RDRUM_PERSISTENT_DISK==='confirmed'&&env.RDRUM_DATA_DIR)store=new FileStore(env.RDRUM_DATA_DIR);
 if(!store||!env.RDRUM_TRIAL_SIGNING_KEY){
  return async(req,res)=>{if(new URL(req.url,'http://localhost').pathname!=='/api/rdrum/trial')return false;res.writeHead(503,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify({error:'La prueba todavía no está configurada en el servidor.'}));return true;};
 }
 return createRoutes({store,privateKey:env.RDRUM_TRIAL_SIGNING_KEY.replace(/\\n/g,'\n')});
}
module.exports={WEEK,FileStore,PostgresStore,issuer,createRoutes,fromEnvironment};
