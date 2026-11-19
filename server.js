const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { Pool } = require('pg');

const pool = process.env.DATABASE_URL ? new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 5, idleTimeoutMillis: 30000, connectionTimeoutMillis: 10000 }) : null;
let pendingWrites = Promise.resolve();

const PORT = Number(process.env.PORT || 3020);
const ROOT = __dirname;
const PUBLIC = path.join(ROOT, 'public');
const DATA_DIR = path.join(ROOT, 'data');
const DB_FILE = path.join(DATA_DIR, 'database.json');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const products = [
  ['AGUA NATURAL 510 ML',8], ['AGUA COM GÁS 510 ML',9], ['BATATA PRINGLES 35G',15],
  ['COCA TRADICIONAL 350ML',10], ['COCA ZERO 350ML',10], ['CHOCOLATE BIS',12],
  ['CERVEJA HEINEKEN 350ML',18], ['GUARANÁ 350 ML',10], ['TRIDENT MENTA',5], ['TRIDENT MELANCIA',5]
].map(([name,price]) => ({name,price}));

const rooms = [
'601','602','603','604','605','606','607','608','609','610','612','614','615','616','617','618','620',
'701','702','703','704','705','706','707','708','709','710','711','712','713','714','715','716','717','718','719','720',
'801','802','803','804','805','806','807','808','809','810','811','812','813','814','815','816','817','818','819','820',
'901','902','903','904','905','906','907','908','909','910','911','912','913','914','915','916','917','918','919','920',
'1001','1002','1003','1004','1005','1006','1007','1008','1009','1010','1011','1012','1013','1014','1015','1016','1017','1018','1019','1020',
'1101','1102','1103','1104','1105','1106','1107','1108','1109','1110','1111','1112','1113','1114','1115','1116','1117','1118','1119','1120',
'1201','1202','1203','1204','1205','1206','1207','1208','1209','1210','1211','1213','1214','1215','1216','1217','1218','1219','1220',
'1301','1302','1303','1304','1305','1306','1307','1308','1309','1310','1311','1312','1313','1314','1315','1316','1317','1318','1319','1320',
'1401','1402','1403','1404','1405','1406','1407','1408','1409','1410','1411','1412','1413','1414','1415','1416','1417','1418','1419','1420',
'1501','1502','1503','1504','1505','1506','1507','1508','1509','1510','1511','1512','1513','1514','1515','1516','1517','1518','1519','1520',
'1601','1602','1603','1604','1605','1606','1607','1608','1609','1610','1611','1612','1613','1614','1615','1616','1617','1618','1619','1620',
'1701','1702','1703','1704','1705','1706','1707','1708','1709','1710','1711','1712','1713','1715','1716','1717','1718','1719','1801','1802'
];
const staffSeed = [
['Ana','Recepção'],['Claudinei','Recepção'],['Mario','Recepção'],['Igor','Recepção'],['Gustavo','Recepção'],['Jonatas','Recepção'],['Wilson','Recepção'],['Ailson','Recepção'],
['Jusselia','Governança'],['Celma','Governança'],['Viviane','Governança'],['Francisco','Governança'],
['Claudio','Gerência'],['Geussiane','Gerência'],
['Antonio','Mini Bar'],['Andrea','Mini Bar'],['Débora','Mini Bar']
];
const defaultDb = {version:4,nextId:1,requests:[],losses:[],occurrences:[],extensions:[],conferences:[],chat:[],staff:staffSeed.map((x,i)=>({id:i+1,name:x[0],sector:x[1],active:true})),settings:{adminPin:'2580',sectorPasswords:{'Recepção':'','Mini Bar':'','Governança':'','Gerência':''},floorAssignments:{'6-11':['Andrea','Débora'],'13-18':['Antonio']},products:products.map(x=>({name:x.name,price:x.price})),configItems:[{id:1,type:'opcao',name:'Solicitação emergencial',description:'Permite que a Recepção solicite atendimento ao Mini Bar.',sector:'Recepção',active:true,order:1},{id:2,type:'status',name:'A CAMINHO',description:'Aviso operacional enviado pelo Mini Bar.',sector:'Mini Bar',active:true,order:2},{id:3,type:'status',name:'ELEVADOR CONGESTIONADO',description:'Aviso de atraso no deslocamento.',sector:'Mini Bar',active:true,order:3},{id:4,type:'mensagem',name:'Mensagem de atendimento',description:'Mensagem livre entre setores.',sector:'Todos',active:true,order:4},{id:5,type:'visibilidade',name:'Acessos / Não perturbe na Recepção',description:'Mostra à Recepção uma cópia somente leitura das ocorrências de acesso.',sector:'Recepção',active:true,order:5}]}};
function loadDb(){try{const x=JSON.parse(fs.readFileSync(DB_FILE,'utf8'));for(const k of ['requests','losses','occurrences','extensions','conferences','chat','staff'])if(!Array.isArray(x[k]))x[k]=[];if(!x.settings)x.settings={adminPin:'2580'}; if(!x.settings.sectorPasswords)x.settings.sectorPasswords={'Recepção':'','Mini Bar':'','Governança':'','Gerência':''}; else if(!Object.prototype.hasOwnProperty.call(x.settings.sectorPasswords,'Mini Bar'))x.settings.sectorPasswords['Mini Bar']=''; if(!x.settings.floorAssignments)x.settings.floorAssignments={'6-11':['Andrea','Débora'],'13-18':['Antonio']}; if(!Array.isArray(x.settings.products))x.settings.products=products.map(x=>({name:x.name,price:x.price})); if(!Array.isArray(x.settings.configItems))x.settings.configItems=structuredClone(defaultDb.settings.configItems); return x;}catch{fs.writeFileSync(DB_FILE,JSON.stringify(defaultDb,null,2));return structuredClone(defaultDb);}}
let db=loadDb();
async function initializeDatabase(){
  if(!pool) return;
  await pool.query(`create table if not exists public.app_state (
    id smallint primary key default 1 check (id = 1),
    payload jsonb not null,
    updated_at timestamptz not null default now()
  )`);
  await pool.query('alter table public.app_state enable row level security');
  const result = await pool.query('select payload from public.app_state where id = 1');
  if(result.rows.length){
    db = result.rows[0].payload;
    for(const k of ['requests','losses','occurrences','extensions','conferences','chat','staff']) if(!Array.isArray(db[k])) db[k]=[];
    if(!db.settings) db.settings=structuredClone(defaultDb.settings);
    if(!db.nextId) db.nextId=1;
  } else {
    await pool.query('insert into public.app_state (id,payload) values (1,$1::jsonb) on conflict (id) do nothing',[JSON.stringify(db)]);
  }
}
function save(){
  if(pool){
    const snapshot = JSON.stringify(db);
    pendingWrites = pendingWrites.then(()=>pool.query(
      'insert into public.app_state (id,payload,updated_at) values (1,$1::jsonb,now()) on conflict (id) do update set payload=excluded.payload, updated_at=now()',
      [snapshot]
    ));
    return pendingWrites;
  }
  const tmp=DB_FILE+'.tmp';fs.writeFileSync(tmp,JSON.stringify(db,null,2));fs.renameSync(tmp,DB_FILE);
}
function hashPassword(v){return crypto.createHash('sha256').update(String(v)).digest('hex');}
function iso(){return new Date().toISOString();}
function brDate(d=new Date()){return d.toLocaleString('pt-BR');}
function send(res,status,obj,type='application/json'){
  const finish=()=>{if(res.writableEnded)return;res.writeHead(status,{'Content-Type':type,'Cache-Control':'no-store','Access-Control-Allow-Origin':'*'});res.end(type==='application/json'?JSON.stringify(obj):obj);};
  if(pool) pendingWrites.then(finish).catch(err=>{console.error('Falha ao persistir no Supabase:',err.message);if(!res.headersSent) {res.writeHead(503,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify({error:'Não foi possível confirmar a gravação no banco de dados. Tente novamente.'}));} else res.destroy(err);});
  else finish();
}
function body(req){return new Promise((resolve,reject)=>{let s='';req.on('data',c=>{s+=c;if(s.length>1e6) req.destroy();});req.on('end',()=>{try{resolve(s?JSON.parse(s):{});}catch(e){reject(e);}});});}
function notify(sectors,payload){const msg='data: '+JSON.stringify(payload)+'\n\n';clients.forEach(c=>{if(sectors.includes(c.sector)) c.res.write(msg);});}
const clients=new Set(); const adminTokens=new Set();
function apiBootstrap(){return {rooms,products:db.settings.products||products,staff:db.staff.filter(x=>x.active),settings:{adminPinSet:!!db.settings.adminPin,floorAssignments:db.settings.floorAssignments||{},configItems:db.settings.configItems||[]}};}
function cleanText(v){return String(v??'').replace(/\s+/g,' ').trim();}
const banned=[/\b(fdp|filho da puta|foda-se|fodase|porra|caralho|merda|puta|puto|idiota|imbecil|otario|otária|otario|babaca|desgraçado|desgracado|vai tomar no cu|vtmnc|vtnc)\b/i];
function hasBadLanguage(v){const t=String(v??'').normalize('NFD').replace(/[\u0300-\u036f]/g,''); return banned.some(r=>r.test(t));}
function sectorSafe(v){return ['Recepção','Mini Bar','Governança','Gerência'].includes(v)?v:'';}
function floorKey(room){const n=Number(String(room).slice(0,-2)); if(n>=6&&n<=11)return '6-11'; if(n>=13&&n<=18)return '13-18'; return '';}
function dashboard(){const today=iso().slice(0,10);const losses=db.losses.filter(x=>x.createdAt.slice(0,10)===today);const total=losses.reduce((s,x)=>s+x.quantity*x.unitPrice,0);const by={};losses.forEach(x=>{by[x.type]??={type:x.type,total:0,count:0};by[x.type].total+=x.quantity*x.unitPrice;by[x.type].count++;});return {lossesToday:losses.length,totalLoss:total,byType:Object.values(by),pendingRequests:db.requests.filter(x=>!['concluida','entregue','finalizada','deu certo'].includes(x.status)).length,pendingOccurrences:db.occurrences.filter(x=>!['deu certo','não deu certo'].includes(x.status)).length};}
function route(req,res){
  const u=new URL(req.url,'http://localhost'); const p=u.pathname;
  if(req.method==='GET' && p==='/api/version') return send(res,200,{version:'V11-CONFIGURACAO-PERSONALIZACAO',port:PORT});
  if(req.method==='GET' && p==='/api/bootstrap') return send(res,200,apiBootstrap());
  if(req.method==='GET' && p==='/api/dashboard') return send(res,200,dashboard());
  if(req.method==='GET' && p==='/api/requests') return send(res,200,db.requests.slice().reverse().slice(0,300));
  if(req.method==='GET' && p==='/api/losses') return send(res,200,db.losses.slice().reverse().slice(0,500));
  if(req.method==='GET' && p==='/api/occurrences') return send(res,200,db.occurrences.slice().reverse().slice(0,300));
  if(req.method==='GET' && p==='/api/extensions') return send(res,200,db.extensions.slice().reverse().slice(0,200));
  if(req.method==='GET' && p==='/api/chat') { const withSector=String(u.searchParams.get('with')||''); const msgs=db.chat.filter(x=>(x.from===sectorSafe(u.searchParams.get('sector')||'')||x.to===sectorSafe(u.searchParams.get('sector')||'')) && (!withSector || x.from===withSector || x.to===withSector)); return send(res,200,msgs.slice(-300)); }
  if(req.method==='GET' && p==='/api/floors') return send(res,200,db.settings.floorAssignments||{});
  if(req.method==='GET' && p==='/api/events'){
    const sector=(u.searchParams.get('sector')||'').trim();res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache','Connection':'keep-alive','Access-Control-Allow-Origin':'*'});res.write(': connected\n\n');const c={res,sector};clients.add(c);req.on('close',()=>clients.delete(c));return;
  }
  if(req.method==='POST' && p==='/api/admin/verify') return body(req).then(b=>{const ok=DEMO_MODE || String(b.pin||'')===String(db.settings.adminPin);const token=ok?crypto.randomBytes(24).toString('hex'):'';if(token)adminTokens.add(token);send(res,200,{ok,token,demo:DEMO_MODE});});
  if(req.method==='POST' && p==='/api/sector/verify') return body(req).then(b=>{const sector=String(b.sector||''); const pass=String(b.password||''); if(DEMO_MODE)return send(res,200,{ok:true,configured:false,demo:true}); const configured=Object.prototype.hasOwnProperty.call(db.settings.sectorPasswords||{},sector) && String(db.settings.sectorPasswords[sector]||'').length>0; const stored=String(db.settings.sectorPasswords[sector]||''); const ok=!configured || hashPassword(pass)===stored || pass===stored; send(res,200,{ok,configured});});
  if(req.method==='POST' && p==='/api/admin/pin') return body(req).then(b=>{if(!adminTokens.has(req.headers['x-admin-token']||'')) return send(res,403,{error:'Acesso administrativo não autorizado.'});if(!b.currentPin||String(b.currentPin)!==String(db.settings.adminPin)) return send(res,403,{error:'PIN atual inválido.'});if(String(b.newPin||'').length<4) return send(res,400,{error:'O novo PIN deve ter pelo menos 4 dígitos.'});db.settings.adminPin=String(b.newPin);save();send(res,200,{ok:true});});
  if(req.method==='GET' && p==='/api/admin/sector-passwords') { if(!adminTokens.has(req.headers['x-admin-token']||'')) return send(res,403,{error:'Acesso administrativo não autorizado.'}); const sp=db.settings.sectorPasswords||{}; return send(res,200,{sectors:Object.keys(sp).map(sector=>({sector,configured:String(sp[sector]||'').length>0}))}); }
  if(req.method==='POST' && p==='/api/admin/floors') return body(req).then(b=>{if(!adminTokens.has(req.headers['x-admin-token']||'')) return send(res,403,{error:'Acesso administrativo não autorizado.'}); const key=String(b.key||''); const names=Array.isArray(b.names)?b.names.map(cleanText).filter(Boolean):[]; if(!['6-11','13-18'].includes(key)) return send(res,400,{error:'Faixa inválida.'}); db.settings.floorAssignments[key]=names; save(); send(res,200,{ok:true});});
  if(req.method==='POST' && p==='/api/admin/products') return body(req).then(b=>{if(!adminTokens.has(req.headers['x-admin-token']||'')) return send(res,403,{error:'Acesso administrativo não autorizado.'}); const name=cleanText(b.name); const price=Number(b.price); if(!name||!Number.isFinite(price)||price<0)return send(res,400,{error:'Nome e preço válidos são obrigatórios.'}); const arr=db.settings.products||products; const existing=arr.find(x=>x.name.toLowerCase()===name.toLowerCase()); if(existing)existing.price=price; else arr.push({name,price}); db.settings.products=arr; save(); send(res,200,{ok:true,products:arr});});
  if(req.method==='POST' && p==='/api/chat') return body(req).then(b=>{const from=sectorSafe(b.from),to=sectorSafe(b.to),text=cleanText(b.text); if(!from||!to||!text)return send(res,400,{error:'Mensagem incompleta.'}); if(hasBadLanguage(text))return send(res,400,{error:'Mensagem não enviada. Utilize linguagem profissional.'}); const x={id:db.nextId++,from,to,user:cleanText(b.user)||from,text,createdAt:iso(),readBy:[]}; db.chat.push(x); save(); notify([to],{kind:'chat',title:'Nova mensagem',message:`${x.from} • ${x.user}`,data:x}); send(res,201,x);});
  if(req.method==='POST' && p==='/api/chat/read') return body(req).then(b=>{const sector=sectorSafe(b.sector); const ids=Array.isArray(b.ids)?b.ids.map(Number):[]; db.chat.forEach(x=>{if(ids.includes(x.id)&&!x.readBy.includes(sector))x.readBy.push(sector);}); save(); send(res,200,{ok:true});});
  if(req.method==='GET' && p==='/api/admin/config-items') { if(!adminTokens.has(req.headers['x-admin-token']||'')) return send(res,403,{error:'Acesso administrativo não autorizado.'}); return send(res,200,(db.settings.configItems||[]).slice().sort((a,b)=>(a.order||0)-(b.order||0))); }
  if(req.method==='POST' && p==='/api/admin/config-items') return body(req).then(b=>{if(!adminTokens.has(req.headers['x-admin-token']||'')) return send(res,403,{error:'Acesso administrativo não autorizado.'}); const name=cleanText(b.name),type=cleanText(b.type)||'opcao',description=cleanText(b.description),sector=cleanText(b.sector)||'Todos'; if(!name)return send(res,400,{error:'Nome da configuração é obrigatório.'}); if(!['opcao','status','mensagem','visibilidade','regra'].includes(type))return send(res,400,{error:'Tipo de configuração inválido.'}); const arr=db.settings.configItems||[]; const id=Math.max(0,...arr.map(x=>Number(x.id)||0))+1; const row={id,type,name,description,sector,active:b.active!==false,order:Number(b.order)||arr.length+1}; arr.push(row); db.settings.configItems=arr; save(); send(res,201,row);});
  if(req.method==='PATCH' && p.startsWith('/api/admin/config-items/')) return body(req).then(b=>{if(!adminTokens.has(req.headers['x-admin-token']||'')) return send(res,403,{error:'Acesso administrativo não autorizado.'}); const id=Number(p.split('/').pop()); const x=(db.settings.configItems||[]).find(x=>Number(x.id)===id); if(!x)return send(res,404,{error:'Configuração não encontrada.'}); if(b.name!==undefined)x.name=cleanText(b.name); if(b.description!==undefined)x.description=cleanText(b.description); if(b.type!==undefined)x.type=cleanText(b.type); if(b.sector!==undefined)x.sector=cleanText(b.sector); if(b.active!==undefined)x.active=!!b.active; if(b.order!==undefined)x.order=Number(b.order)||x.order; save(); send(res,200,x);});
  if(req.method==='POST' && p==='/api/admin/sector-passwords') return body(req).then(b=>{if(!adminTokens.has(req.headers['x-admin-token']||'')) return send(res,403,{error:'Acesso administrativo não autorizado.'}); const sector=String(b.sector||''); if(!['Recepção','Mini Bar','Governança','Gerência'].includes(sector)) return send(res,400,{error:'Setor inválido.'}); const password=String(b.password||''); if(password && password.length<4) return send(res,400,{error:'A senha deve ter pelo menos 4 caracteres.'}); db.settings.sectorPasswords[sector]=password?hashPassword(password):''; save(); send(res,200,{ok:true,configured:!!password});});
  if(req.method==='POST' && p==='/api/staff') return body(req).then(b=>{if(!adminTokens.has(req.headers['x-admin-token']||'')) return send(res,403,{error:'Acesso administrativo não autorizado.'});if(!b.name||!b.sector) return send(res,400,{error:'Nome e setor são obrigatórios.'});const row={id:Math.max(0,...db.staff.map(x=>x.id))+1,name:String(b.name).trim(),sector:String(b.sector),active:true};db.staff.push(row);save();send(res,201,row);});
  if(req.method==='GET' && p==='/api/staff/all') { if(!adminTokens.has(req.headers['x-admin-token']||'')) return send(res,403,{error:'Acesso administrativo não autorizado.'}); return send(res,200,db.staff); }
  if(req.method==='PATCH' && p.startsWith('/api/staff/')) return body(req).then(b=>{if(!adminTokens.has(req.headers['x-admin-token']||'')) return send(res,403,{error:'Acesso administrativo não autorizado.'}); const id=Number(p.split('/').pop()); const x=db.staff.find(x=>x.id===id); if(!x)return send(res,404,{error:'Funcionário não encontrado.'}); if(b.name!==undefined)x.name=cleanText(b.name); if(b.sector!==undefined)x.sector=String(b.sector); if(b.active!==undefined)x.active=!!b.active; save(); send(res,200,x);});
  if(req.method==='GET' && p==='/api/conferences') return send(res,200,db.conferences.slice().reverse().slice(0,300));
  if(req.method==='POST' && p==='/api/conferences') return body(req).then(b=>{const x={id:db.nextId++,room:b.room||'',operation:b.operation||'Conferência',requester:b.requester||'',status:'pendente',items:[],total:0,note:b.note||'',createdAt:iso(),updatedAt:iso()};db.conferences.push(x);save();notify(['Mini Bar'],{kind:'conference',title:'Nova conferência de minibar',message:`Apto ${x.room} • ${x.operation}`,data:x});send(res,201,x);});
  if(req.method==='PATCH' && p.startsWith('/api/conferences/')) return body(req).then(b=>{const id=Number(p.split('/').pop());const x=db.conferences.find(x=>x.id===id);if(!x)return send(res,404,{error:'Conferência não encontrada.'});if(b.items){x.items=b.items;x.total=b.items.reduce((s,i)=>s+(Number(i.qty)||0)*(Number(i.price)||0),0);}Object.assign(x,{status:b.status||x.status,assignee:b.assignee||x.assignee||'',note:b.note??x.note,updatedAt:iso()});save();notify(['Recepção','Mini Bar'],{kind:'conference-update',title:'Conferência atualizada',message:`Apto ${x.room} • ${x.status} • R$ ${x.total.toFixed(2)}`,data:x});send(res,200,x);});
  if(req.method==='POST' && p==='/api/requests') return body(req).then(b=>{const row={id:db.nextId++,kind:b.kind||'solicitacao',room:b.room||'',priority:b.priority||'normal',type:b.type||'',quantity:Number(b.quantity)||1,note:b.note||'',requester:b.requester||'',assignee:'',status:'nova',createdAt:iso(),updatedAt:iso()};db.requests.push(row);save();notify(['Mini Bar'],{kind:'request',title:'Nova solicitação',message:`Apto ${row.room} • ${row.type}`,data:row});send(res,201,row);});
  if(req.method==='PATCH' && p.startsWith('/api/requests/')) return body(req).then(b=>{const id=Number(p.split('/').pop());const x=db.requests.find(x=>x.id===id);if(!x)return send(res,404,{error:'Solicitação não encontrada.'});Object.assign(x,b,{updatedAt:iso()});save();notify(['Recepção','Mini Bar'],{kind:'request-update',title:'Solicitação atualizada',message:`Apto ${x.room} • ${x.type} • ${x.status}`,data:x});send(res,200,x);});
  if(req.method==='POST' && p==='/api/occurrences') return body(req).then(b=>{const x={id:db.nextId++,room:b.room||'',type:b.type||'',reportedBy:b.reportedBy||'',status:'aguardando',createdAt:iso(),updatedAt:iso()};db.occurrences.push(x);save();notify(['Governança'],{kind:'occurrence',title:'Nova ocorrência do Mini Bar',message:`Apto ${x.room} • ${x.type}`,data:x});send(res,201,x);});
  if(req.method==='PATCH' && p.startsWith('/api/occurrences/')) return body(req).then(b=>{const id=Number(p.split('/').pop());const x=db.occurrences.find(x=>x.id===id);if(!x)return send(res,404,{error:'Ocorrência não encontrada.'});Object.assign(x,b,{updatedAt:iso()});save();const targets=x.status==='tentar novamente'?['Mini Bar']:['Mini Bar','Governança'];notify(targets,{kind:'occurrence-update',title:'Ocorrência atualizada',message:`Apto ${x.room} • ${x.type} • ${x.status}`,data:x});send(res,200,x);});
  if(req.method==='POST' && p==='/api/extensions') return body(req).then(b=>{const x={id:db.nextId++,room:b.room||'',newDate:b.newDate||'',note:b.note||'',requester:b.requester||'',createdAt:iso()};db.extensions.push(x);save();notify(['Mini Bar'],{kind:'extension',title:'Prorrogação de hospedagem',message:`Apto ${x.room}`,data:x});send(res,201,x);});
  if(req.method==='POST' && p==='/api/losses') return body(req).then(b=>{if(!['Quebra','Não Conferido','Não Reconhecido'].includes(b.type))return send(res,400,{error:'Tipo inválido.'});const x={id:db.nextId++,type:b.type,room:b.room||'',product:b.product||'',quantity:Number(b.quantity)||1,unitPrice:Number(b.unitPrice)||0,guest:b.guest||'',note:b.note||'',responsible:b.responsible||'',createdAt:iso()};db.losses.push(x);save();notify(['Gerência'],{kind:'loss',title:'Nova perda registrada',message:`${x.type} • Apto ${x.room}`,data:x});send(res,201,x);});
  if(req.method==='GET'){
    let file=p==='/'?'/index.html':p; const full=path.normalize(path.join(PUBLIC,file)); if(!full.startsWith(PUBLIC)) return send(res,403,{error:'Acesso negado.'});
    if(!fs.existsSync(full)||fs.statSync(full).isDirectory()) return send(res,404,{error:'Não encontrado.'});
    const ext=path.extname(full);const types={'.html':'text/html;charset=utf-8','.js':'text/javascript;charset=utf-8','.css':'text/css;charset=utf-8','.png':'image/png','.jpg':'image/jpeg','.svg':'image/svg+xml','.ico':'image/x-icon'};res.writeHead(200,{'Content-Type':types[ext]||'application/octet-stream','Cache-Control':'no-cache'});return fs.createReadStream(full).pipe(res);
  }
  return send(res,404,{error:'Rota não encontrada.'});
}
const server=http.createServer((req,res)=>{if(req.method==='OPTIONS'){res.writeHead(204,{'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,PATCH,OPTIONS','Access-Control-Allow-Headers':'Content-Type'});return res.end();}route(req,res);});
async function startServer(){
  if(process.env.NODE_ENV==='production'&&!pool) throw new Error('DATABASE_URL é obrigatório em produção para evitar perda de dados.');
  if(pool) await initializeDatabase();
  server.listen(PORT,'0.0.0.0',()=>{console.log(`GRAN VILLAGIO MINI BAR ativo em http://localhost:${PORT}`);const nets=os.networkInterfaces();for(const [name,arr] of Object.entries(nets))for(const n of arr||[])if(n.family==='IPv4'&&!n.internal)console.log(`REDE: http://${n.address}:${PORT}`);});
}
startServer().catch(err=>{console.error('Falha ao iniciar o sistema:',err);process.exit(1);});
