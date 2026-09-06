import express from 'express';
import helmet from 'helmet';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import pg from 'pg';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const { Pool, Client } = pg;
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 10000;
const DATABASE_URL = process.env.DATABASE_URL;
const JWT_SECRET = process.env.JWT_SECRET || 'dev-only-campusbite-secret';
if (!DATABASE_URL) console.warn('DATABASE_URL is not set. The server cannot use PostgreSQL until it is configured.');
const pool = DATABASE_URL ? new Pool({ connectionString: DATABASE_URL, max: 10, idleTimeoutMillis: 30000, connectionTimeoutMillis: 5000, ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false }) : null;

// Dedicated PostgreSQL listener for authoritative live menu updates. This connection
// is intentionally separate from the API pool so LISTEN cannot consume an API slot.
const menuRealtimeClients = new Set();
let menuRealtimeListenerClient = null;
let menuRealtimeReconnectTimer = null;
let menuRealtimeStarting = false;
const app = express();
app.use(helmet({ contentSecurityPolicy: false }));
app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders(res, filePath){
    // index.html contains the complete application bundle. Prevent browsers
    // and proxies from retaining an older authentication/payment implementation
    // after a deployment. API endpoints already send their own no-store headers.
    if(path.basename(filePath).toLowerCase()==='index.html'){
      res.setHeader('Cache-Control','no-store, no-cache, must-revalidate, proxy-revalidate');
      res.setHeader('Pragma','no-cache');
      res.setHeader('Expires','0');
    }
  }
}));

const schema = fs.readFileSync(path.join(__dirname, 'schema.sql'), 'utf8');
const shops = ['Hari Sandwich','Reo Store','Campus Café'];
const statusNames = ['Received','Preparing','Ready','Completed'];

async function db(){ if(!pool) throw new Error('DATABASE_URL is not configured'); return pool; }
async function acquireClient(timeoutMs=5000){
  if(!pool) throw Object.assign(new Error('DATABASE_URL is not configured'),{status:503});
  let timer;
  try{
    return await Promise.race([
      pool.connect(),
      new Promise((_,reject)=>{ timer=setTimeout(()=>reject(Object.assign(new Error('Database connection pool is busy. Please try again.'),{code:'POOL_TIMEOUT',status:503})),timeoutMs); })
    ]);
  }finally{clearTimeout(timer);}
}
async function init(){
  if(!pool) return;
  const client=await pool.connect();
  try{
    // Serialize startup migrations across Render instances. Everything is done
    // in one transaction so the API never sees a half-migrated database.
    await client.query('BEGIN');
    await client.query(`SELECT pg_advisory_xact_lock(hashtext('campusbite:init'))`);

    // schema.sql contains only table creation. We intentionally keep indexes
    // and legacy ALTERs out of that multi-statement batch because PostgreSQL can
    // parse references against the pre-existing Render schema before later
    // migration statements run.
    await client.query(schema);

    // ---- Legacy-schema migrations: add EVERY column used by the application
    // before any query, index, constraint, seed, or API can reference it. ----
    await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS customer_code TEXT`);
    await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS name TEXT`);
    await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS year TEXT`);
    await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS role TEXT`);
    await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS password_hash TEXT`);
    await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS wallet_balance NUMERIC(12,2) DEFAULT 1250`);
    await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS cashback NUMERIC(12,2) DEFAULT 75`);
    await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS autopay_enabled BOOLEAN DEFAULT FALSE`);
    await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS autopay_threshold NUMERIC(12,2) DEFAULT 200`);
    await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS autopay_amount NUMERIC(12,2) DEFAULT 500`);
    await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()`);

    await client.query(`ALTER TABLE staff_users ADD COLUMN IF NOT EXISTS name TEXT`);
    await client.query(`ALTER TABLE staff_users ADD COLUMN IF NOT EXISTS staff_code TEXT`);
    await client.query(`ALTER TABLE staff_users ADD COLUMN IF NOT EXISTS shop TEXT`);
    await client.query(`ALTER TABLE staff_users ADD COLUMN IF NOT EXISTS pin_hash TEXT`);
    await client.query(`ALTER TABLE staff_users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()`);

    await client.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS public_id TEXT`);
    await client.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS customer_id BIGINT`);
    await client.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS shop TEXT`);
    await client.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS items JSONB`);
    await client.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS total NUMERIC(12,2) DEFAULT 0`);
    await client.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS discount NUMERIC(12,2) DEFAULT 0`);
    await client.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS slot TEXT`);
    await client.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS status SMALLINT DEFAULT 0`);
    await client.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()`);
    await client.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS prep_started_at TIMESTAMPTZ`);
    await client.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS ready_at TIMESTAMPTZ`);
    await client.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ`);
    await client.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS checkout_request_id TEXT`);

    await client.query(`ALTER TABLE wallet_transactions ADD COLUMN IF NOT EXISTS customer_id BIGINT`);
    await client.query(`ALTER TABLE wallet_transactions ADD COLUMN IF NOT EXISTS icon TEXT`);
    await client.query(`ALTER TABLE wallet_transactions ADD COLUMN IF NOT EXISTS title TEXT`);
    await client.query(`ALTER TABLE wallet_transactions ADD COLUMN IF NOT EXISTS sub TEXT`);
    await client.query(`ALTER TABLE wallet_transactions ADD COLUMN IF NOT EXISTS amount NUMERIC(12,2) DEFAULT 0`);
    await client.query(`ALTER TABLE wallet_transactions ADD COLUMN IF NOT EXISTS type TEXT`);
    await client.query(`ALTER TABLE wallet_transactions ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()`);
    // CRITICAL MIGRATION: wallet_transactions.order_id is created here, as a
    // standalone statement, before ANY later statement can mention the column.
    // This is intentionally not present in schema.sql, because Render may already
    // have an older wallet_transactions table without this column.
    await client.query(`ALTER TABLE wallet_transactions ADD COLUMN IF NOT EXISTS order_id BIGINT`);
    const walletOrderColumn = await client.query(`
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='public' AND table_name='wallet_transactions' AND column_name='order_id'
    `);
    if (walletOrderColumn.rowCount !== 1) {
      throw new Error('Database migration failed: wallet_transactions.order_id was not created');
    }

    await client.query(`ALTER TABLE menu_items ADD COLUMN IF NOT EXISTS shop TEXT`);
    await client.query(`ALTER TABLE menu_items ADD COLUMN IF NOT EXISTS name TEXT`);
    await client.query(`ALTER TABLE menu_items ADD COLUMN IF NOT EXISTS price NUMERIC(12,2) DEFAULT 0`);
    await client.query(`ALTER TABLE menu_items ADD COLUMN IF NOT EXISTS stock INTEGER DEFAULT 0`);
    await client.query(`ALTER TABLE menu_items ADD COLUMN IF NOT EXISTS available BOOLEAN DEFAULT TRUE`);

    await client.query(`ALTER TABLE order_ratings ADD COLUMN IF NOT EXISTS customer_id BIGINT`);
    await client.query(`ALTER TABLE order_ratings ADD COLUMN IF NOT EXISTS order_id BIGINT`);
    await client.query(`ALTER TABLE order_ratings ADD COLUMN IF NOT EXISTS rating SMALLINT`);
    await client.query(`ALTER TABLE order_ratings ADD COLUMN IF NOT EXISTS comment TEXT`);
    await client.query(`ALTER TABLE order_ratings ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()`);
    await client.query(`ALTER TABLE order_ratings ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()`);

    await client.query(`ALTER TABLE split_bill_requests ADD COLUMN IF NOT EXISTS requester_id BIGINT`);
    await client.query(`ALTER TABLE split_bill_requests ADD COLUMN IF NOT EXISTS contributor_id BIGINT`);
    await client.query(`ALTER TABLE split_bill_requests ADD COLUMN IF NOT EXISTS amount NUMERIC(12,2) DEFAULT 0`);
    await client.query(`ALTER TABLE split_bill_requests ADD COLUMN IF NOT EXISTS total NUMERIC(12,2) DEFAULT 0`);
    await client.query(`ALTER TABLE split_bill_requests ADD COLUMN IF NOT EXISTS own_share NUMERIC(12,2) DEFAULT 0`);
    await client.query(`ALTER TABLE split_bill_requests ADD COLUMN IF NOT EXISTS shop TEXT`);
    await client.query(`ALTER TABLE split_bill_requests ADD COLUMN IF NOT EXISTS slot TEXT`);
    await client.query(`ALTER TABLE split_bill_requests ADD COLUMN IF NOT EXISTS items JSONB`);
    await client.query(`ALTER TABLE split_bill_requests ADD COLUMN IF NOT EXISTS discount NUMERIC(12,2) DEFAULT 0`);
    await client.query(`ALTER TABLE split_bill_requests ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending'`);
    await client.query(`ALTER TABLE split_bill_requests ADD COLUMN IF NOT EXISTS order_id BIGINT`);
    await client.query(`ALTER TABLE split_bill_requests ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()`);
    await client.query(`ALTER TABLE split_bill_requests ADD COLUMN IF NOT EXISTS responded_at TIMESTAMPTZ`);

    // ---- Safe defaults/backfills for columns just added to an old database. ----
    await client.query(`UPDATE customers SET wallet_balance=1250 WHERE wallet_balance IS NULL`);
    await client.query(`UPDATE customers SET cashback=75 WHERE cashback IS NULL`);
    await client.query(`UPDATE customers SET autopay_enabled=FALSE WHERE autopay_enabled IS NULL`);
    await client.query(`UPDATE customers SET autopay_threshold=200 WHERE autopay_threshold IS NULL`);
    await client.query(`UPDATE customers SET autopay_amount=500 WHERE autopay_amount IS NULL`);
    await client.query(`UPDATE customers SET year='' WHERE year IS NULL`);
    await client.query(`UPDATE menu_items SET price=0 WHERE price IS NULL`);
    await client.query(`UPDATE menu_items SET stock=0 WHERE stock IS NULL`);
    await client.query(`UPDATE menu_items SET available=TRUE WHERE available IS NULL`);

    // ---- Constraints/indexes are created only AFTER all referenced columns exist. ----
    await client.query(`DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='wallet_transactions_order_id_fkey') THEN
        ALTER TABLE wallet_transactions ADD CONSTRAINT wallet_transactions_order_id_fkey
          FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE SET NULL;
      END IF;
    END $$`);
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS customers_customer_code_uidx ON customers(customer_code)`);
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS customers_customer_code_lower_uidx ON customers(LOWER(customer_code))`);
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS staff_users_shop_uidx ON staff_users(shop)`);
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS staff_users_staff_code_uidx ON staff_users(LOWER(staff_code))`);
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS orders_public_id_uidx ON orders(public_id)`);
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS orders_customer_checkout_request_uidx ON orders(customer_id, checkout_request_id) WHERE checkout_request_id IS NOT NULL`);
    await client.query(`CREATE INDEX IF NOT EXISTS orders_customer_created_idx ON orders(customer_id, created_at DESC)`);
    await client.query(`CREATE INDEX IF NOT EXISTS orders_shop_created_idx ON orders(shop, created_at DESC)`);
    await client.query(`CREATE INDEX IF NOT EXISTS orders_shop_status_idx ON orders(shop, status)`);
    await client.query(`CREATE INDEX IF NOT EXISTS wallet_tx_customer_created_idx ON wallet_transactions(customer_id, created_at DESC)`);
    await client.query(`CREATE INDEX IF NOT EXISTS wallet_tx_order_idx ON wallet_transactions(order_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS order_ratings_customer_idx ON order_ratings(customer_id, created_at DESC)`);
    await client.query(`CREATE INDEX IF NOT EXISTS split_bill_requests_contributor_idx ON split_bill_requests(contributor_id, status, created_at DESC)`);
    await client.query(`CREATE INDEX IF NOT EXISTS split_bill_requests_requester_idx ON split_bill_requests(requester_id, created_at DESC)`);

    // The column has already been independently verified above. Keep the repair
    // query separate from the ALTER so PostgreSQL can never resolve wt.order_id
    // against the pre-migration table definition.
    await client.query(`
      UPDATE wallet_transactions
      SET order_id = (
        SELECT o.id
        FROM orders o
        WHERE o.customer_id = wallet_transactions.customer_id
          AND o.total = wallet_transactions.amount
          AND o.created_at <= wallet_transactions.created_at
        ORDER BY o.created_at DESC
        LIMIT 1
      )
      WHERE type='debit'
        AND order_id IS NULL
        AND title='CampusBite order'
    `);

    // Customer accounts are persistent. Older builds seeded demo customers and
    // even deleted non-demo records during startup; that is incompatible with
    // self-registration because a restart/deploy could erase a real user's account.
    // Existing demo accounts remain in the database, but startup never overwrites
    // or deletes customer accounts anymore.
    // Staff credentials are backend-owned and unique per staff/shop. Only bcrypt hashes
    // are persisted in PostgreSQL; plaintext credentials are not embedded in the application.
    // These fixed hashes allow a fresh database to be initialized deterministically while
    // keeping the actual login passwords out of source code.
    const staffSeeds=[
      ['HARI-STAFF','Hari Staff','Hari Sandwich','$2b$12$7bPkROhpZ2Sk9OimGdjZEe3XsYp.szZ/i0HIIAdgGMUbzoyIlRgxK'],
      ['REO-STAFF','Reo Staff','Reo Store','$2b$12$4yfaAh7q7CELtOnzXSCzYehtJk6oVFvvRzyhSG8d/CtLQtHS5FyMG'],
      ['CAFE-STAFF','Cafe Staff','Campus Café','$2b$12$ynUnKOHb5vIQm/Bl6u.eeeN/c5sSXxD5kWpTbTvh7D59CdyLBxJeC']
    ];
    for(const [staffCode,name,shop,hash] of staffSeeds){
      await client.query(`INSERT INTO staff_users(staff_code,name,shop,pin_hash) VALUES($1,$2,$3,$4) ON CONFLICT(shop) DO UPDATE SET staff_code=EXCLUDED.staff_code,name=EXCLUDED.name,pin_hash=EXCLUDED.pin_hash`,[staffCode,name,shop,hash]);
    }
    const menu=[
      [101,'Hari Sandwich','Chicken Sandwich',75,18],[102,'Hari Sandwich','Veg Club Sandwich',65,16],[103,'Hari Sandwich','Paneer Wrap',80,14],[104,'Hari Sandwich','Cheese Toastie',55,20],
      [201,'Reo Store','Lays Classic',30,30],[202,'Reo Store','French Fries',25,25],[203,'Reo Store','Cold Cola',40,22],[204,'Reo Store','Mango Drink',35,18],
      [301,'Campus Café','Cappuccino',75,14],[302,'Campus Café','Masala Chai',30,24],[303,'Campus Café','Veg Hakka Noodles',90,15],[304,'Campus Café','Chocolate Muffin',55,17]
    ];
    for(const [id,shop,name,price,stock] of menu) {
      await client.query(`INSERT INTO menu_items(id,shop,name,price,stock) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET shop=EXCLUDED.shop,name=EXCLUDED.name,price=EXCLUDED.price`,[id,shop,name,price,stock]);
      await client.query(`UPDATE menu_items SET stock=$1 WHERE id=$2 AND stock IS NULL`,[stock,id]);
    }

    // Final hard verification: the exact column that caused the Render failure
    // MUST exist before the transaction can commit.
    const orderIdCheck = await client.query(`SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='wallet_transactions' AND column_name='order_id'`);
    if (orderIdCheck.rowCount !== 1) throw new Error('Database migration verification failed: wallet_transactions.order_id is missing');

    // Authoritative live-menu event source. PostgreSQL emits only after a
    // transaction commits, so customers never receive an update that was later
    // rolled back. The trigger covers availability, stock and price mutations.
    await client.query(`
      CREATE OR REPLACE FUNCTION campusbite_notify_menu_change()
      RETURNS trigger
      LANGUAGE plpgsql
      AS $fn$
      BEGIN
        IF TG_OP = 'DELETE' THEN
          PERFORM pg_notify('campusbite_menu_changed', json_build_object(
            'op', TG_OP,
            'id', OLD.id,
            'shop', OLD.shop,
            'name', OLD.name
          )::text);
          RETURN OLD;
        ELSIF TG_OP = 'INSERT' THEN
          PERFORM pg_notify('campusbite_menu_changed', json_build_object(
            'op', TG_OP,
            'id', NEW.id,
            'shop', NEW.shop,
            'name', NEW.name,
            'price', NEW.price,
            'stock', NEW.stock,
            'available', NEW.available
          )::text);
          RETURN NEW;
        ELSIF OLD.available IS DISTINCT FROM NEW.available
           OR OLD.stock IS DISTINCT FROM NEW.stock
           OR OLD.price IS DISTINCT FROM NEW.price
           OR OLD.name IS DISTINCT FROM NEW.name
           OR OLD.shop IS DISTINCT FROM NEW.shop THEN
          PERFORM pg_notify('campusbite_menu_changed', json_build_object(
            'op', TG_OP,
            'id', NEW.id,
            'shop', NEW.shop,
            'name', NEW.name,
            'price', NEW.price,
            'stock', NEW.stock,
            'available', NEW.available
          )::text);
        END IF;
        RETURN NEW;
      END;
      $fn$
    `);
    await client.query(`DROP TRIGGER IF EXISTS campusbite_menu_changed_trigger ON menu_items`);
    await client.query(`
      CREATE TRIGGER campusbite_menu_changed_trigger
      AFTER INSERT OR UPDATE OR DELETE ON menu_items
      FOR EACH ROW EXECUTE FUNCTION campusbite_notify_menu_change()
    `);

    await client.query('COMMIT');
  }catch(e){
    try{await client.query('ROLLBACK');}catch{}
    throw e;
  }finally{
    client.release();
  }
}

function tokenFor(payload){return jwt.sign(payload,JWT_SECRET,{expiresIn:'12h'});}
function auth(req,res,next){
  try{
    const h=req.headers.authorization||''; const t=h.startsWith('Bearer ')?h.slice(7):'';
    req.user=jwt.verify(t,JWT_SECRET); next();
  }catch{res.status(401).json({error:'Unauthorized'});}
}
function role(r){return (req,res,next)=>{if(req.user?.role!==r)return res.status(403).json({error:'Forbidden'});next();};}

// Customer authentication is intentionally resilient to tokens issued by an
// older CampusBite build.  A signed token that carries a valid customer id
// and customerCode is still a customer credential even if its legacy role
// claim is stale.  This prevents a post-login checkout from being rejected
// with 403 while keeping the JWT signature as the trust boundary.
async function customerRole(req,res,next){
  // NEVER authorize a customer endpoint from the JWT role claim alone.
  // Older CampusBite sessions may contain a stale/mismatched role claim, which
  // was the source of the intermittent post-login 403 on checkout/payment.
  // The JWT signature proves the credential was issued by CampusBite; the
  // customer id + customer code are then resolved against the live database.
  try{
    const id=Number(req.user?.id);
    const code=String(req.user?.customerCode||'').trim();
    if(!Number.isInteger(id)||id<=0||!code){
      return res.status(403).json({error:'Customer authentication is invalid. Please sign in again.',code:'CUSTOMER_AUTH_INVALID'});
    }
    const d=await db();
    const r=(await d.query('SELECT id,customer_code,role FROM customers WHERE id=$1 AND customer_code=$2',[id,code])).rows[0];
    if(!r){
      return res.status(403).json({error:'Customer account could not be verified. Please sign in again.',code:'CUSTOMER_AUTH_INVALID'});
    }
    // Normalize the request identity from PostgreSQL, not from mutable client state.
    req.user={...req.user,role:'customer',id:Number(r.id),customerCode:r.customer_code};
    return next();
  }catch(e){
    console.error('Customer authentication error:',e);
    return res.status(503).json({error:'Customer authentication service is temporarily unavailable. Please try again.',code:'CUSTOMER_AUTH_UNAVAILABLE'});
  }
}
function serializeOrder(r){return {id:r.public_id,items:r.items,total:Number(r.total),status:Number(r.status),discount:Number(r.discount),slot:r.slot,shop:r.shop,createdAt:new Date(r.created_at).getTime(),prepStartedAt:r.prep_started_at?new Date(r.prep_started_at).getTime():null,readyAt:r.ready_at?new Date(r.ready_at).getTime():null,completedAt:r.completed_at?new Date(r.completed_at).getTime():null,customerName:r.customer_name||null,customerCode:r.customer_code||null};}
function serializeTx(r){return {icon:r.icon,title:r.title,sub:r.sub,shop:r.order_shop||'',food:r.order_food||'',amt:Number(r.amount),type:r.type,createdAt:new Date(r.created_at).getTime()};}

let dbReady = false;
app.use('/api',(req,res,next)=>{
  if(req.path !== '/health' && !dbReady) return res.status(503).json({error:'Database initializing. Please retry shortly.'});
  next();
});
app.get('/api/health',(req,res)=>{
  res.status(dbReady?200:200).json({
    ok:true,
    service:'CampusBite',
    database:dbReady?'ready':'initializing',
    time:new Date().toISOString()
  });
});
app.post('/api/auth/customer/register',async(req,res)=>{
  try{
    const name=String(req.body?.name||'').trim();
    const customerCode=String(req.body?.customerCode||'').trim();
    const password=String(req.body?.password||'');
    if(name.length<2 || name.length>80) return res.status(400).json({error:'Enter a valid name.'});
    if(!/^[A-Za-z0-9._-]{4,32}$/.test(customerCode)) return res.status(400).json({error:'CampusBite ID must be 4–32 characters using letters, numbers, dot, underscore or hyphen.'});
    if(password.length<8 || password.length>128) return res.status(400).json({error:'Password must be 8–128 characters.'});
    const d=await db();
    const exists=(await d.query('SELECT 1 FROM customers WHERE LOWER(customer_code)=LOWER($1) LIMIT 1',[customerCode])).rowCount;
    if(exists) return res.status(409).json({error:'That CampusBite ID is already taken. Please choose another.'});
    const hash=await bcrypt.hash(password,12);
    const r=(await d.query(`INSERT INTO customers(customer_code,name,year,password_hash,role)
      VALUES($1,$2,'',$3,'customer') RETURNING id,customer_code,name,year`,[customerCode,name,hash])).rows[0];
    await d.query(`INSERT INTO wallet_transactions(customer_id,icon,title,sub,amount,type) VALUES($1,'💳','Wallet loaded','Demo opening balance',1250,'credit')`,[r.id]);
    await d.query(`INSERT INTO wallet_transactions(customer_id,icon,title,sub,amount,type) VALUES($1,'🎁','Welcome cashback','CampusBite reward',75,'credit')`,[r.id]);
    const token=tokenFor({role:'customer',id:r.id,customerCode:r.customer_code});
    res.status(201).json({token,user:{role:'customer',name:r.name,customerCode:r.customer_code,year:r.year,profileRole:'Customer'}});
  }catch(e){
    if(e?.code==='23505') return res.status(409).json({error:'That CampusBite ID is already taken. Please choose another.'});
    console.error(e);res.status(500).json({error:'Account creation unavailable'});
  }
});
app.post('/api/auth/customer',async(req,res)=>{
  try{const {customerCode,password}=req.body; const r=(await (await db()).query('SELECT * FROM customers WHERE LOWER(customer_code)=LOWER($1)',[String(customerCode||'').trim()])).rows[0];
    if(!r || !(await bcrypt.compare(String(password||''),r.password_hash))) return res.status(401).json({error:'Invalid CampusBite ID or password'});
    const token=tokenFor({role:'customer',id:r.id,customerCode:r.customer_code});
    res.json({token,user:{role:'customer',name:r.name,customerCode:r.customer_code,year:r.year,profileRole:'Customer'}});
  }catch(e){console.error(e);res.status(500).json({error:'Login unavailable'});}
});
app.post('/api/auth/staff',async(req,res)=>{
  try{
    const staffCode=String(req.body.staffCode||'').trim();
    const pin=String(req.body.pin||'');
    if(!/^[A-Za-z0-9-]{4,32}$/.test(staffCode) || pin.length<6 || pin.length>128) return res.status(401).json({error:'Invalid staff credentials'});
    const r=(await (await db()).query('SELECT * FROM staff_users WHERE LOWER(staff_code)=LOWER($1)',[staffCode])).rows[0];
    if(!r || !(await bcrypt.compare(pin,r.pin_hash))) return res.status(401).json({error:'Invalid staff credentials'});
    const token=tokenFor({role:'staff',id:r.id,shop:r.shop});
    res.json({token,user:{role:'staff',name:r.name,staffCode:r.staff_code,shop:r.shop}});
  }catch(e){console.error(e);res.status(500).json({error:'Staff login unavailable'});}
});

app.get('/api/customer/state',auth,customerRole,async(req,res)=>{
  try{const d=await db(); const s=(await d.query('SELECT * FROM customers WHERE id=$1',[req.user.id])).rows[0]; if(!s)return res.status(404).json({error:'Customer not found'});
    const os=(await d.query('SELECT * FROM orders WHERE customer_id=$1 ORDER BY created_at DESC',[s.id])).rows;
    const tx=(await d.query(`SELECT wt.*, o.shop AS order_shop, COALESCE((SELECT string_agg(item->>'name',' • ' ORDER BY ord) FROM jsonb_array_elements(o.items) WITH ORDINALITY AS a(item,ord)), '') AS order_food FROM wallet_transactions wt LEFT JOIN orders o ON o.id=wt.order_id WHERE wt.customer_id=$1 ORDER BY wt.created_at DESC LIMIT 100`,[s.id])).rows;
    const ratings=(await d.query(`SELECT o.public_id AS order_id,r.rating,r.comment FROM order_ratings r JOIN orders o ON o.id=r.order_id WHERE r.customer_id=$1`,[s.id])).rows;
    const splitRequests=(await d.query(`SELECT r.*, rq.name AS requester_name, rq.customer_code AS requester_code, c.name AS contributor_name, c.customer_code AS contributor_code, o.public_id AS order_public_id
      FROM split_bill_requests r
      JOIN customers rq ON rq.id=r.requester_id
      JOIN customers c ON c.id=r.contributor_id
      LEFT JOIN orders o ON o.id=r.order_id
      WHERE r.requester_id=$1 OR r.contributor_id=$1
      ORDER BY r.created_at DESC LIMIT 30`,[s.id])).rows;
    res.json({wallet:Number(s.wallet_balance),cashback:Number(s.cashback),autopay:{enabled:s.autopay_enabled,threshold:Number(s.autopay_threshold),amount:Number(s.autopay_amount)},orders:os.map(serializeOrder),transactions:tx.map(serializeTx),ratings:ratings.map(r=>({orderId:String(r.order_id),rating:Number(r.rating),comment:r.comment||''})),splitRequests:splitRequests.map(r=>({id:Number(r.id),direction:Number(r.contributor_id)===Number(s.id)?'incoming':'outgoing',status:r.status,amount:Number(r.amount),total:Number(r.total),ownShare:Number(r.own_share),shop:r.shop,slot:r.slot,requesterName:r.requester_name,requesterCode:r.requester_code,contributorName:r.contributor_name,contributorCode:r.contributor_code,orderId:r.order_public_id||null,createdAt:new Date(r.created_at).getTime(),respondedAt:r.responded_at?new Date(r.responded_at).getTime():null}))});
  }catch(e){console.error(e);res.status(500).json({error:'State unavailable'});}
});

async function validateOrderForRequest(client, reqUserId, body){
  const {items,slot,shop}=body;
  const allowedPickupSlots=new Set(['ASAP',...Array.from({length:33},(_,i)=>{const total=8*60+i*15;const h24=Math.floor(total/60),m=total%60;const h=h24%12||12;return `${h}:${String(m).padStart(2,'0')} ${h24<12?'AM':'PM'}`;})]);
  if(!shops.includes(shop)||!Array.isArray(items)||!items.length||!allowedPickupSlots.has(String(slot||''))) throw Object.assign(new Error('Please select a valid pickup time'),{status:400});
  const normalized=items.map(i=>({id:Number(i.id),q:Number(i.q),name:String(i.name||''),emoji:String(i.emoji||'🍱')}));
  if(normalized.some(i=>!Number.isInteger(i.id)||!Number.isInteger(i.q)||i.q<1||i.q>20)) throw Object.assign(new Error('Invalid quantity'),{status:400});
  const quantities=new Map(); for(const i of normalized) quantities.set(i.id,(quantities.get(i.id)||0)+i.q);
  if([...quantities.values()].some(q=>q>20)) throw Object.assign(new Error('Invalid quantity'),{status:400});
  const ids=[...new Set(normalized.map(i=>i.id))];
  const menuRows=(await client.query('SELECT id,shop,name,price,stock,available FROM menu_items WHERE id=ANY($1::int[]) FOR UPDATE',[ids])).rows;
  if(menuRows.length!==ids.length) throw Object.assign(new Error('One or more menu items are unavailable'),{status:400});
  const byId=new Map(menuRows.map(r=>[Number(r.id),r]));
  if(normalized.some(i=>{const m=byId.get(i.id);return !m||m.shop!==shop||!m.available})) throw Object.assign(new Error('One or more selected items are unavailable'),{status:400});
  if([...quantities.entries()].some(([id,q])=>Number(byId.get(id)?.stock ?? 0)<q)) throw Object.assign(new Error('One or more selected items do not have enough stock'),{status:400});
  const calculatedSubtotal=normalized.reduce((sum,i)=>sum+Number(byId.get(i.id).price||0)*i.q,0);
  if(calculatedSubtotal<=0) throw Object.assign(new Error('Invalid menu pricing'),{status:400});
  const discount=Number(body.discount)>0&&calculatedSubtotal>=200?Math.round(calculatedSubtotal*.10):0;
  const payable=calculatedSubtotal-discount+3;
  const storedItems=normalized.map(i=>({id:i.id,name:byId.get(i.id).name,price:Number(byId.get(i.id).price),q:i.q,emoji:i.emoji}));
  return {normalized,quantities,storedItems,discount,payable,slot:String(slot||'ASAP'),shop};
}

app.post('/api/customer/order',auth,customerRole,async(req,res)=>{
  let client=null;
  let inTransaction=false;
  try{
    client=await acquireClient(5000);
    // Never allow a checkout request to wait indefinitely for a pooled connection
    // or a database lock. Render's proxy can turn an indefinitely hanging request
    // into a 502, so fail cleanly with a retryable 503 instead.
    const checkoutRequestId=String(req.body.checkoutRequestId||'').trim();
    if(!/^[A-Za-z0-9_-]{16,80}$/.test(checkoutRequestId)) throw Object.assign(new Error('Invalid checkout request. Please reopen checkout and try again.'),{status:400});
    const splitWalletId=String(req.body.splitWalletId||'').trim();
    const splitAmount=Number(req.body.splitAmount||0);
    const splitRequested=!!splitWalletId||req.body.splitAmount!==undefined;
    await client.query('BEGIN');
    inTransaction=true;
    await client.query("SET LOCAL lock_timeout = '5000ms'");
    await client.query("SET LOCAL statement_timeout = '10000ms'");
    await client.query("SET LOCAL idle_in_transaction_session_timeout = '12000ms'");
    const s=(await client.query('SELECT * FROM customers WHERE id=$1 FOR UPDATE',[req.user.id])).rows[0];
    if(!s) throw Object.assign(new Error('Customer not found'),{status:404});
    // Idempotency: if the browser retries because a response was lost, return the
    // already-created order instead of charging the wallet and stock a second time.
    const prior=(await client.query('SELECT * FROM orders WHERE customer_id=$1 AND checkout_request_id=$2',[s.id,checkoutRequestId])).rows[0];
    if(prior){
      await client.query('COMMIT');
      return res.json({idempotent:true,order:serializeOrder(prior),wallet:Number(s.wallet_balance)});
    }
    const v=await validateOrderForRequest(client,s.id,req.body);
    if(splitRequested){
      if(!splitWalletId||!Number.isFinite(splitAmount)||splitAmount<=0) throw Object.assign(new Error('Enter a valid other Customer / Wallet ID and split amount'),{status:400});
      if(splitAmount>=v.payable) throw Object.assign(new Error('The other wallet must pay less than the full order total'),{status:400});
      const contributor=(await client.query('SELECT id,customer_code,name FROM customers WHERE customer_code=$1',[splitWalletId])).rows[0];
      if(!contributor) throw Object.assign(new Error('Other Customer / Wallet ID not found'),{status:404});
      if(Number(contributor.id)===Number(s.id)) throw Object.assign(new Error('Use another student or teacher wallet ID'),{status:400});
      const ownShare=v.payable-splitAmount;
      const duplicate=(await client.query(`SELECT id FROM split_bill_requests WHERE requester_id=$1 AND contributor_id=$2 AND status='pending'`,[s.id,contributor.id])).rows[0];
      if(duplicate) throw Object.assign(new Error('A split bill request is already pending with this customer'),{status:409});
      const request=(await client.query(`INSERT INTO split_bill_requests(requester_id,contributor_id,amount,total,own_share,shop,slot,items,discount) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id,created_at`,[s.id,contributor.id,splitAmount,v.payable,ownShare,v.shop,v.slot,JSON.stringify(v.storedItems),v.discount])).rows[0];
      await client.query('COMMIT');
      inTransaction=false;
      return res.status(202).json({pending:true,request:{id:Number(request.id),walletId:contributor.customer_code,contributorName:contributor.name,amount:splitAmount,ownShare,total:v.payable,createdAt:new Date(request.created_at).getTime()}});
    }
    if(Number(s.wallet_balance)<v.payable){await client.query('ROLLBACK');return res.status(400).json({error:'Insufficient wallet balance — use Split bill or add money'});}
    const publicId='CB-'+Date.now().toString(36).toUpperCase()+'-'+Math.random().toString(36).slice(2,7).toUpperCase();
    for(const [id,q] of v.quantities) await client.query('UPDATE menu_items SET stock=GREATEST(0,COALESCE(stock,0)-$1) WHERE id=$2',[q,id]);
    await client.query('UPDATE customers SET wallet_balance=wallet_balance-$1 WHERE id=$2',[v.payable,s.id]);
    const row=(await client.query(`INSERT INTO orders(public_id,customer_id,shop,items,total,discount,slot,checkout_request_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,[publicId,s.id,v.shop,JSON.stringify(v.storedItems),v.payable,v.discount,v.slot,checkoutRequestId])).rows[0];
    await client.query(`INSERT INTO wallet_transactions(customer_id,icon,title,sub,amount,type,order_id) VALUES($1,'🍱',$2,$3,$4,'debit',$5)`,[s.id,`${v.shop} • ${v.storedItems.map(i=>i.name).join(' • ')}`,`${v.storedItems.length} menu item${v.storedItems.length>1?'s':''}`,v.payable,row.id]);
    let balance=Number(s.wallet_balance)-v.payable;
    const ns=(await client.query('SELECT * FROM customers WHERE id=$1',[s.id])).rows[0];
    if(ns.autopay_enabled&&balance<=Number(ns.autopay_threshold)){const amount=Number(ns.autopay_amount);balance+=amount;await client.query('UPDATE customers SET wallet_balance=wallet_balance+$1 WHERE id=$2',[amount,s.id]);await client.query(`INSERT INTO wallet_transactions(customer_id,icon,title,sub,amount,type) VALUES($1,'🔄','Auto-pay top-up',$2,$3,'credit')`,[s.id,`Automatic refill • threshold ₹${Number(ns.autopay_threshold)}`,amount]);}
    await client.query('COMMIT');
    inTransaction=false;
    res.json({order:serializeOrder(row),wallet:balance});
  }catch(e){
    if(inTransaction && client){try{await client.query('ROLLBACK')}catch{}}
    console.error('Checkout order error:',e);
    const code=(e?.code==='57014'||e?.code==='55P03'||e?.code==='POOL_TIMEOUT')?503:(e?.status||500);
    res.status(code).json({error:(e?.code==='57014'||e?.code==='55P03'||e?.code==='POOL_TIMEOUT')?'Checkout is taking too long or the database is busy. Please try again.':(e?.message||'Could not place order')});
  }finally{if(client)client.release();}
});

app.post('/api/customer/split-bill/requests/:id/respond',auth,customerRole,async(req,res)=>{
  const client=await (await db()).connect();
  try{
    const action=String(req.body.action||'').toLowerCase();
    if(!['approve','reject'].includes(action)) return res.status(400).json({error:'Invalid response'});
    await client.query('BEGIN');
    const request=(await client.query(`SELECT r.*,rq.name AS requester_name,rq.customer_code AS requester_code FROM split_bill_requests r JOIN customers rq ON rq.id=r.requester_id WHERE r.id=$1 FOR UPDATE`,[req.params.id])).rows[0];
    if(!request){await client.query('ROLLBACK');return res.status(404).json({error:'Split bill request not found'});}
    if(Number(request.contributor_id)!==Number(req.user.id)){await client.query('ROLLBACK');return res.status(403).json({error:'You are not the recipient of this request'});}
    if(request.status!=='pending'){await client.query('ROLLBACK');return res.status(409).json({error:`This request is already ${request.status}`});}
    if(action==='reject'){
      await client.query(`UPDATE split_bill_requests SET status='rejected',responded_at=NOW() WHERE id=$1`,[request.id]);
      await client.query('COMMIT');
      return res.json({status:'rejected'});
    }
    // Lock both wallets in a stable order so simultaneous approvals cannot deadlock.
    const ids=[Number(request.requester_id),Number(request.contributor_id)].sort((a,b)=>a-b);
    const locked=(await client.query('SELECT * FROM customers WHERE id=ANY($1::bigint[]) FOR UPDATE',[ids])).rows;
    const requester=locked.find(x=>Number(x.id)===Number(request.requester_id));
    const contributor=locked.find(x=>Number(x.id)===Number(request.contributor_id));
    if(!requester||!contributor) throw Object.assign(new Error('Customer wallet not found'),{status:404});
    const amount=Number(request.amount), ownShare=Number(request.own_share);
    if(Number(contributor.wallet_balance)<amount) throw Object.assign(new Error(`${contributor.name}'s wallet has insufficient balance`),{status:400});
    if(Number(requester.wallet_balance)<ownShare) throw Object.assign(new Error(`${requester.name}'s wallet has insufficient balance for their share`),{status:400});
    const items=Array.isArray(request.items)?request.items:JSON.parse(request.items);
    const quantities=new Map(); for(const i of items) quantities.set(Number(i.id),(quantities.get(Number(i.id))||0)+Number(i.q));
    const menuIds=[...quantities.keys()];
    const menuRows=(await client.query('SELECT id,stock,available,shop FROM menu_items WHERE id=ANY($1::int[]) FOR UPDATE',[menuIds])).rows;
    const byId=new Map(menuRows.map(r=>[Number(r.id),r]));
    if(menuRows.length!==menuIds.length||menuRows.some(m=>!m.available||m.shop!==request.shop)) throw Object.assign(new Error('One or more items are no longer available'),{status:400});
    if([...quantities.entries()].some(([id,q])=>Number(byId.get(id).stock||0)<q)) throw Object.assign(new Error('One or more items are out of stock'),{status:400});
    const publicId='CB-'+Date.now().toString(36).toUpperCase()+'-'+Math.random().toString(36).slice(2,7).toUpperCase();
    for(const [id,q] of quantities) await client.query('UPDATE menu_items SET stock=GREATEST(0,COALESCE(stock,0)-$1) WHERE id=$2',[q,id]);
    await client.query('UPDATE customers SET wallet_balance=wallet_balance-$1 WHERE id=$2',[ownShare,requester.id]);
    await client.query('UPDATE customers SET wallet_balance=wallet_balance-$1 WHERE id=$2',[amount,contributor.id]);
    const row=(await client.query(`INSERT INTO orders(public_id,customer_id,shop,items,total,discount,slot) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING *`,[publicId,requester.id,request.shop,JSON.stringify(items),Number(request.total),Number(request.discount),request.slot])).rows[0];
    if(ownShare>0) await client.query(`INSERT INTO wallet_transactions(customer_id,icon,title,sub,amount,type,order_id) VALUES($1,'🍱',$2,$3,$4,'debit',$5)`,[requester.id,`${request.shop} • ${items.map(i=>i.name).join(' • ')}`,`Your share • ₹${ownShare}`,ownShare,row.id]);
    await client.query(`INSERT INTO wallet_transactions(customer_id,icon,title,sub,amount,type,order_id) VALUES($1,'🤝',$2,$3,$4,'debit',$5)`,[contributor.id,`${request.shop} • Split bill contribution`,`Approved for ${requester.name} • ${requester.customer_code}`,amount,row.id]);
    let requesterBalance=Number(requester.wallet_balance)-ownShare;
    const ns=(await client.query('SELECT * FROM customers WHERE id=$1',[requester.id])).rows[0];
    if(ns.autopay_enabled&&requesterBalance<=Number(ns.autopay_threshold)){const refill=Number(ns.autopay_amount);requesterBalance+=refill;await client.query('UPDATE customers SET wallet_balance=wallet_balance+$1 WHERE id=$2',[refill,requester.id]);await client.query(`INSERT INTO wallet_transactions(customer_id,icon,title,sub,amount,type) VALUES($1,'🔄','Auto-pay top-up',$2,$3,'credit')`,[requester.id,`Automatic refill • threshold ₹${Number(ns.autopay_threshold)}`,refill]);}
    await client.query(`UPDATE split_bill_requests SET status='approved',order_id=$1,responded_at=NOW() WHERE id=$2`,[row.id,request.id]);
    await client.query('COMMIT');
    res.json({status:'approved',amount,order:serializeOrder(row),wallet:Number(contributor.wallet_balance)-amount,requesterWallet:requesterBalance});
  }catch(e){await client.query('ROLLBACK');console.error(e);res.status(e.status||500).json({error:e.message||'Could not process split bill request'});}finally{client.release();}
});

app.post('/api/customer/orders/:id/rating',auth,customerRole,async(req,res)=>{
  try{
    const rating=Number(req.body.rating);
    if(!Number.isInteger(rating)||rating<1||rating>5)return res.status(400).json({error:'Rating must be between 1 and 5'});
    const d=await db();
    const order=(await d.query('SELECT id,status FROM orders WHERE public_id=$1 AND customer_id=$2',[req.params.id,req.user.id])).rows[0];
    if(!order)return res.status(404).json({error:'Order not found'});
    if(Number(order.status)<3)return res.status(400).json({error:'You can rate an order after it is completed'});
    const comment=String(req.body.comment||'').trim().slice(0,300);
    const existing=(await d.query('SELECT order_id,rating,comment FROM order_ratings WHERE order_id=$1 AND customer_id=$2',[order.id,req.user.id])).rows[0];
    if(existing)return res.status(409).json({error:'You have already rated this order'});
    const row=(await d.query(`INSERT INTO order_ratings(customer_id,order_id,rating,comment) VALUES($1,$2,$3,$4) RETURNING order_id,rating,comment`,[req.user.id,order.id,rating,comment||null])).rows[0];
    res.json({rating:{orderId:req.params.id,rating:Number(row.rating),comment:row.comment||''}});
  }catch(e){console.error(e);res.status(500).json({error:'Could not save rating'});}
});

app.post('/api/customer/wallet/topup',auth,customerRole,async(req,res)=>{try{const d=await db(),amount=Number(req.body.amount);if(![250,500,1000,2000].includes(amount))return res.status(400).json({error:'Invalid amount'});await d.query('UPDATE customers SET wallet_balance=wallet_balance+$1 WHERE id=$2',[amount,req.user.id]);await d.query(`INSERT INTO wallet_transactions(customer_id,icon,title,sub,amount,type) VALUES($1,'💰','Wallet top-up','Demo wallet load',$2,'credit')`,[req.user.id,amount]);const s=(await d.query('SELECT wallet_balance FROM customers WHERE id=$1',[req.user.id])).rows[0];res.json({wallet:Number(s.wallet_balance)});}catch(e){console.error(e);res.status(500).json({error:'Top-up failed'});}});
app.post('/api/customer/autopay',auth,customerRole,async(req,res)=>{const client=await (await db()).connect();try{const enabled=!!req.body.enabled,threshold=Number(req.body.threshold)||200,amount=Number(req.body.amount)||500;await client.query('BEGIN');const s=(await client.query('SELECT * FROM customers WHERE id=$1 FOR UPDATE',[req.user.id])).rows[0];if(!s)throw new Error('Customer not found');await client.query('UPDATE customers SET autopay_enabled=$1,autopay_threshold=$2,autopay_amount=$3 WHERE id=$4',[enabled,threshold,amount,req.user.id]);let balance=Number(s.wallet_balance);if(enabled && balance<=threshold){balance+=amount;await client.query('UPDATE customers SET wallet_balance=wallet_balance+$1 WHERE id=$2',[amount,req.user.id]);await client.query(`INSERT INTO wallet_transactions(customer_id,icon,title,sub,amount,type) VALUES($1,'🔄','Auto-pay top-up',$2,$3,'credit')`,[req.user.id,`Automatic refill • threshold ₹${threshold}`,amount]);}await client.query('COMMIT');res.json({autopay:{enabled,threshold,amount},wallet:balance});}catch(e){await client.query('ROLLBACK');console.error(e);res.status(500).json({error:'Auto-pay update failed'});}finally{client.release();}});


async function broadcastMenuSnapshot(payload){
  const message=`data: ${JSON.stringify(payload)}\n\n`;
  for(const res of menuRealtimeClients){
    try{res.write(message);}
    catch(e){menuRealtimeClients.delete(res);try{res.end();}catch{}}
  }
}

async function startMenuRealtimeListener(){
  if(!DATABASE_URL || menuRealtimeListenerClient || menuRealtimeStarting)return;
  menuRealtimeStarting=true;
  let client=null;
  try{
    // Use a standalone pg.Client rather than a Pool client. LISTEN is a long-lived
    // connection and must never consume an API connection from the request pool.
    client=new Client({
      connectionString:DATABASE_URL,
      ssl:process.env.NODE_ENV==='production'?{rejectUnauthorized:false}:false,
      connectionTimeoutMillis:5000
    });
    await client.connect();
    menuRealtimeListenerClient=client;
    client.on('notification',msg=>{
      if(msg.channel!=='campusbite_menu_changed')return;
      try{
        const payload=JSON.parse(msg.payload||'{}');
        void broadcastMenuSnapshot(payload);
      }catch(e){console.error('Invalid menu realtime payload:',e);}
    });
    let closed=false;
    const scheduleReconnect=()=>{
      if(closed)return;
      closed=true;
      if(menuRealtimeListenerClient===client)menuRealtimeListenerClient=null;
      if(menuRealtimeReconnectTimer || !dbReady)return;
      menuRealtimeReconnectTimer=setTimeout(()=>{
        menuRealtimeReconnectTimer=null;
        void startMenuRealtimeListener();
      },2000);
    };
    client.on('error',e=>{
      console.error('Menu realtime listener error:',e.message);
      scheduleReconnect();
    });
    client.on('end',scheduleReconnect);
    await client.query('LISTEN campusbite_menu_changed');
    console.log('CampusBite menu realtime listener ready');
  }catch(e){
    if(menuRealtimeListenerClient===client)menuRealtimeListenerClient=null;
    try{if(client)await client.end();}catch{}
    menuRealtimeListenerClient=null;
    console.error('CampusBite menu realtime listener startup failed:',e.message);
    if(dbReady && !menuRealtimeReconnectTimer){
      menuRealtimeReconnectTimer=setTimeout(()=>{
        menuRealtimeReconnectTimer=null;
        void startMenuRealtimeListener();
      },3000);
    }
  }finally{menuRealtimeStarting=false;}
}
app.get('/api/menu/events',async(req,res)=>{
  res.status(200);
  res.setHeader('Content-Type','text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control','no-cache, no-transform');
  res.setHeader('Connection','keep-alive');
  res.setHeader('X-Accel-Buffering','no');
  if(typeof res.flushHeaders==='function')res.flushHeaders();
  menuRealtimeClients.add(res);
  // Send an immediate heartbeat so proxies establish the stream without waiting.
  res.write(': connected\n\n');
  const heartbeat=setInterval(()=>{try{res.write(': heartbeat\n\n');}catch{}},15000);
  const cleanup=()=>{clearInterval(heartbeat);menuRealtimeClients.delete(res);};
  req.on('close',cleanup);
  res.on('error',cleanup);
});

app.get('/api/menu',async(req,res)=>{try{res.setHeader('Cache-Control','no-store, no-cache, must-revalidate, proxy-revalidate');res.setHeader('Pragma','no-cache');res.setHeader('Expires','0');const d=await db();const rows=(await d.query('SELECT id,shop,name,price,stock,available FROM menu_items ORDER BY id')).rows;res.json({items:rows});}catch(e){console.error(e);res.status(500).json({error:'Menu unavailable'});}});
app.patch('/api/staff/menu/:id',auth,role('staff'),async(req,res)=>{
  try{
    const d=await db();
    const id=Number(req.params.id);
    if(!Number.isInteger(id)) return res.status(400).json({error:'Invalid menu item'});
    const row=(await d.query('SELECT * FROM menu_items WHERE id=$1',[id])).rows[0];
    if(!row) return res.status(404).json({error:'Menu item not found'});
    const canonicalShop=id>=100&&id<200?'Hari Sandwich':id>=200&&id<300?'Reo Store':id>=300&&id<400?'Campus Café':null;
    const sameShop=canonicalShop && (
      row.shop===req.user.shop ||
      row.shop.replace('é','e').toLowerCase()===String(req.user.shop).replace('é','e').toLowerCase()
    );
    if(!sameShop || canonicalShop!==req.user.shop) return res.status(403).json({error:'You cannot change another shop\'s menu'});
    const out=(await d.query(
      'UPDATE menu_items SET available=$1 WHERE id=$2 RETURNING id,shop,name,available',
      [!!req.body.available,row.id]
    )).rows[0];
    res.json({item:out});
  }catch(e){
    console.error(e);
    res.status(500).json({error:'Availability update failed'});
  }
});

app.get('/api/staff/orders',auth,role('staff'),async(req,res)=>{try{const d=await db();const rows=(await d.query(`SELECT o.*, s.name AS customer_name, s.customer_code FROM orders o JOIN customers s ON s.id=o.customer_id WHERE o.shop=$1 ORDER BY o.created_at DESC LIMIT 500`,[req.user.shop])).rows;res.json({orders:rows.map(serializeOrder)});}catch(e){console.error(e);res.status(500).json({error:'Orders unavailable'});}});
app.patch('/api/staff/orders/:id/status',auth,role('staff'),async(req,res)=>{try{const d=await db();const row=(await d.query('SELECT * FROM orders WHERE public_id=$1 AND shop=$2',[req.params.id,req.user.shop])).rows[0];if(!row)return res.status(404).json({error:'Order not found'});if(Number(row.status)>=3)return res.status(400).json({error:'Order is already completed'});const next=Number(row.status)+1;if(next!==Number(req.body.status))return res.status(400).json({error:'Invalid next status'});let sql='UPDATE orders SET status=$1';const vals=[next,row.id];if(next===1)sql+=', prep_started_at=COALESCE(prep_started_at,NOW())';if(next===2)sql+=', ready_at=COALESCE(ready_at,NOW())';if(next===3)sql+=', completed_at=COALESCE(completed_at,NOW())';sql+=' WHERE id=$2 RETURNING *';const out=(await d.query(sql,vals)).rows[0];res.json({order:serializeOrder(out)});}catch(e){console.error(e);res.status(500).json({error:'Status update failed'});}});
app.get('/api/staff/summary',auth,role('staff'),async(req,res)=>{try{const d=await db();const day=req.query.date||new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Kolkata',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date());const rows=(await d.query(`SELECT * FROM orders WHERE shop=$1 AND (created_at AT TIME ZONE 'Asia/Kolkata')::date = $2::date`,[req.user.shop,day])).rows;const revenue=rows.reduce((a,r)=>a+Number(r.total),0);const active=rows.filter(r=>r.status<3).length;res.json({date:day,orders:rows.length,revenue,active});}catch(e){console.error(e);res.status(500).json({error:'Summary unavailable'});}});
app.get('/api/staff/rating',auth,role('staff'),async(req,res)=>{try{const d=await db();const row=(await d.query(`SELECT AVG(r.rating)::numeric(4,2) AS average, COUNT(r.rating)::int AS count FROM order_ratings r JOIN orders o ON o.id=r.order_id WHERE o.shop=$1`,[req.user.shop])).rows[0]||{};res.json({shop:req.user.shop,average:row.average==null?null:Number(row.average),count:Number(row.count||0)});}catch(e){console.error(e);res.status(500).json({error:'Rating unavailable'});}});

app.get('/{*splat}',(req,res)=>res.sendFile(path.join(__dirname,'public','index.html')));

// Start HTTP immediately so Render can reach the service even while PostgreSQL
// is waking up or the first-time schema initialization is running.
const server = app.listen(PORT,'0.0.0.0',()=>{
  console.log(`CampusBite listening on ${PORT}`);
  const initializeWithRetry=async()=>{
    while(!dbReady){
      try{
        await init();
        dbReady=true;
        console.log('CampusBite database initialization complete');
        // Start the realtime listener after the database transaction commits.
        // Listener failure must never invalidate a successful DB initialization.
        void startMenuRealtimeListener();
      }catch(e){
        console.error('CampusBite database initialization failed:',e);
        console.log('Retrying database initialization in 5 seconds...');
        await new Promise(r=>setTimeout(r,5000));
      }
    }
  };
  initializeWithRetry();
});

async function shutdown(){
  for(const res of menuRealtimeClients){try{res.end();}catch{}}
  menuRealtimeClients.clear();
  if(menuRealtimeListenerClient){try{await menuRealtimeListenerClient.end();}catch{} menuRealtimeListenerClient=null;}
  if(menuRealtimeReconnectTimer)clearTimeout(menuRealtimeReconnectTimer);
  await new Promise(resolve=>server.close(resolve));
  if(pool)await pool.end().catch(()=>{});
  process.exit(0);
}
process.on('SIGTERM',()=>void shutdown());
process.on('SIGINT',()=>void shutdown());
