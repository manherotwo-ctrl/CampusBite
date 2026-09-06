# CampusBite — Production Deployment Package

This package is the single deployment candidate for CampusBite.

## Architecture

- Customer and staff clients use the same Node/Express API.
- PostgreSQL is the source of truth for customers, wallets, transactions, orders, menu availability, and staff status changes.
- Browser localStorage is used only for the login token/session and the selected-shop UI preference. Orders, wallet balances, expenditure, transactions, and auto-pay are NOT stored in localStorage.
- Customer clients use a PostgreSQL-backed Server-Sent Events stream for immediate menu availability/stock/price updates, with low-frequency polling as a reconnect fallback. Account/order state is refreshed every 2 seconds.
- Staff clients use a controlled 2-second refresh for orders/summary data and an isolated menu lifecycle so availability toggles cannot race with background redraws.
- Staff status transitions are enforced server-side: Received -> Preparing -> Ready -> Completed.
- Staff access is restricted to the authenticated staff member's assigned shop.
- Daily revenue/order counts use the Asia/Kolkata calendar date and are recalculated on every staff refresh/poll, so they roll over automatically when the date changes.

## Render

The included `render.yaml` provisions:
- one Node web service
- one PostgreSQL database
- `DATABASE_URL` from the database connection string
- a generated `JWT_SECRET`
- `NODE_ENV=production`

Build command: `npm install`
Start command: `npm start`

## Important

Do not deploy the standalone HTML preview. Deploy the contents of this package as the Render/Git repository root.

The server initializes the database schema on startup and preserves existing customer records. New customers create their own CampusBite ID and password through the customer registration flow. Staff credentials remain backend-controlled.

## Customer account model

- Customers create their own CampusBite ID and password.
- Customer IDs are case-insensitively unique.
- Passwords are stored as bcrypt hashes; plaintext passwords are not stored.
- Customer accounts, wallets, transactions, orders, ratings, and split-bill data are PostgreSQL-backed.
- The customer role is assigned internally by the server; customers do not select Student/Teacher roles during authentication.

## Customer pickup time

Checkout supports ASAP or scheduled pickup from 8:00 AM through 4:00 PM in 15-minute increments. The server validates the selected pickup slot.



## Database initialization
On startup, the server automatically applies `schema.sql` before running demo-account
and menu seeding. This allows a fresh PostgreSQL database (including a new Render or
Supabase database) to start without manually creating `menu_items` first.

Staff authentication (current build): staff credentials are backend-owned and persisted in PostgreSQL. Each staff account has a unique Staff ID and password and a fixed assigned shop. The login response derives the staff name/shop from the authenticated database record; the browser cannot choose a different shop or identity.

Current demo staff credentials:
- HARI-STAFF / Hari#482731 → Hari Sandwich
- REO-STAFF / Reo#615904 → Reo Store
- CAFE-STAFF / Cafe#738215 → Campus Café
