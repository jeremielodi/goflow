FROM node:22-alpine AS builder
WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .
# VITE_API_URL is empty — the nginx proxy handles /engine-rest, /auth, etc.
RUN npm run build

# ── Runtime ────────────────────────────────────────────────────────────────

FROM nginx:1.27-alpine

COPY --from=builder /app/dist /usr/share/nginx/html

# Nginx config: serve SPA + reverse-proxy /engine-rest + /auth to GoFlow
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
