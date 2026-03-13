FROM node:22-bookworm-slim

WORKDIR /app

ARG APP_VERSION=0.0.0

RUN apt-get update \
  && apt-get install -y --no-install-recommends default-jre-headless \
  && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY app.js index.html server.js styles.css ./

ENV NODE_ENV=production
ENV PORT=8086
ENV APP_VERSION=${APP_VERSION}
ENV TLS_CERT_FILE=/run/certs/tls.crt
ENV TLS_KEY_FILE=/run/certs/tls.key

EXPOSE 8086

CMD ["npm", "start"]
