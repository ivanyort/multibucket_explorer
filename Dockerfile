FROM node:22-bookworm-slim

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends default-jre-headless \
  && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY app.js index.html server.js styles.css ./

ENV NODE_ENV=production
ENV PORT=8086

EXPOSE 8086

CMD ["npm", "start"]
