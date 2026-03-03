FROM node:22-bookworm-slim

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY app.js index.html server.js styles.css ./

ENV NODE_ENV=production
ENV PORT=8086

EXPOSE 8086

CMD ["npm", "start"]
