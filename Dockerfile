FROM node:26-alpine

# tini forwards signals and reaps the webR child processes
RUN apk add --no-cache tini

EXPOSE 3000

COPY package.json package-lock.json /app/
WORKDIR /app
RUN npm ci --omit=dev

COPY bin /app/bin
COPY src /app/src

HEALTHCHECK --interval=30s --timeout=5s CMD wget -qO /dev/null http://localhost:3000/status || exit 1

ENTRYPOINT ["tini", "--"]
CMD ["node", "bin/server.js"]
