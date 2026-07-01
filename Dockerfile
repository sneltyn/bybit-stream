FROM node:22-alpine AS base
WORKDIR /app
ENV YARN_ENABLE_GLOBAL_CACHE=false

FROM base AS deps
COPY package.json yarn.lock .yarnrc.yml ./
COPY .yarn .yarn
RUN yarn install --immutable

FROM base AS builder
COPY --from=deps /app/node_modules ./node_modules
COPY --from=deps /app/.yarn ./.yarn
COPY --from=deps /app/.yarnrc.yml ./.yarnrc.yml
COPY . .
RUN yarn build

FROM base AS runner
ENV NODE_ENV=production
COPY --from=builder /app/dist ./dist
COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./

CMD ["node", "./dist/server.js"]
