FROM apify/actor-node:24
COPY package*.json ./
RUN npm --quiet set progress=false && npm install --omit=dev --omit=optional --no-audit
COPY . ./
CMD npm start --silent
